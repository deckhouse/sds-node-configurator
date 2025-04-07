/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bd"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/lvg"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/throttler"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
	"github.com/pilebones/go-udev/netlink"
	"k8s.io/utils/clock"
)

func RunScanner(
	ctx context.Context,
	log logger.Logger,
	cfg config.Config,
	sdsCache *cache.Cache,
	bdCtrl func(context.Context) (controller.Result, error),
	lvgDiscoverCtrl func(context.Context) (controller.Result, error),
) error {
	log.Info("[RunScanner] starts the work")

	t := throttler.New(cfg.ThrottleInterval)

	conn := new(netlink.UEventConn)
	if err := conn.Connect(netlink.UdevEvent); err != nil {
		log.Error(err, "[RunScanner] Failed to connect to Netlink")
		return err
	}
	log.Debug("[RunScanner] system socket connection succeeded")

	errChan := make(chan error)
	eventChan := make(chan netlink.UEvent)
	matcher := &netlink.RuleDefinitions{
		Rules: []netlink.RuleDefinition{
			{
				Env: map[string]string{
					"SUBSYSTEM": "block",
				},
			},
		},
	}
	quit := conn.Monitor(eventChan, errChan, matcher)

	log.Info("[RunScanner] start to listen to events")

	duration := 1 * time.Second
	timer := time.NewTimer(duration)
	for {
		select {
		case device, open := <-eventChan:
			timer.Reset(duration)
			log.Debug(fmt.Sprintf("[RunScanner] event triggered for device: %s", device.Env["DEVNAME"]))
			log.Trace(fmt.Sprintf("[RunScanner] device from the event: %s", device.String()))
			if !open {
				err := errors.New("EventChan has been closed when monitor udev event")
				log.Error(err, "[RunScanner] unable to read from the event channel")
				return err
			}

			t.Do(func() {
				log.Info("[RunScanner] start to fill the cache")
				err := fillTheCache(ctx, log, sdsCache, cfg)
				if err != nil {
					log.Error(err, "[RunScanner] unable to fill the cache. Retry")
					go func() {
						eventChan <- device
					}()
					return
				}
				log.Info("[RunScanner] successfully filled the cache")

				err = runControllersReconcile(ctx, log, bdCtrl, lvgDiscoverCtrl)
				if err != nil {
					log.Error(err, "[RunScanner] unable to run controllers reconciliations")
				}

				log.Info("[RunScanner] successfully ran the controllers reconcile funcs")
			})

		case err := <-errChan:
			log.Error(err, "[RunScanner] Monitor udev event error")
			quit = conn.Monitor(eventChan, errChan, matcher)
			timer.Reset(duration)
			continue

		case <-quit:
			err := errors.New("receive quit signal when monitor udev event")
			log.Error(err, "[RunScanner] unable to read from the event channel")
			return err

		case <-timer.C:
			log.Info("[RunScanner] events ran out. Start to fill the cache")
			err := fillTheCache(ctx, log, sdsCache, cfg)
			if err != nil {
				log.Error(err, "[RunScanner] unable to fill the cache after all events passed. Retry")
				timer.Reset(duration)
				continue
			}

			log.Info("[RunScanner] successfully filled the cache after all events passed")

			err = runControllersReconcile(ctx, log, bdCtrl, lvgDiscoverCtrl)
			if err != nil {
				log.Error(err, "[RunScanner] unable to run controllers reconciliations")
			}

			log.Info("[RunScanner] successfully ran the controllers reconcile funcs")
		}
	}
}

func runControllersReconcile(
	ctx context.Context,
	log logger.Logger,
	bdCtrl func(context.Context) (controller.Result, error),
	lvgDiscoverCtrl func(context.Context) (controller.Result, error),
) error {
	log.Info(fmt.Sprintf("[runControllersReconcile] run %s reconcile", bd.DiscovererName))
	bdRes, err := bdCtrl(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[runControllersReconcile] an error occurred while %s reconcile", bd.DiscovererName))
		return err
	}

	if bdRes.RequeueAfter > 0 {
		go func() {
			for bdRes.RequeueAfter > 0 {
				log.Warning(fmt.Sprintf("[runControllersReconcile] BlockDevices reconcile needs a retry in %s", bdRes.RequeueAfter.String()))
				time.Sleep(bdRes.RequeueAfter)
				bdRes, err = bdCtrl(ctx)
			}

			log.Info("[runControllersReconcile] successfully reconciled BlockDevices after a retry")
		}()
	}

	log.Info(fmt.Sprintf("[runControllersReconcile] run %s successfully reconciled", bd.DiscovererName))

	log.Info(fmt.Sprintf("[runControllersReconcile] run %s reconcile", lvg.DiscovererName))
	lvgRes, err := lvgDiscoverCtrl(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[runControllersReconcile] an error occurred while %s reconcile", lvg.DiscovererName))
		return err
	}
	if lvgRes.RequeueAfter > 0 {
		go func() {
			for lvgRes.RequeueAfter > 0 {
				log.Warning(fmt.Sprintf("[runControllersReconcile] LVMVolumeGroups reconcile needs a retry in %s", lvgRes.RequeueAfter.String()))
				time.Sleep(lvgRes.RequeueAfter)
				lvgRes, err = lvgDiscoverCtrl(ctx)
			}

			log.Info("[runControllersReconcile] successfully reconciled LVMVolumeGroups after a retry")
		}()
	}
	log.Info(fmt.Sprintf("[runControllersReconcile] run %s successfully reconciled", lvg.DiscovererName))

	return nil
}

func fillTheCache(ctx context.Context, log logger.Logger, cache *cache.Cache, cfg config.Config) error {
	// the scan operations order is very important as it guarantees the consistent and reliable data from the node
	realClock := clock.RealClock{}
	now := time.Now()
	lvs, lvsErr, err := scanLVs(ctx, log, cfg)
	log.Trace(fmt.Sprintf("[fillTheCache] LVS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	vgs, vgsErr, err := scanVGs(ctx, log, cfg)
	log.Trace(fmt.Sprintf("[fillTheCache] VGS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	pvs, pvsErr, err := scanPVs(ctx, log, cfg)
	log.Trace(fmt.Sprintf("[fillTheCache] PVS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	devices, devErr, err := scanDevices(ctx, log, cfg)
	log.Trace(fmt.Sprintf("[fillTheCache] LSBLK command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	log.Debug("[fillTheCache] successfully scanned entities. Starts to fill the cache")
	cache.StoreDevices(devices, devErr)
	cache.StorePVs(pvs, pvsErr)
	cache.StoreVGs(vgs, vgsErr)
	cache.StoreLVs(lvs, lvsErr)
	log.Debug("[fillTheCache] successfully filled the cache")
	cache.PrintTheCache(log)

	return nil
}

func scanDevices(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.Device, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	devices, cmdStr, stdErr, err := utils.GetBlockDevices(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanDevices] unable to scan the devices, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return devices, stdErr, nil
}

func scanPVs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.PVData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	pvs, cmdStr, stdErr, err := utils.GetAllPVs(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanPVs] unable to scan the PVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return pvs, stdErr, nil
}

func scanVGs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.VGData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	vgs, cmdStr, stdErr, err := utils.GetAllVGs(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanVGs] unable to scan the VGs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return vgs, stdErr, nil
}

func scanLVs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.LVData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	lvs, cmdStr, stdErr, err := utils.GetAllLVs(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanLVs] unable to scan LVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return lvs, stdErr, nil
}
