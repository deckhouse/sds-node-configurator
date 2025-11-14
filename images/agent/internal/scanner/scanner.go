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
	"time"

	"github.com/pilebones/go-udev/netlink"
	"k8s.io/utils/clock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bd"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/lvg"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/throttler"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

type Scanner interface {
	Run(ctx context.Context,
		log logger.Logger,
		cfg config.Config,
		sdsCache *cache.Cache,
		bdCtrl func(context.Context) (controller.Result, error),
		lvgDiscoverCtrl func(context.Context) (controller.Result, error)) error
}

type scanner struct {
	commands utils.Commands
}

func NewScanner(commands utils.Commands) Scanner {
	return &scanner{commands: commands}
}

func (s *scanner) Run(
	ctx context.Context,
	log logger.Logger,
	cfg config.Config,
	sdsCache *cache.Cache,
	bdCtrl func(context.Context) (controller.Result, error),
	lvgDiscoverCtrl func(context.Context) (controller.Result, error),
) error {
	log = log.WithName("RunScanner")
	log.Info("starts the work")

	t := throttler.New(cfg.ThrottleInterval)

	conn := new(netlink.UEventConn)
	if err := conn.Connect(netlink.UdevEvent); err != nil {
		log.Error(err, "Failed to connect to Netlink")
		return err
	}
	log.Debug("system socket connection succeeded")

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

	log.Info("start to listen to events")

	duration := 1 * time.Second
	timer := time.NewTimer(duration)
	for {
		select {
		case device, open := <-eventChan:
			timer.Reset(duration)
			deviceName := device.Env["DEVNAME"]
			log := log.WithValues("deviceName", deviceName)
			log.Debug("event triggered for device")
			log.Trace("device from the event", "device", device.String())
			if !open {
				err := errors.New("EventChan has been closed when monitor udev event")
				log.Error(err, "unable to read from the event channel")
				return err
			}

			t.Do(func() {
				log.Info("start to fill the cache")
				err := s.fillTheCache(ctx, log, sdsCache, cfg)
				if err != nil {
					log.Error(err, "unable to fill the cache. Retry")
					go func() {
						eventChan <- device
					}()
					return
				}
				log.Info("successfully filled the cache")

				err = runControllersReconcile(ctx, log, bdCtrl, lvgDiscoverCtrl)
				if err != nil {
					log.Error(err, "unable to run controllers reconciliations")
				}

				log.Info("successfully ran the controllers reconcile funcs")
			})

		case err := <-errChan:
			log.Error(err, "Monitor udev event error")
			quit = conn.Monitor(eventChan, errChan, matcher)
			timer.Reset(duration)
			continue

		case <-quit:
			err := errors.New("receive quit signal when monitor udev event")
			log.Error(err, "unable to read from the event channel")
			return err

		case <-timer.C:
			log.Info("events ran out. Start to fill the cache")
			err := s.fillTheCache(ctx, log, sdsCache, cfg)
			if err != nil {
				log.Error(err, "unable to fill the cache after all events passed. Retry")
				timer.Reset(duration)
				continue
			}

			log.Info("successfully filled the cache after all events passed")

			err = runControllersReconcile(ctx, log, bdCtrl, lvgDiscoverCtrl)
			if err != nil {
				log.Error(err, "unable to run controllers reconciliations")
			}

			log.Info("successfully ran the controllers reconcile funcs")
		}
	}
}

func runControllersReconcile(
	ctx context.Context,
	log logger.Logger,
	bdCtrl func(context.Context) (controller.Result, error),
	lvgDiscoverCtrl func(context.Context) (controller.Result, error),
) error {
	log = log.WithName("runControllersReconcile")
	log = log.WithValues("discovererName", bd.DiscovererName)
	log.Info("run reconcile")
	bdRes, err := bdCtrl(ctx)
	if err != nil {
		log.Error(err, "an error occurred while reconcile")
		return err
	}

	if bdRes.RequeueAfter > 0 {
		go func() {
			for bdRes.RequeueAfter > 0 {
				log.Warning("BlockDevices reconcile needs a retry", "retryIn", bdRes.RequeueAfter)
				time.Sleep(bdRes.RequeueAfter)
				bdRes, err = bdCtrl(ctx)
			}

			log.Info("successfully reconciled BlockDevices after a retry")
		}()
	}

	log.Info("run successfully reconciled")

	log = log.WithValues("discovererName", lvg.DiscovererName)
	log.Info("run reconcile")
	lvgRes, err := lvgDiscoverCtrl(ctx)
	if err != nil {
		log.Error(err, "an error occurred while reconcile")
		return err
	}
	if lvgRes.RequeueAfter > 0 {
		go func() {
			for lvgRes.RequeueAfter > 0 {
				log.Warning("LVMVolumeGroups reconcile needs a retry", "retryIn", lvgRes.RequeueAfter)
				time.Sleep(lvgRes.RequeueAfter)
				lvgRes, err = lvgDiscoverCtrl(ctx)
			}

			log.Info("successfully reconciled LVMVolumeGroups after a retry")
		}()
	}
	log.Info("run successfully reconciled")

	return nil
}

func (s *scanner) fillTheCache(ctx context.Context, log logger.Logger, cache *cache.Cache, cfg config.Config) error {
	log = log.WithName("fillTheCache")
	// the scan operations order is very important as it guarantees the consistent and reliable data from the node
	realClock := clock.RealClock{}
	now := time.Now()
	lvs, lvsErr, err := s.scanLVs(ctx, log, cfg)
	log.Trace("LVS command runs", "duration", realClock.Since(now))
	if err != nil {
		return err
	}

	now = time.Now()
	vgs, vgsErr, err := s.scanVGs(ctx, log, cfg)
	log.Trace("VGS command runs", "duration", realClock.Since(now))
	if err != nil {
		return err
	}

	now = time.Now()
	pvs, pvsErr, err := s.scanPVs(ctx, log, cfg)
	log.Trace("PVS command runs", "duration", realClock.Since(now))
	if err != nil {
		return err
	}

	now = time.Now()
	devices, devErr, err := s.scanDevices(ctx, log, cfg)
	log.Trace("LSBLK command runs", "duration", realClock.Since(now))
	if err != nil {
		return err
	}

	log.Debug("successfully scanned entities. Starts to fill the cache")
	cache.StoreDevices(devices, devErr)
	cache.StorePVs(pvs, pvsErr)
	cache.StoreVGs(vgs, vgsErr)
	cache.StoreLVs(lvs, lvsErr)
	log.Debug("successfully filled the cache")
	cache.PrintTheCache(log)

	return nil
}

func (s *scanner) scanDevices(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.Device, bytes.Buffer, error) {
	log = log.WithName("ScanDevices")
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	devices, cmdStr, stdErr, err := s.commands.GetBlockDevices(ctx)
	if err != nil {
		log.Error(err, "unable to scan the devices", "command", cmdStr)
		return nil, stdErr, err
	}

	return devices, stdErr, nil
}

func (s *scanner) scanPVs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.PVData, bytes.Buffer, error) {
	log = log.WithName("ScanPVs")
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	pvs, cmdStr, stdErr, err := s.commands.GetAllPVs(ctx)
	if err != nil {
		log.Error(err, "unable to scan the PVs", "command", cmdStr)
		return nil, stdErr, err
	}

	return pvs, stdErr, nil
}

func (s *scanner) scanVGs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.VGData, bytes.Buffer, error) {
	log = log.WithName("ScanVGs")
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	vgs, cmdStr, stdErr, err := s.commands.GetAllVGs(ctx)
	if err != nil {
		log.Error(err, "unable to scan the VGs", "command", cmdStr)
		return nil, stdErr, err
	}

	return vgs, stdErr, nil
}

func (s *scanner) scanLVs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.LVData, bytes.Buffer, error) {
	log = log.WithName("ScanLVs")
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	lvs, cmdStr, stdErr, err := s.commands.GetAllLVs(ctx)
	if err != nil {
		log.Error(err, "unable to scan LVs", "command", cmdStr)
		return nil, stdErr, err
	}

	return lvs, stdErr, nil
}
