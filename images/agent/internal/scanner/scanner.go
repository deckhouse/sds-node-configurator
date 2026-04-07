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

	"github.com/pilebones/go-udev/crawler"
	"github.com/pilebones/go-udev/netlink"
	"k8s.io/utils/clock"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/config"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/bd"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller/lvg"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/throttler"
	udevpkg "github.com/deckhouse/sds-node-configurator/images/agent/internal/udev"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

type Scanner interface {
	Run(ctx context.Context,
		bdCtrl func(context.Context) (controller.Result, error),
		lvgDiscoverCtrl func(context.Context) (controller.Result, error)) error
}

type scanner struct {
	commands  utils.Commands
	deviceMap *udevpkg.DeviceMap
	log       logger.Logger
	cfg       config.Config
	cache     *cache.Cache
	metrics   *monitoring.Metrics
}

func NewScanner(commands utils.Commands, log logger.Logger, cfg config.Config, sdsCache *cache.Cache, metrics *monitoring.Metrics) Scanner {
	return &scanner{
		commands:  commands,
		log:       log,
		deviceMap: udevpkg.NewDeviceMap(log),
		cfg:       cfg,
		cache:     sdsCache,
		metrics:   metrics,
	}
}

func (s *scanner) Run(
	ctx context.Context,
	bdCtrl func(context.Context) (controller.Result, error),
	lvgDiscoverCtrl func(context.Context) (controller.Result, error),
) error {
	s.log.Info("[RunScanner] starts the work")

	t := throttler.New(s.cfg.ThrottleInterval)

	conn := new(netlink.UEventConn)
	if err := conn.Connect(netlink.UdevEvent); err != nil {
		s.log.Error(err, "[RunScanner] Failed to connect to Netlink")
		return err
	}
	s.log.Debug("[RunScanner] system socket connection succeeded")

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

	s.crawlDevices(matcher)

	s.log.Info("[RunScanner] start to listen to events")

	duration := 1 * time.Second
	timer := time.NewTimer(duration)

	const mountScanInterval = 10 * time.Second
	mountScanTimer := time.NewTimer(mountScanInterval)

	for {
		select {
		case device, open := <-eventChan:
			timer.Reset(duration)
			s.log.Debug(fmt.Sprintf("[RunScanner] event triggered for device: %s", device.Env["DEVNAME"]))
			s.log.Trace(fmt.Sprintf("[RunScanner] device from the event: %s", device.String()))
			if !open {
				err := errors.New("EventChan has been closed when monitor udev event")
				s.log.Error(err, "[RunScanner] unable to read from the event channel")
				return err
			}

			s.deviceMap.HandleEvent(&device)

			t.Do(func() {
				s.log.Info("[RunScanner] start to fill the cache")
				err := s.fillTheCache(ctx)
				if err != nil {
					s.log.Error(err, "[RunScanner] unable to fill the cache. Retry")
					go func() {
						eventChan <- device
					}()
					return
				}
				s.log.Info("[RunScanner] successfully filled the cache")

				err = runControllersReconcile(ctx, s.log, bdCtrl, lvgDiscoverCtrl)
				if err != nil {
					s.log.Error(err, "[RunScanner] unable to run controllers reconciliations")
				}

				s.log.Info("[RunScanner] successfully ran the controllers reconcile funcs")
			})

		case err := <-errChan:
			s.log.Error(err, "[RunScanner] Monitor udev event error, restarting monitor and re-crawling devices")
			quit = conn.Monitor(eventChan, errChan, matcher)
			s.crawlDevices(matcher)
			timer.Reset(duration)
			continue

		case <-quit:
			err := errors.New("receive quit signal when monitor udev event")
			s.log.Error(err, "[RunScanner] unable to read from the event channel")
			return err

		case <-timer.C:
			s.log.Info("[RunScanner] events ran out. Start to fill the cache")
			err := s.fillTheCache(ctx)
			if err != nil {
				s.log.Error(err, "[RunScanner] unable to fill the cache after all events passed. Retry")
				timer.Reset(duration)
				continue
			}

			s.log.Info("[RunScanner] successfully filled the cache after all events passed")

			err = runControllersReconcile(ctx, s.log, bdCtrl, lvgDiscoverCtrl)
			if err != nil {
				s.log.Error(err, "[RunScanner] unable to run controllers reconciliations")
			}

			s.log.Info("[RunScanner] successfully ran the controllers reconcile funcs")

		case <-mountScanTimer.C:
			s.log.Debug("[RunScanner] periodic mount scan")
			s.scanAndRefreshMounts()
			mountScanTimer.Reset(mountScanInterval)
		}
	}
}

func (s *scanner) scanAndRefreshMounts() {
	devices, deviceErrs := s.cache.GetDevices()
	if len(devices) == 0 {
		return
	}

	mountPoints, mountErr := udevpkg.ParseMountInfo()
	if mountErr != nil {
		deviceErrs = append(deviceErrs, fmt.Sprintf("[scanAndRefreshMounts] mountinfo unavailable: %v", mountErr))
		for i := range devices {
			devices[i].MountPoint = "unknown"
		}
	} else {
		for i := range devices {
			devices[i].MountPoint = mountPoints[devices[i].DevID]
		}
	}

	s.cache.StoreDevices(devices, deviceErrs)
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
		go func(res controller.Result) {
			for res.RequeueAfter > 0 {
				log.Warning(fmt.Sprintf("[runControllersReconcile] BlockDevices reconcile needs a retry in %s", res.RequeueAfter.String()))
				time.Sleep(res.RequeueAfter)
				var retryErr error
				res, retryErr = bdCtrl(ctx)
				if retryErr != nil {
					log.Error(retryErr, "[runControllersReconcile] BlockDevices retry reconcile failed")
					return
				}
			}
			log.Info("[runControllersReconcile] successfully reconciled BlockDevices after a retry")
		}(bdRes)
	}

	log.Info(fmt.Sprintf("[runControllersReconcile] run %s successfully reconciled", bd.DiscovererName))

	log.Info(fmt.Sprintf("[runControllersReconcile] run %s reconcile", lvg.DiscovererName))
	lvgRes, err := lvgDiscoverCtrl(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[runControllersReconcile] an error occurred while %s reconcile", lvg.DiscovererName))
		return err
	}
	if lvgRes.RequeueAfter > 0 {
		go func(res controller.Result) {
			for res.RequeueAfter > 0 {
				log.Warning(fmt.Sprintf("[runControllersReconcile] LVMVolumeGroups reconcile needs a retry in %s", res.RequeueAfter.String()))
				time.Sleep(res.RequeueAfter)
				var retryErr error
				res, retryErr = lvgDiscoverCtrl(ctx)
				if retryErr != nil {
					log.Error(retryErr, "[runControllersReconcile] LVMVolumeGroups retry reconcile failed")
					return
				}
			}
			log.Info("[runControllersReconcile] successfully reconciled LVMVolumeGroups after a retry")
		}(lvgRes)
	}
	log.Info(fmt.Sprintf("[runControllersReconcile] run %s successfully reconciled", lvg.DiscovererName))

	return nil
}

func (s *scanner) fillTheCache(ctx context.Context) error {
	// the scan operations order is very important as it guarantees the consistent and reliable data from the node
	realClock := clock.RealClock{}
	now := time.Now()
	lvs, lvsErr, err := s.scanLVs(ctx)
	s.log.Trace(fmt.Sprintf("[fillTheCache] LVS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	vgs, vgsErr, err := s.scanVGs(ctx)
	s.log.Trace(fmt.Sprintf("[fillTheCache] VGS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	pvs, pvsErr, err := s.scanPVs(ctx)
	s.log.Trace(fmt.Sprintf("[fillTheCache] PVS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	devices, deviceErrs := s.deviceMap.Snapshot()
	s.log.Trace(fmt.Sprintf("[fillTheCache] device scan runs for: %s", realClock.Since(now).String()))

	now = time.Now()
	mountPoints, mountErr := udevpkg.ParseMountInfo()
	s.log.Trace(fmt.Sprintf("[fillTheCache] mountinfo scan runs for: %s", realClock.Since(now).String()))
	if mountErr != nil {
		deviceErrs = append(deviceErrs, fmt.Sprintf("[fillTheCache] mountinfo unavailable, all devices will be not consumable: %v", mountErr))
		for i := range devices {
			devices[i].MountPoint = "unknown"
		}
	} else {
		for i := range devices {
			devices[i].MountPoint = mountPoints[devices[i].DevID]
		}
	}

	if activated := utils.EnsureVGActivation(ctx, s.log, s.commands, s.metrics, vgs, lvs); activated {
		s.log.Info("[fillTheCache] LVs were activated, re-scanning LVs and VGs")
		now = time.Now()
		lvs, lvsErr, err = s.scanLVs(ctx)
		s.log.Trace(fmt.Sprintf("[fillTheCache] LVS re-scan runs for: %s", realClock.Since(now).String()))
		if err != nil {
			return err
		}
		now = time.Now()
		vgs, vgsErr, err = s.scanVGs(ctx)
		s.log.Trace(fmt.Sprintf("[fillTheCache] VGS re-scan runs for: %s", realClock.Since(now).String()))
		if err != nil {
			return err
		}
	}

	s.log.Debug("[fillTheCache] successfully scanned entities. Starts to fill the cache")
	s.cache.StoreDevices(devices, deviceErrs)
	s.cache.StorePVs(pvs, pvsErr)
	s.cache.StoreVGs(vgs, vgsErr)
	s.cache.StoreLVs(lvs, lvsErr)
	s.log.Debug("[fillTheCache] successfully filled the cache")
	s.cache.PrintTheCache(s.log)

	// Update LVM metrics only for VGs managed by LVMVolumeGroup resources
	managedVGs := s.cache.GetManagedVGs()
	if errs := s.metrics.UpdateLVMMetrics(vgs, lvs, managedVGs); len(errs) > 0 {
		for _, err := range errs {
			s.log.Warning(fmt.Sprintf("[fillTheCache] metrics update error: %v", err))
		}
	}

	return nil
}

// crawlDevices performs a full udev crawl and replaces the device map contents.
// Used at startup and after netlink monitor reconnection to recover from
// potentially lost events.
func (s *scanner) crawlDevices(matcher netlink.Matcher) {
	crawlerQueue := make(chan crawler.Device)
	crawlerErrors := make(chan error)
	crawlerQuit := crawler.ExistingDevices(crawlerQueue, crawlerErrors, matcher)

	var devices []crawler.Device
	done := false
	for !done {
		select {
		case dev, open := <-crawlerQueue:
			if !open {
				done = true
			} else {
				devices = append(devices, dev)
			}
		case err := <-crawlerErrors:
			s.log.Error(err, "[crawlDevices] crawler error")
		}
	}
	close(crawlerQuit)

	s.deviceMap.FillFromCrawler(devices)
	if len(devices) == 0 {
		s.log.Warning("[crawlDevices] crawl returned zero devices, device map is now empty")
	} else {
		s.log.Info(fmt.Sprintf("[crawlDevices] crawl found %d devices", len(devices)))
	}
}

func (s *scanner) scanPVs(ctx context.Context) ([]internal.PVData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CmdDeadlineDuration)
	defer cancel()
	pvs, cmdStr, stdErr, err := s.commands.GetAllPVs(ctx)
	if err != nil {
		s.log.Error(err, fmt.Sprintf("[ScanPVs] unable to scan the PVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return pvs, stdErr, nil
}

func (s *scanner) scanVGs(ctx context.Context) ([]internal.VGData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CmdDeadlineDuration)
	defer cancel()
	vgs, cmdStr, stdErr, err := s.commands.GetAllVGs(ctx)
	if err != nil {
		s.log.Error(err, fmt.Sprintf("[ScanVGs] unable to scan the VGs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return vgs, stdErr, nil
}

func (s *scanner) scanLVs(ctx context.Context) ([]internal.LVData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.CmdDeadlineDuration)
	defer cancel()
	lvs, cmdStr, stdErr, err := s.commands.GetAllLVs(ctx)
	if err != nil {
		s.log.Error(err, fmt.Sprintf("[ScanLVs] unable to scan LVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return lvs, stdErr, nil
}
