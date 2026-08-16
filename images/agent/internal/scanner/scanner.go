/*
	Copyright 2026 Flant JSC

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
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/throttler"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/udev"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

type Scanner interface {
	Run(ctx context.Context,
		log logger.Logger,
		cfg config.Config,
		sdsCache *cache.Cache,
		metrics *monitoring.Metrics,
		// discover is one discovery pass: every discoverer of the agent, in the
		// order they have to run in. The scanner decides when a pass happens, not
		// what is in one — see controller.DiscoverInOrder for the order and why it
		// is a contract.
		//
		// It is expected to be wrapped in controller.WithSingleRetryChain, which is
		// what honours a requeue the pass asks for. The scanner is one of two things
		// that run a pass — the BlockDeviceFilter reconciler is the other — so the
		// chain cannot belong to either of them without becoming two chains.
		discover func(context.Context) (controller.Result, error)) error
}

type scanner struct {
	commands  utils.Commands
	deviceMap *udev.DeviceMap
}

func NewScanner(commands utils.Commands) Scanner {
	return &scanner{commands: commands}
}

func (s *scanner) Run(
	ctx context.Context,
	log logger.Logger,
	cfg config.Config,
	sdsCache *cache.Cache,
	metrics *monitoring.Metrics,
	discover func(context.Context) (controller.Result, error),
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
	if cfg.Features.NetlinkBlockDeviceDiscovery {
		s.deviceMap = udev.NewDeviceMap(udev.DefaultRunUdevDataPath)

		crawlerDevs, crawlErr := s.collectCrawlerDevices(ctx, matcher)
		if crawlErr != nil {
			log.Error(crawlErr, "[RunScanner] Failed to collect crawler devices")
		} else {
			fillErr := s.deviceMap.FillFromCrawler(ctx, crawlerDevs)
			if fillErr != nil {
				log.Error(fillErr, "[RunScanner] Failed to fill device map")
			}
		}

		log.Info(fmt.Sprintf("[RunScanner] initial crawl found %d block devices", s.deviceMap.Len()))
	}

	quit := conn.Monitor(eventChan, errChan, matcher)

	log.Info("[RunScanner] start to listen to events")

	// duration debounces a burst of udev events: every event pushes the fill back
	// by a second so the cache is built once the burst settles.
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

			if cfg.Features.NetlinkBlockDeviceDiscovery {
				if err := s.deviceMap.HandleEvent(device.Action, device.Env); err != nil {
					log.Error(err, fmt.Sprintf("[RunScanner] handle event error: %s", device.String()))
				}
				log.Info(fmt.Sprintf("[HandleEvent] udev event action=%s devname=%s", device.Action.String(), device.Env["DEVNAME"]))
			}

			t.Do(func() {
				log.Info("[RunScanner] start to fill the cache")
				err := s.fillTheCache(ctx, log, sdsCache, cfg, metrics)
				if err != nil {
					log.Error(err, "[RunScanner] unable to fill the cache. Retry")
					go func() {
						eventChan <- device
					}()
					return
				}
				log.Info("[RunScanner] successfully filled the cache")

				err = s.runDiscoveryPass(ctx, log, discover)
				if err != nil {
					log.Error(err, "[RunScanner] unable to run controllers reconciliations")
				}

				log.Info("[RunScanner] successfully ran the controllers reconcile funcs")
			})

		case err := <-errChan:
			log.Error(err, "[RunScanner] Monitor udev event error")
			quit = conn.Monitor(eventChan, errChan, matcher)
			if cfg.Features.NetlinkBlockDeviceDiscovery {
				devs, crErr := s.collectCrawlerDevices(ctx, matcher)
				if crErr != nil {
					log.Error(crErr, "[RunScanner] unable to collect crawler devices")
				} else {
					fillErr := s.deviceMap.FillFromCrawler(ctx, devs)
					if fillErr != nil {
						log.Error(fillErr, "[RunScanner] Failed to fill device map")
					}
				}
				log.Info(fmt.Sprintf("[RunScanner] re-crawl found %d block devices", s.deviceMap.Len()))
			}
			timer.Reset(duration)
			continue

		case <-quit:
			err := errors.New("receive quit signal when monitor udev event")
			log.Error(err, "[RunScanner] unable to read from the event channel")
			return err

		case <-timer.C:
			log.Info("[RunScanner] events ran out. Start to fill the cache")
			err := s.fillTheCache(ctx, log, sdsCache, cfg, metrics)
			if err != nil {
				log.Error(err, "[RunScanner] unable to fill the cache after all events passed. Retry")
				timer.Reset(duration)
				continue
			}

			log.Info("[RunScanner] successfully filled the cache after all events passed")

			err = s.runDiscoveryPass(ctx, log, discover)
			if err != nil {
				log.Error(err, "[RunScanner] unable to run controllers reconciliations")
			}

			log.Info("[RunScanner] successfully ran the controllers reconcile funcs")
		}
	}
}

func (s *scanner) runDiscoveryPass(
	ctx context.Context,
	log logger.Logger,
	discover func(context.Context) (controller.Result, error),
) error {
	log.Info("[runDiscoveryPass] run the discovery pass")

	// The requeue a discoverer asks for is not handled here and does not come back
	// in the Result either. It belongs to controller.WithSingleRetryChain, which
	// wraps this pass in main: the scanner is not the only caller that runs one, and
	// a retry chain per caller is what lvg.maxUnnamedPVPasses cannot survive.
	if _, err := discover(ctx); err != nil {
		log.Error(err, "[runDiscoveryPass] an error occurred while running the discovery pass")
		return err
	}

	log.Info("[runDiscoveryPass] the discovery pass successfully reconciled")

	return nil
}

func (s *scanner) fillTheCache(ctx context.Context, log logger.Logger, cache *cache.Cache, cfg config.Config, metrics *monitoring.Metrics) error {
	// Before any LVM command, because it decides what they are allowed to see:
	// internal.LVMGlobalFilter rejects /dev/loop* — a loop device on a hypervisor
	// is a virtual machine's disk — and this is what exempts the ones the agent
	// attached itself. In-process bookkeeping covers the loops this incarnation
	// created; the full reconcile here is what covers the rest, chiefly the loops
	// that survived a restart of the agent.
	//
	// A failure is logged and not returned: it leaves the previously known set in
	// place, which is the conservative direction. Returning would abort the whole
	// cache fill over losetup, taking down block-device-backed Volume Groups that
	// have nothing to do with loop devices.
	if err := utils.RefreshOwnedLoops(ctx, log, s.commands, cfg.CmdDeadlineDuration); err != nil {
		log.Warning(fmt.Sprintf("[fillTheCache] %v", err))
	}

	// the scan operations order is very important as it guarantees the consistent and reliable data from the node
	realClock := clock.RealClock{}
	now := time.Now()
	lvs, lvsErr, err := s.scanLVs(ctx, log, cfg)
	log.Trace(fmt.Sprintf("[fillTheCache] LVS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	vgs, vgsErr, err := s.scanVGs(ctx, log, cfg)
	log.Trace(fmt.Sprintf("[fillTheCache] VGS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	now = time.Now()
	pvs, pvsErr, err := s.scanPVs(ctx, log, cfg)
	log.Trace(fmt.Sprintf("[fillTheCache] PVS command runs for: %s", realClock.Since(now).String()))
	if err != nil {
		return err
	}

	// VGs were listed before PVs, so a VG created in between — including one this
	// agent has just created for a spec.fileDevices LVMVolumeGroup — is missing
	// from vgs while its PV is already here. Re-list VGs once when a PV points at
	// a VG we do not have: without it the cache stores a PV whose VG is unknown,
	// FindVG returns nil, and the reconciler re-runs create on an existing VG.
	if missingVGs := utils.PVsReferenceUnknownVG(vgs, pvs); missingVGs {
		log.Info("[fillTheCache] a PV references a VG absent from the VG list, re-scanning VGs")
		now = time.Now()
		vgs, vgsErr, err = s.scanVGs(ctx, log, cfg)
		log.Trace(fmt.Sprintf("[fillTheCache] VGS re-scan for late VG runs for: %s", realClock.Since(now).String()))
		if err != nil {
			return err
		}
	}

	now = time.Now()
	var devices []internal.Device
	var devErr bytes.Buffer
	var scanDevErr error
	if cfg.Features.NetlinkBlockDeviceDiscovery {
		devices, devErr, scanDevErr = s.scanDevicesNetlink()
		log.Trace(fmt.Sprintf("[fillTheCache] device scan runs for: %s", realClock.Since(now).String()))
	} else {
		devices, devErr, scanDevErr = s.scanDevices(ctx, log, cfg)
		log.Trace(fmt.Sprintf("[fillTheCache] LSBLK command runs for: %s", realClock.Since(now).String()))
	}

	if scanDevErr != nil {
		return scanDevErr
	}

	// Work out once, for this scan, which Volume Groups are the module's own. Both
	// consumers below need the same answer, and the classification costs host
	// commands only for Volume Groups that live entirely on loop devices — none, on
	// a node that does not use spec.fileDevices.
	//
	// It has to happen before EnsureVGActivation, not after: that call issues
	// `vgchange -ay`, and until spec.fileDevices removed `loop` from
	// LVMGlobalFilter it could not see a loop-backed Volume Group at all. Handing
	// it the raw scan means an image of a former node disk — attached with
	// `losetup` for a restore, or backing a nested cluster — gets activated on the
	// host, once per udev burst, because it carries the managed tag.
	loopVerdicts := utils.ClassifyLoopVGs(ctx, log, s.commands, cfg.CmdDeadlineDuration, vgs, pvs)

	if activated := utils.EnsureVGActivation(ctx, log, s.commands, metrics, vgs, lvs, loopVerdicts, cfg.CmdDeadlineDuration); activated {
		log.Info("[fillTheCache] LVs were activated, re-scanning LVs and VGs")
		now = time.Now()
		lvs, lvsErr, err = s.scanLVs(ctx, log, cfg)
		log.Trace(fmt.Sprintf("[fillTheCache] LVS re-scan runs for: %s", realClock.Since(now).String()))
		if err != nil {
			return err
		}
		now = time.Now()
		vgs, vgsErr, err = s.scanVGs(ctx, log, cfg)
		log.Trace(fmt.Sprintf("[fillTheCache] VGS re-scan runs for: %s", realClock.Since(now).String()))
		if err != nil {
			return err
		}
	}

	log.Debug("[fillTheCache] successfully scanned entities. Starts to fill the cache")

	// lvm.static bundled in /opt/deckhouse/sds/bin is built without udev
	// integration and so reports PVs found on any block device it sees in
	// /dev, including Ceph RBD/DRBD/NBD images that happen to carry LVM
	// signatures from nested LVM (see ADR / PR description). Filter those out
	// before feeding the cache so that the LVG/BD reconcile logic never sees
	// duplicate VG names produced by foreign storage layers.
	//
	// FilterForeignPVs does NOT reject loop devices: the agent manages
	// file-backed loop devices as LVM PVs (spec.fileDevices). PVs of a
	// loop-backed VG the agent does not own are dropped just below by
	// FilterForeignLoopPVs so a guest VM's nested-LVM loop VG cannot collide by
	// name with a managed VG. Ownership is the backing file's name, not the LVM
	// tag — see utils.ClassifyLoopVGs.
	//
	// cfg.CmdDeadlineDuration bounds every per-PV nsenter+readlink call:
	// a hung resolver on a single foreign device cannot block the entire
	// scan loop. This is the same per-command timeout contract every
	// other lvm.static invocation in this function obeys (see PR #290).
	beforePV := len(pvs)
	pvs = utils.FilterForeignPVs(ctx, log, nil, pvs, cfg.CmdDeadlineDuration)
	if dropped := beforePV - len(pvs); dropped > 0 {
		log.Info(fmt.Sprintf("[fillTheCache] dropped %d foreign PV(s) backed by rbd/drbd/nbd devices", dropped))
	}

	// Also drop PVs of foreign, purely loop-backed VGs (nested LVM inside a
	// guest VM's file-backed disk attached via losetup, an image of another
	// node's disk mounted for a restore). Loop PVs are not rejected by
	// FilterForeignPVs because the agent manages its own file-backed loop
	// devices, but a foreign loop VG that shares a name with a managed VG would
	// otherwise be detected as a duplicate by findDuplicateVGNames and take the
	// managed LVMVolumeGroup offline.
	//
	// The verdicts were computed above, before activation: a VG that appeared in
	// the post-activation re-scan carries no verdict and is kept, which is the
	// safe direction — a VG of ours must never be dropped for want of an answer.
	beforeLoopPV := len(pvs)
	pvs = utils.FilterForeignLoopPVs(log, pvs, loopVerdicts)
	if dropped := beforeLoopPV - len(pvs); dropped > 0 {
		log.Info(fmt.Sprintf("[fillTheCache] dropped %d PV(s) of unmanaged loop-backed VG(s)", dropped))
	}

	if len(pvs) < beforePV {
		beforeVG := len(vgs)
		vgs = utils.FilterVGsByPresentPVs(vgs, pvs)
		beforeLV := len(lvs)
		lvs = utils.FilterLVsByPresentVGs(lvs, vgs)
		log.Info(fmt.Sprintf(
			"[fillTheCache] cache pruned to local view: VGs %d -> %d, LVs %d -> %d",
			beforeVG, len(vgs), beforeLV, len(lvs),
		))
	}

	cache.StoreDevices(devices, devErr)
	cache.StorePVs(pvs, pvsErr)
	cache.StoreVGs(vgs, vgsErr)
	cache.StoreLVs(lvs, lvsErr)
	log.Debug("[fillTheCache] successfully filled the cache")
	cache.PrintTheCache(log)

	// Update LVM metrics only for VGs managed by LVMVolumeGroup resources
	managedVGs := cache.GetManagedVGs()
	if errs := metrics.UpdateLVMMetrics(vgs, lvs, managedVGs); len(errs) > 0 {
		for _, err := range errs {
			log.Warning(fmt.Sprintf("[fillTheCache] metrics update error: %v", err))
		}
	}

	return nil
}

func (s *scanner) scanDevices(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.Device, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	devices, cmdStr, stdErr, err := s.commands.GetBlockDevices(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanDevices] unable to scan the devices, cmd: %s", cmdStr))
		return nil, stdErr, err
	}
	return devices, stdErr, nil
}

func (s *scanner) scanDevicesNetlink() ([]internal.Device, bytes.Buffer, error) {
	var stderr bytes.Buffer
	mounts, err := utils.ParseMountInfo(utils.ProcHostMountInfo)
	if err != nil {
		return []internal.Device{}, stderr, fmt.Errorf("[scanDevicesNetlink] failed to parse mountinfo: %v", err)
	}
	devices, errs := s.deviceMap.Snapshot(mounts)
	for _, e := range errs {
		stderr.WriteString(e.Error() + "\n")
	}
	return devices, stderr, nil
}

func (s *scanner) scanPVs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.PVData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	pvs, cmdStr, stdErr, err := s.commands.GetAllPVs(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanPVs] unable to scan the PVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return pvs, stdErr, nil
}

func (s *scanner) scanVGs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.VGData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	vgs, cmdStr, stdErr, err := s.commands.GetAllVGs(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanVGs] unable to scan the VGs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return vgs, stdErr, nil
}

func (s *scanner) scanLVs(ctx context.Context, log logger.Logger, cfg config.Config) ([]internal.LVData, bytes.Buffer, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.CmdDeadlineDuration)
	defer cancel()
	lvs, cmdStr, stdErr, err := s.commands.GetAllLVs(ctx)
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanLVs] unable to scan LVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return lvs, stdErr, nil
}

func (s *scanner) collectCrawlerDevices(ctx context.Context, matcher netlink.Matcher) ([]crawler.Device, error) {
	queue := make(chan crawler.Device)
	errs := make(chan error)
	crawler.ExistingDevices(queue, errs, matcher)

	result := make([]crawler.Device, 0)
	var crawlErr error

	for queue != nil && errs != nil {
		select {
		case <-ctx.Done():
			if crawlErr != nil {
				return result, errors.Join(crawlErr, ctx.Err())
			}
			return result, ctx.Err()
		case dev, ok := <-queue:
			if !ok {
				queue = nil
				continue
			}
			result = append(result, dev)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil {
				crawlErr = errors.Join(crawlErr, err)
			}
		}
	}

	return result, crawlErr
}
