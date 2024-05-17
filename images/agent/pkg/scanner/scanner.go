package scanner

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/pilebones/go-udev/netlink"
	"sds-node-configurator/config"
	"sds-node-configurator/internal"
	"sds-node-configurator/pkg/cache"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/throttler"
	"sds-node-configurator/pkg/utils"
	"time"
)

func RunScanner(log logger.Logger, cfg config.Options, sdsCache *cache.Cache) error {
	log.Info("[RunScanner] starts the work")

	t := throttler.New(cfg.ThrottleInterval * time.Second)

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

	timer := time.NewTimer(1 * time.Second)
	for {
		select {
		case device, open := <-eventChan:
			timer.Reset(1 * time.Second)
			log.Debug(fmt.Sprintf("[RunScanner] event triggered for device: %s", device.Env["DEVNAME"]))
			log.Trace(fmt.Sprintf("[RunScanner] device from the event: %s", device.String()))
			if !open {
				err := errors.New("EventChan has been closed when monitor udev event")
				log.Error(err, "[RunScanner] unable to read from the event channel")
				return err
			}

			t.Do(func() {
				log.Info("[RunScanner] start to fill the cache")
				err := fillTheCache(log, sdsCache)
				if err != nil {
					log.Error(err, "[RunScanner] unable to fill the cache")
					return
				}

				log.Info("[RunScanner] successfully filled the cache")
			})

		case err := <-errChan:
			log.Error(err, "[RunScanner] Monitor udev event error")
			return err

		case <-quit:
			err := errors.New("receive quit signal when monitor udev event")
			log.Error(err, "[RunScanner] unable to read from the event channel")
			return err

		case <-timer.C:
			log.Info("[RunScanner] events ran out. Start to fill the cache")
			err := fillTheCache(log, sdsCache)
			if err != nil {
				log.Error(err, "[RunScanner] unable to fill the cache after all events passed")
				break
			}
			log.Info("[RunScanner] successfully filled the cache after all events passed")
		}
	}
}

func fillTheCache(log logger.Logger, cache *cache.Cache) error {
	devices, devErr, err := scanDevices(log)
	if err != nil {
		return err
	}

	pvs, pvsErr, err := scanPVs(log)
	if err != nil {
		return err
	}

	vgs, vgsErr, err := scanVGs(log)
	if err != nil {
		return err
	}

	lvs, lvsErr, err := scanLVs(log)
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

func scanDevices(log logger.Logger) ([]internal.Device, bytes.Buffer, error) {
	devices, cmdStr, stdErr, err := utils.GetBlockDevices()
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanDevices] unable to scan the devices, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return devices, stdErr, nil
}

func scanPVs(log logger.Logger) ([]internal.PVData, bytes.Buffer, error) {
	pvs, cmdStr, stdErr, err := utils.GetAllPVs()
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanPVs] unable to scan the PVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return pvs, stdErr, nil
}

func scanVGs(log logger.Logger) ([]internal.VGData, bytes.Buffer, error) {
	vgs, cmdStr, stdErr, err := utils.GetAllVGs()
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanVGs] unable to scan the VGs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return vgs, stdErr, nil
}

func scanLVs(log logger.Logger) ([]internal.LVData, bytes.Buffer, error) {
	lvs, cmdStr, stdErr, err := utils.GetAllLVs()
	if err != nil {
		log.Error(err, fmt.Sprintf("[ScanLVs] unable to scan LVs, cmd: %s", cmdStr))
		return nil, stdErr, err
	}

	return lvs, stdErr, nil
}
