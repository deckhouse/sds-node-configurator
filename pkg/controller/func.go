package controller

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"os/exec"
	"storage-configurator/api/v1alpha1"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/klog"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func ScanBlockDevices(ctx context.Context, kc kclient.Client, nodeName string, interval int, nodeUID string, deviceCount prometheus.GaugeVec) error {
	candiCh := make(chan []Candidate)
	errCh := make(chan error)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				candidates, err := getCandidates(nodeName)
				if err != nil {
					klog.Error("fatal error, cannot cmd  %w", err)
					errCh <- err
					return
				}
				candiCh <- candidates
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-errCh:
			return fmt.Errorf("cannot get storage pool candidates: %w", err)

		case cand := <-candiCh:

			candidate := map[string]v1alpha1.BlockDeviceStatus{}
			for _, j := range cand {
				candidate[CreateUniqNameDevice(j, nodeName)] = v1alpha1.BlockDeviceStatus{
					NodeName:  nodeName,
					ID:        j.ID,
					Path:      j.Path,
					Size:      j.Size,
					Model:     j.Model,
					MachineID: MachineID,
				}
			}

			// reconciliation of local devices with an external list
			// read kubernetes list device
			listBlockDevices, err := GetListBlockDevices(ctx, kc)
			if err != nil {
				klog.Errorf(err.Error())
			}

			// create device
			for i, j := range candidate {
				if _, ok := listBlockDevices[i]; ok {
					continue
				} else {
					err = CreateBlockDeviceObject(ctx, kc, Candidate{
						NodeName: nodeName,
						ID:       j.ID,
						Path:     j.Path,
						Size:     j.Size,
						Model:    j.Model,
					}, nodeName, i, nodeUID)
					if err != nil {
						klog.Errorf(err.Error())
					}
					klog.Info("create device: ", i)

					listBlockDevices[i] = v1alpha1.BlockDeviceStatus{
						NodeName:  nodeName,
						ID:        j.ID,
						Path:      j.Path,
						Size:      j.Size,
						Model:     j.Model,
						MachineID: j.MachineID,
					}
				}
			}

			// delete device
			for i, j := range listBlockDevices {
				if _, ok := candidate[i]; ok {
					continue
				} else {
					if j.NodeName == nodeName {
						err = DeleteBlockDeviceObject(ctx, kc, i)
						if err != nil {
							klog.Errorf(err.Error())
						}
						delete(listBlockDevices, i)
						klog.Info("delete device: ", i)
					}
				}
			}
			// metrics
			deviceCount.With(prometheus.Labels{"device": "count"}).Set(float64(len(candidate)))
		}
	}
}

func getCandidates(nodeName string) ([]Candidate, error) {
	var candidates []Candidate
	var candidateHandlers = []CandidateHandler{
		{
			Command:   lsblkCommand,
			ParseFunc: parseFreeBlockDev,
		},
	}
	for _, handler := range candidateHandlers {
		cmd := exec.Command(handler.Command[0], handler.Command[1:]...)

		var outs, errs bytes.Buffer
		cmd.Stdout = &outs
		cmd.Stderr = &errs

		err := cmd.Run()
		if err != nil {
			return nil, fmt.Errorf("exec lsblk error out %w", err)
		}

		cs, err := handler.ParseFunc(nodeName, outs.Bytes())
		if err != nil {
			return nil, fmt.Errorf("faled to read %s. Error was %s", err, err.Error())
		}
		candidates = append(candidates, cs...)
	}
	return candidates, nil
}

func parseFreeBlockDev(nodeName string, out []byte) ([]Candidate, error) {
	var devices Devices
	err := json.Unmarshal(out, &devices)
	if err != nil {
		return nil, fmt.Errorf("parse out lsblk error %w", err)
	}

	tempMapKName := make(map[string]int)
	tempMapPKName := make(map[string]int)
	var r []Candidate

	for i, j := range devices.BlockDevices {
		tempMapKName[j.KName] = i
		tempMapPKName[j.PkName] = i
	}

	for i, j := range devices.BlockDevices {
		if len(j.MountPoint) == 0 && !j.HotPlug && !strings.HasPrefix(j.Name, DRBDName) && j.Type != LoopDeviceType && len(j.FSType) == 0 {
			_, ok := tempMapPKName[j.KName]
			if !ok {
				r = append(r, Candidate{
					NodeName:   nodeName,
					ID:         j.Wwn,
					Name:       j.Name,
					Path:       devices.BlockDevices[i].Name,
					Size:       devices.BlockDevices[i].Size,
					Model:      devices.BlockDevices[i].Model,
					MountPoint: devices.BlockDevices[i].MountPoint,
					FSType:     devices.BlockDevices[i].FSType,
				})
			}
		}
	}
	return r, nil
}

func CreateBlockDeviceObject(ctx context.Context, kc kclient.Client, can Candidate, nodeName, deviceName, nodeUID string) error {
	device := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: deviceName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.OwnerReferencesAPIVersion,
					Kind:       v1alpha1.Node,
					Name:       nodeName,
					UID:        types.UID(nodeUID),
				},
			},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.OwnerReferencesKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		Status: v1alpha1.BlockDeviceStatus{
			NodeName:  nodeName,
			ID:        can.ID,
			Path:      can.Path,
			Size:      can.Size,
			Model:     can.Model,
			MachineID: MachineID,
		},
	}

	err := kc.Create(ctx, device)
	if err != nil {
		return fmt.Errorf("create block device %w", err)
	}
	return nil
}

func GetListBlockDevices(ctx context.Context, kc kclient.Client) (map[string]v1alpha1.BlockDeviceStatus, error) {

	deviceList := make(map[string]v1alpha1.BlockDeviceStatus)

	listDevice := &v1alpha1.BlockDeviceList{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.OwnerReferencesKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
		ListMeta: metav1.ListMeta{},
		Items:    []v1alpha1.BlockDevice{},
	}
	err := kc.List(ctx, listDevice)
	if err != nil {
		return nil, fmt.Errorf("list block devices %w", err)
	}
	for _, j := range listDevice.Items {
		deviceList[j.Name] = j.Status
	}
	return deviceList, nil
}

func CreateUniqNameDevice(can Candidate, nodeName string) string {
	temp := fmt.Sprintf("%s%s%s%s%s", nodeName, can.ID, can.Path, can.Size, can.Model)
	s := fmt.Sprintf("dev-%x", sha1.Sum([]byte(temp)))
	return s
}

func DeleteBlockDeviceObject(ctx context.Context, kc kclient.Client, deviceName string) error {
	device := &v1alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: deviceName,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1.OwnerReferencesKind,
			APIVersion: v1alpha1.TypeMediaAPIVersion,
		},
	}

	err := kc.Delete(ctx, device)
	if err != nil {
		return fmt.Errorf("delete block device %w", err)
	}
	return nil
}
