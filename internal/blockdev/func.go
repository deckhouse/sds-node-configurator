package blockdev

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"os/exec"
	"storage-configurator/api/v2alpha1"
	"storage-configurator/pkg/utils/errors/scerror"
	"strings"
	"time"

	"k8s.io/klog"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func ScanBlockDevices(ctx context.Context, kc kclient.Client, nodeName string, interval int, nodeUID string) error {
	candiCh := make(chan Candidate)
	errCh := make(chan error)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)

	go func() {
		seen := make(map[string]struct{})

		for {
			select {
			case <-ticker.C:
				candidates, err := getCandidates(nodeName)
				if err != nil {
					// only fatal error, cannot cmd
					errCh <- err
					return
				}
				// if duplicate
				for _, cand := range candidates {
					// seen[cand.UUID.String()+"+"+cand.Name]
					// if duplicate

					//if _, yes := seen[cand.Name]; yes {
					//	continue
					//}
					// seen[cand.UUID.String()+"+"+cand.Name]
					//
					seen[cand.Name] = struct{}{}
					candiCh <- cand
				}
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

			//klog.Info("candidate : ", cand.Name)

			hashLocalDevice := createUniqNameDevice(cand, nodeName)
			fmt.Println("---------- HASH LOCAL DEVICE ---------", hashLocalDevice, cand.Path, cand.Name)

			// Get Devices Node
			listBlockDevices, err := getListBlockDevices(ctx, kc)
			if err != nil {
				klog.Errorf(err.Error())
			}

			// Нужно уникальное имя для поиска в устройствах
			//status := listBlockDevices[""]
			//fmt.Println(status.Path, status.Size)

			fmt.Printf("%v", listBlockDevices)

			//Create Device
			err = createBlockDeviceObject(ctx, kc, cand, nodeName, nodeUID)
			if err != nil {
				klog.Errorf("error create DEVICE ", err)
			}

			klog.Infof("create DEVICE")

			//todo
			// Delete -- get -> etcd --> ( NAME /dev/sda/  <=> kube get ) --> Delete CR
			// Edit ^

			if cand.SkipReason != "" {
				klog.Infof("Skip %s as it %s", cand.Name, cand.SkipReason)
				continue
			}
		}
	}
}

func getCandidates(nodeName string) ([]Candidate, error) {
	var candidates []Candidate
	var candidateHandlers = []CandidateHandler{
		{
			Name:      "deviceX",
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
			return nil, fmt.Errorf(scerror.ExeLSBLK, err, errs.String())
		}

		cs, err := handler.ParseFunc(nodeName, outs.Bytes())
		if err != nil {
			return nil, fmt.Errorf("faled to read %s devices: %s. Error was %s", handler.Name, err, err.Error())
		}
		candidates = append(candidates, cs...)
	}
	return candidates, nil
}

func parseFreeBlockDev(nodeName string, out []byte) ([]Candidate, error) {
	var devices Devices
	err := json.Unmarshal(out, &devices)
	if err != nil {
		return nil, fmt.Errorf(scerror.ParseOutlsblkError+"%w", err)
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
					NodeName: nodeName,
					ID:       j.Wwn,
					//Name:       buildNameDevices(devices.BlockDevices[i].Name[1:]),
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

func buildNameDevices(name string) string {
	var tempName strings.Builder
	var tempBuff []byte
	tempName.WriteString(strings.Replace(name, "/", "-", -1))
	tempName.WriteString("-")
	tempName.Write(tempBuff)
	return tempName.String()
}

func createBlockDeviceObject(ctx context.Context, kc kclient.Client, can Candidate, nodeName, nodeUID string) error {
	device := &v2alpha1.BlockDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name: createUniqNameDevice(can, nodeName),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v2alpha1.OwnerReferencesAPIVersion,
					Kind:       v2alpha1.Node,
					Name:       nodeName,
					UID:        types.UID(nodeUID),
				},
			},
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       v2alpha1.OwnerReferencesKind,
			APIVersion: v2alpha1.TypeMediaAPIVersion,
		},
		Status: v2alpha1.BlockDeviceStatus{
			NodeName:  nodeName,
			ID:        can.ID,
			Path:      can.Path,
			Size:      can.Size,
			Model:     can.Model,
			MachineID: "machine-ID",
		},
	}

	err := kc.Create(ctx, device)
	if err != nil {
		return fmt.Errorf(scerror.CreateBlockDevice+"%w", err)
	}
	return nil
}

func getListBlockDevices(ctx context.Context, kc kclient.Client) (map[string]v2alpha1.BlockDeviceStatus, error) {

	deviceList := make(map[string]v2alpha1.BlockDeviceStatus)

	listDevice := &v2alpha1.BlockDeviceList{
		TypeMeta: metav1.TypeMeta{
			Kind:       v2alpha1.OwnerReferencesKind,
			APIVersion: v2alpha1.TypeMediaAPIVersion,
		},
		ListMeta: metav1.ListMeta{},
		Items:    []v2alpha1.BlockDevice{},
	}
	err := kc.List(ctx, listDevice)
	if err != nil {
		return nil, fmt.Errorf(scerror.GetListBlockDevices+"%w", err)
	}
	for _, j := range listDevice.Items {
		deviceList[j.Name] = j.Status
	}
	return deviceList, nil
}

func createUniqNameDevice(can Candidate, nodeName string) string {
	temp := fmt.Sprintf("%s%s%s%s%s", nodeName, can.ID, can.Path, can.Size, can.Model)
	return fmt.Sprintf("dev-%x", sha1.Sum([]byte(temp)))
}

func compareListDevices() {

}
