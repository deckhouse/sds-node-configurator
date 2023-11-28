package controller

import (
	"context"
	"errors"
	"fmt"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/utils"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StatusLVMVolumeGroup struct {
	Health  string
	Phase   string
	Message string
}

func getLVMVolumeGroup(ctx context.Context, cl client.Client, namespace, name string) (*v1alpha1.LvmVolumeGroup, error) {
	obj := &v1alpha1.LvmVolumeGroup{}
	err := cl.Get(ctx, client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func updateLVMVolumeGroup(ctx context.Context, cl client.Client, group *v1alpha1.LvmVolumeGroup) error {
	err := cl.Update(ctx, group)
	if err != nil {
		return err
	}
	return nil
}

func updateLVMVolumeGroupStatus(ctx context.Context, cl client.Client, name, namespace, message, health string) error {
	obj := &v1alpha1.LvmVolumeGroup{}
	err := cl.Get(ctx, client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}, obj)
	if err != nil {
		return err
	}

	if obj.Status.Health == health && health == Operational {
		return nil
	}

	if obj.Status.Health == health && obj.Status.Message == message {
		return nil
	}

	obj.Status.Health = health
	obj.Status.Message = message

	err = cl.Update(ctx, obj)
	if err != nil {
		return err
	}
	return nil
}

func deleteLVMVolumeGroup(ctx context.Context, cl client.Client, name string) error {
	req := &v1alpha1.LvmVolumeGroup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apiextensions.k8s.io/v1",
			Kind:       "LvmVolumeGroup",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	err := cl.Delete(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func getBlockDevice(ctx context.Context, cl client.Client, namespace, name string) (*v1alpha1.BlockDevice, error) {
	obj := &v1alpha1.BlockDevice{}
	err := cl.Get(ctx, client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func ValidationLVMGroup(ctx context.Context, cl client.Client, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, namespace, nodeName string) (bool, *StatusLVMVolumeGroup, error) {
	status := StatusLVMVolumeGroup{}
	if lvmVolumeGroup == nil {
		return false, nil, errors.New("lvmVolumeGroup in empty")
	}

	membership := 0
	if lvmVolumeGroup.Spec.Type == Local {
		for _, blockDev := range lvmVolumeGroup.Spec.BlockDeviceNames {
			device, err := getBlockDevice(ctx, cl, namespace, blockDev)
			if err != nil {
				status.Health = ""
				return false, &status, err
			}
			if device.Status.NodeName == nodeName {
				membership++
			}
		}

		if membership == len(lvmVolumeGroup.Spec.BlockDeviceNames) {
			return true, &status, nil
		}

		// TODO devices not affiliated ?
		if membership != len(lvmVolumeGroup.Spec.BlockDeviceNames) {
			status.Health = NoOperational
			status.Phase = Failed
			status.Message = "one or some devices not affiliated this node"
			return false, &status, nil
		}

		if len(lvmVolumeGroup.Spec.BlockDeviceNames)-membership == len(lvmVolumeGroup.Spec.BlockDeviceNames) {
			status.Health = NoOperational
			status.Phase = Failed
			status.Message = "no one devices not affiliated this node"
			return false, &status, nil
		}
	}

	if lvmVolumeGroup.Spec.Type == Shared {
		if len(lvmVolumeGroup.Spec.BlockDeviceNames) > 1 {
			status.Health = NoOperational
			status.Phase = Failed
			status.Message = "LVMVolumeGroup Type != shared"
			return false, &status, errors.New(status.Message)
		}

		if len(lvmVolumeGroup.Spec.BlockDeviceNames) == 1 && (len(lvmVolumeGroup.Spec.BlockDeviceNames)-membership) == 0 {
			return true, &status, nil
		}
	}
	return false, &status, nil
}

func ValidationTypeLVMGroup(ctx context.Context, cl client.Client, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, l logger.Logger) (extendPV, shrinkPV []string, err error) {
	pvs, cmdStr, _, err := utils.GetAllPVs()
	l.Debug(fmt.Sprintf("GetAllPVs exec cmd: %s", cmdStr))
	if err != nil {
		return nil, nil, nil
	}

	for _, devName := range lvmVolumeGroup.Spec.BlockDeviceNames {
		dev, err := getBlockDevice(ctx, cl, lvmVolumeGroup.Namespace, devName)
		if err != nil {
			return nil, nil, nil
		}

		for _, pv := range pvs {
			if dev.Status.LvmVolumeGroupName == pv.VGName || dev.Status.Consumable == true {
				extendPV = append(extendPV, pv.PVName)
			}

			if dev.Status.LvmVolumeGroupName != pv.PVName {
				shrinkPV = append(shrinkPV, pv.PVName)
			}
		}
	}
	return extendPV, shrinkPV, nil
}

func CreateEventLVMVolumeGroup(ctx context.Context, cl client.Client, reason, actions, nodeName string, obj *v1alpha1.LvmVolumeGroup) error {
	e := &v1.Event{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Event",
			APIVersion: "events.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: obj.Name + "-",
			Namespace:    nameSpaceEvent,
		},
		Reason: reason,
		InvolvedObject: v1.ObjectReference{
			Kind:       obj.Kind,
			Name:       obj.Name,
			UID:        obj.UID,
			APIVersion: "apiextensions.k8s.io/v1",
		},
		Type: v1.EventTypeNormal,
		EventTime: metav1.MicroTime{
			Time: time.Now(),
		},
		Action:              actions,
		ReportingInstance:   nodeName,
		ReportingController: lvmVolumeGroupName,
		Message:             "Event Message",
	}

	err := cl.Create(ctx, e)
	if err != nil {
		return err
	}
	return nil
}

func DeleteVG(vgName string, log logger.Logger) error {
	// if VG exist
	vgs, command, _, err := utils.GetAllVGs()
	log.Debug(command)
	if err != nil {
		log.Error(err, "GetAllVGs "+command)
		return err
	}

	if len(vgs) == 0 {
		return nil
	}

	// if exist LV in VG
	lvs, command, _, err := utils.GetAllLVs()
	log.Debug(command)
	if err != nil {
		log.Error(err, "GetAllLVs "+command)
		return err
	}

	for _, lv := range lvs {
		if lv.VGName == vgName {
			return fmt.Errorf(fmt.Sprintf(`[ERROR] VG "%s" contains LV "%s"`, vgName, lv.LVName))
		}
	}

	pvs, command, _, err := utils.GetAllPVs()
	log.Debug(command)
	if err != nil {
		log.Error(err, "RemoveVG "+command)
		return err
	}

	command, err = utils.RemoveVG(vgName)
	log.Debug(command)
	if err != nil {
		log.Error(err, "RemoveVG "+command)
		return err
	}

	var listDeletingPV []string

	for _, pv := range pvs {
		if pv.VGName == vgName {
			listDeletingPV = append(listDeletingPV, pv.PVName)
		}
	}

	command, err = utils.RemovePV(listDeletingPV)
	log.Debug(command)
	if err != nil {
		log.Error(err, "RemovePV "+command)
		return err
	}

	return nil
}

func ExistVG(vgName string, log logger.Logger) (bool, error) {
	vg, command, _, err := utils.GetAllVGs()
	log.Debug(command)
	if err != nil {
		log.Error(err, " error CreateEventLVMVolumeGroup")
		return false, err
	}

	for _, v := range vg {
		if v.VGName == vgName {
			return true, nil
		}
	}
	return false, nil
}

func ConsumableAllDevices(ctx context.Context, cl client.Client, group *v1alpha1.LvmVolumeGroup) (bool, error) {
	if group == nil {
		return false, fmt.Errorf("group is empty")
	}
	countConsumable := 0
	for _, device := range group.Spec.BlockDeviceNames {
		d, err := getBlockDevice(ctx, cl, group.Namespace, device)
		if err != nil {
			return false, err
		}
		if d.Status.Consumable == true {
			countConsumable++
		}
	}
	if len(group.Spec.BlockDeviceNames) == countConsumable {
		return true, nil
	}
	return true, nil
}

func GetPathsConsumableDevicesFromLVMVG(ctx context.Context, cl client.Client, group *v1alpha1.LvmVolumeGroup) ([]string, error) {
	if group == nil {
		return nil, fmt.Errorf("group is empty")
	}
	var paths []string

	for _, device := range group.Spec.BlockDeviceNames {
		d, err := getBlockDevice(ctx, cl, group.Namespace, device)
		paths = append(paths, d.Status.Path)
		if err != nil {
			return nil, err
		}
	}
	return paths, nil
}

func ExtendVGComplex(extendPVs []string, VGName string, l logger.Logger) error {

	for _, pvPath := range extendPVs {
		command, err := utils.CreatePV(pvPath)
		l.Debug(command)
		if err != nil {
			l.Error(err, "CreatePV ")
			return err
		}
	}

	command, err := utils.ExtendVG(VGName, extendPVs)
	l.Debug(command)
	if err != nil {
		l.Error(err, "ExtendVG ")
		return err
	}
	return nil
}

func CreateVGComplex(ctx context.Context, cl client.Client, group *v1alpha1.LvmVolumeGroup, l logger.Logger) error {
	AllConsumable, err := ConsumableAllDevices(ctx, cl, group)
	if err != nil {
		l.Error(err, " error ConsumableAllDevices")
		return err
	}
	if !AllConsumable {
		l.Error(err, " error not all devices is consumable")
		return err
	}
	paths, err := GetPathsConsumableDevicesFromLVMVG(ctx, cl, group)
	if err != nil {
		l.Error(err, "error GetPathsConsumableDevicesFromLVMVG")
		return err
	}

	for _, path := range paths {
		p := path
		command, err := utils.CreatePV(p)
		l.Debug(command)
		if err != nil {
			l.Error(err, "CreatePV "+p)
			return err
		}
	}

	if group.Spec.Type == Local {
		cmd, err := utils.CreateVGLocal(group.Spec.ActualVGNameOnTheNode, group.Name, paths)
		l.Debug(cmd)
		if err != nil {
			l.Error(err, "error CreateVGLocal")
			return err
		}
	}

	if group.Spec.Type == Shared {
		cmd, err := utils.CreateVGShared(group.Spec.ActualVGNameOnTheNode, group.Name, paths)
		l.Debug(cmd)
		if err != nil {
			l.Error(err, "error CreateVGShared")
			return err
		}
	}
	return nil
}
