package controller

import (
	"context"
	"errors"
	"fmt"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/pkg/logger"
	"sds-node-configurator/pkg/monitoring"
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

func getLVMVolumeGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, namespace, name string) (*v1alpha1.LvmVolumeGroup, error) {
	obj := &v1alpha1.LvmVolumeGroup{}
	start := time.Now()
	err := cl.Get(ctx, client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}, obj)
	metrics.ApiMethodsDuration(watcherLVMVGCtrlName, "get").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(watcherLVMVGCtrlName, "get").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(watcherLVMVGCtrlName, "get").Inc()
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

func updateLVMVolumeGroupStatus(ctx context.Context, cl client.Client, metrics monitoring.Metrics, name, namespace, message, health string) error {
	obj := &v1alpha1.LvmVolumeGroup{}

	start := time.Now()
	err := cl.Get(ctx, client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}, obj)
	metrics.ApiMethodsDuration(watcherLVMVGCtrlName, "get").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(watcherLVMVGCtrlName, "get").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(watcherLVMVGCtrlName, "get").Inc()
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

	start = time.Now()
	err = cl.Update(ctx, obj)
	metrics.ApiMethodsDuration(watcherLVMVGCtrlName, "update").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(watcherLVMVGCtrlName, "update").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(watcherLVMVGCtrlName, "update").Inc()
		return err
	}
	return nil
}

func getBlockDevice(ctx context.Context, cl client.Client, metrics monitoring.Metrics, namespace, name string) (*v1alpha1.BlockDevice, error) {
	obj := &v1alpha1.BlockDevice{}

	start := time.Now()
	err := cl.Get(ctx, client.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}, obj)
	metrics.ApiMethodsDuration(watcherLVMVGCtrlName, "get").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(watcherLVMVGCtrlName, "get").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(watcherLVMVGCtrlName, "get").Inc()
		return nil, err
	}
	return obj, nil
}

func ValidationLVMGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, namespace, nodeName string) (bool, *StatusLVMVolumeGroup, error) {
	status := StatusLVMVolumeGroup{}
	if lvmVolumeGroup == nil {
		return false, nil, errors.New("lvmVolumeGroup in empty")
	}

	membership := 0
	if lvmVolumeGroup.Spec.Type == Local {
		for _, blockDev := range lvmVolumeGroup.Spec.BlockDeviceNames {
			device, err := getBlockDevice(ctx, cl, metrics, namespace, blockDev)
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

func ValidationTypeLVMGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, lvmVolumeGroup *v1alpha1.LvmVolumeGroup, l logger.Logger) (extendPV, shrinkPV []string, err error) {
	pvs, cmdStr, _, err := utils.GetAllPVs()
	l.Debug(fmt.Sprintf("GetAllPVs exec cmd: %s", cmdStr))
	if err != nil {
		return nil, nil, err
	}

	for _, devName := range lvmVolumeGroup.Spec.BlockDeviceNames {
		dev, err := getBlockDevice(ctx, cl, metrics, lvmVolumeGroup.Namespace, devName)
		if err != nil {
			return nil, nil, err
		}

		if dev.Status.Consumable == true {
			extendPV = append(extendPV, dev.Status.Path)
			continue
		}

		if dev.Status.ActualVGNameOnTheNode != lvmVolumeGroup.Spec.ActualVGNameOnTheNode && (len(dev.Status.VGUuid) != 0) {
			return nil, nil, nil
			// validation fail, send message => LVG  ?
		}
	}

	var flag bool

	for _, pv := range pvs {
		if pv.VGName == lvmVolumeGroup.Spec.ActualVGNameOnTheNode {
			flag = false
			for _, devName := range lvmVolumeGroup.Spec.BlockDeviceNames {
				dev, err := getBlockDevice(ctx, cl, metrics, lvmVolumeGroup.Namespace, devName)
				if err != nil {
					return nil, nil, err
				}

				if pv.PVUuid == dev.Status.PVUuid {
					flag = true
				}
			}
		}
		if !flag {
			shrinkPV = append(shrinkPV, pv.PVName)
		}
	}
	return extendPV, shrinkPV, nil
}

func CreateEventLVMVolumeGroup(ctx context.Context, cl client.Client, metrics monitoring.Metrics, reason, actions, nodeName string, obj *v1alpha1.LvmVolumeGroup) error {
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
		ReportingController: watcherLVMVGCtrlName,
		Message:             "Event Message",
	}

	start := time.Now()
	err := cl.Create(ctx, e)
	metrics.ApiMethodsDuration(watcherLVMVGCtrlName, "create").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.ApiMethodsExecutionCount(watcherLVMVGCtrlName, "create").Inc()
	if err != nil {
		metrics.ApiMethodsErrors(watcherLVMVGCtrlName, "create").Inc()
		return err
	}
	return nil
}

func DeleteVG(vgName string, log logger.Logger, metrics monitoring.Metrics) error {
	// if VG exist
	start := time.Now()
	vgs, command, _, err := utils.GetAllVGs()
	metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "vgs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "vgs").Inc()
		log.Error(err, "GetAllVGs "+command)
		return err
	}

	if len(vgs) == 0 {
		return nil
	}

	// if exist LV in VG
	start = time.Now()
	lvs, command, _, err := utils.GetAllLVs()
	metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "lvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "lvs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "lvs").Inc()
		log.Error(err, "GetAllLVs "+command)
		return err
	}

	for _, lv := range lvs {
		if lv.VGName == vgName {
			return fmt.Errorf(fmt.Sprintf(`[ERROR] VG "%s" contains LV "%s"`, vgName, lv.LVName))
		}
	}

	start = time.Now()
	pvs, command, _, err := utils.GetAllPVs()
	metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "pvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "pvs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "pvs").Inc()
		log.Error(err, "RemoveVG "+command)
		return err
	}

	start = time.Now()
	command, err = utils.RemoveVG(vgName)
	metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "vgremove").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "vgremove").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "vgremove").Inc()
		log.Error(err, "RemoveVG "+command)
		return err
	}

	var listDeletingPV []string

	for _, pv := range pvs {
		if pv.VGName == vgName {
			listDeletingPV = append(listDeletingPV, pv.PVName)
		}
	}

	start = time.Now()
	command, err = utils.RemovePV(listDeletingPV)
	metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "pvremove").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "pvremove").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "pvremove").Inc()
		log.Error(err, "RemovePV "+command)
		return err
	}

	return nil
}

func ExistVG(vgName string, log logger.Logger, metrics monitoring.Metrics) (bool, error) {
	start := time.Now()
	vg, command, _, err := utils.GetAllVGs()
	metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "vgs").Inc()
	log.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "vgs").Inc()
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

func ConsumableAllDevices(ctx context.Context, cl client.Client, metrics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup) (bool, error) {
	if group == nil {
		return false, fmt.Errorf("group is empty")
	}
	countConsumable := 0
	for _, device := range group.Spec.BlockDeviceNames {
		d, err := getBlockDevice(ctx, cl, metrics, group.Namespace, device)
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

func GetPathsConsumableDevicesFromLVMVG(ctx context.Context, cl client.Client, mertics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup) ([]string, error) {
	if group == nil {
		return nil, fmt.Errorf("group is empty")
	}
	var paths []string

	for _, device := range group.Spec.BlockDeviceNames {
		d, err := getBlockDevice(ctx, cl, mertics, group.Namespace, device)
		paths = append(paths, d.Status.Path)
		if err != nil {
			return nil, err
		}
	}
	return paths, nil
}

func ExtendVGComplex(metrics monitoring.Metrics, extendPVs []string, VGName string, l logger.Logger) error {
	for _, pvPath := range extendPVs {
		start := time.Now()
		command, err := utils.CreatePV(pvPath)
		metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "pvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "pvcreate").Inc()
		l.Debug(command)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "pvcreate").Inc()
			l.Error(err, "CreatePV ")
			return err
		}
	}

	start := time.Now()
	command, err := utils.ExtendVG(VGName, extendPVs)
	metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "vgextend").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "vgextend").Inc()
	l.Debug(command)
	if err != nil {
		metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "vgextend").Inc()
		l.Error(err, "ExtendVG ")
		return err
	}
	return nil
}

func CreateVGComplex(ctx context.Context, cl client.Client, metrics monitoring.Metrics, group *v1alpha1.LvmVolumeGroup, l logger.Logger) error {
	AllConsumable, err := ConsumableAllDevices(ctx, cl, metrics, group)
	if err != nil {
		l.Error(err, " error ConsumableAllDevices")
		return err
	}
	if !AllConsumable {
		l.Error(err, " error not all devices is consumable")
		return err
	}
	paths, err := GetPathsConsumableDevicesFromLVMVG(ctx, cl, metrics, group)
	if err != nil {
		l.Error(err, "error GetPathsConsumableDevicesFromLVMVG")
		return err
	}

	for _, path := range paths {
		p := path
		start := time.Now()
		command, err := utils.CreatePV(p)
		metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "pvcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "pvcreate").Inc()
		l.Debug(command)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "pvcreate").Inc()
			l.Error(err, "CreatePV "+p)
			return err
		}
	}

	if group.Spec.Type == Local {
		start := time.Now()
		cmd, err := utils.CreateVGLocal(group.Spec.ActualVGNameOnTheNode, group.Name, paths)
		metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "vgcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "vgcreate").Inc()
		l.Debug(cmd)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "vgcreate").Inc()
			l.Error(err, "error CreateVGLocal")
			return err
		}
	}

	if group.Spec.Type == Shared {
		start := time.Now()
		cmd, err := utils.CreateVGShared(group.Spec.ActualVGNameOnTheNode, group.Name, paths)
		metrics.UtilsCommandsDuration(watcherLVMVGCtrlName, "vgcreate").Observe(metrics.GetEstimatedTimeInSeconds(start))
		metrics.UtilsCommandsExecutionCount(watcherLVMVGCtrlName, "vgcreate").Inc()
		l.Debug(cmd)
		if err != nil {
			metrics.UtilsCommandsErrorsCount(watcherLVMVGCtrlName, "vgcreate").Inc()
			l.Error(err, "error CreateVGShared")
			return err
		}
	}
	return nil
}
