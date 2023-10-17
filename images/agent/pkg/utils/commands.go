package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"storage-configurator/api/v1alpha1"
	"storage-configurator/internal"
	"strings"
)

func GetBlockDevices() ([]internal.Device, string, error) {
	var outs bytes.Buffer
	cmd := exec.Command("lsblk", "-J", "-lpf", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,FSTYPE,TYPE,WWN,KNAME,PKNAME,ROTA")
	cmd.Stdout = &outs

	err := cmd.Run()
	if err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to GetBlockDevices, err: %w", err)
	}

	devices, err := unmarshalDevices(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to unmarshal devices, err: %w", err)
	}

	return devices, cmd.String(), nil
}

func GetAllVGs() ([]internal.VGData, string, error) {
	var outs bytes.Buffer
	cmd := exec.Command("vgs", "-o", "+uuid,tags,shared", "--reportformat", "json")
	cmd.Stdout = &outs

	if err := cmd.Run(); err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to GetAllVGs, err: %w", err)
	}

	vgs, err := unmarshalVGs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to GetAllVGs, err: %w", err)
	}

	return vgs, cmd.String(), nil
}

func GetAllPVs() ([]internal.PVData, string, error) {
	var outs bytes.Buffer
	cmd := exec.Command("pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--reportformat", "json")
	cmd.Stdout = &outs

	if err := cmd.Run(); err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	pvs, err := unmarshalPVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	return pvs, cmd.String(), nil
}

func GetSinglePV(pVname string) (*internal.PVData, string, error) {
	var outs bytes.Buffer
	cmd := exec.Command("pvs", pVname, "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--reportformat", "json")
	cmd.Stdout = &outs

	if err := cmd.Run(); err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to GetSinglePV, err: %w", err)
	}

	pvs, err := unmarshalPVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to GetSinglePV, err: %w", err)
	}

	singlePv := pvs[0]

	if len(pvs) != 1 ||
		singlePv.PVName != pVname {
		return nil, cmd.String(), fmt.Errorf(`unable to GetSinglePV by name: "%s"`, pVname)
	}

	return &singlePv, cmd.String(), nil
}

func CreatePV(path string) (string, error) {
	cmd := exec.Command("pvcreate", path)

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreatePV, err: %w", err)
	}

	return cmd.String(), nil
}

func CreateVGLocal(vgName string, lvm *v1alpha1.LvmVolumeGroup) (string, error) {
	if lvm == nil {
		return "", fmt.Errorf("unable to CreateVGLocal, err: lvm is nil")
	}

	cmd := exec.Command(
		"vgcreate",
		vgName,
		strings.Join(lvm.Spec.BlockDeviceNames, " "),
		"--addtag storage.deckhouse.io/enabled=true",
		fmt.Sprintf("--addtag storage.deckhouse.io/lvmVolumeGroupName=%s", lvm.Name))

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreateVGLocal, err: %w", err)
	}

	return cmd.String(), nil
}

func CreateVGShared(vgName string, lvm *v1alpha1.LvmVolumeGroup) (string, error) {
	if lvm == nil {
		return "", fmt.Errorf("unable to CreateVGShared, err: lvm is nil")
	}

	cmd := exec.Command(
		"vgcreate",
		"--shared",
		vgName,
		strings.Join(lvm.Spec.BlockDeviceNames, " "),
		"--addtag storage.deckhouse.io/enabled=true",
		fmt.Sprintf("--addtag storage.deckhouse.io/lvmVolumeGroupName=%s", lvm.Name))

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreateVGShared, err: %w", err)
	}

	return cmd.String(), nil
}

func CreateLV(thinPool v1alpha1.ThinPool, spec *v1alpha1.LvmVolumeGroupSpec) (string, error) {
	if spec == nil {
		return "", fmt.Errorf("unable to CreateLV, err: spec is nil")
	}

	cmd := exec.Command(
		"lvcreate",
		"-L",
		thinPool.Size,
		"-T",
		fmt.Sprintf("%s/%s", spec.ActualVGNameOnTheNode, thinPool.Name))

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreateLV, err: %w", err)
	}
	return cmd.String(), nil
}

func ExtendVG(vgName string, paths ...string) (string, error) {
	cmd := exec.Command(
		"vgextend",
		vgName,
		strings.Join(paths, " "))

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to ExtendVG, err: %w", err)
	}

	return cmd.String(), nil
}

func ExtendLV(size, vgName, lvName string) (string, error) {
	cmd := exec.Command(
		"lvextend",
		"-L",
		size,
		fmt.Sprintf("/dev/%s/%s", vgName, lvName))

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to ExtendLV, err: %w", err)
	}

	return cmd.String(), nil
}

func ResizePV(pvName string) (string, error) {
	cmd := exec.Command("pvresize", pvName)
	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to ResizePV, err: %w", err)
	}

	return cmd.String(), nil
}

func RemovePVFromVG(pvName, vgName string) (string, error) {
	pvs, cmdStr, err := GetAllPVs()
	if err != nil {
		return cmdStr, fmt.Errorf("unable to RemovePVFromVG, err: %w", err)
	}

	if len(pvs) == 1 {
		pv := pvs[0]

		if pv.PVName != pvName ||
			pv.VGName != vgName {
			return cmdStr,
				fmt.Errorf(
					`unable to RemovePVFromVG, err: unexpected pv gotten, no pv with pvName: "%s", vgName: "%s"`,
					pvName, vgName)
		}

		if pv.PVUsed != "0 " {
			return cmdStr, fmt.Errorf("unable to RemovePVFromVG, err: single PVData has data")
		}

		cmdStr, err = RemoveVG(pv.VGName)
		return cmdStr, err
	}

	if cmdStr, err = MovePV(pvName); err != nil {
		return cmdStr, err
	}

	clear, cmdStr, err := CheckPVHasNoData(pvName)
	if err != nil {
		return cmdStr, err
	}

	if !clear {
		return cmdStr, fmt.Errorf(`unable to RemovePVFromVG, err: can't move LV segments from PVData, pv name: "%s"`, pvName)
	}

	cmd := exec.Command("vgreduce", vgName, pvName)
	if err = cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf(`unable to RemovePVFromVG with vgName: "%s", pvName: "%s", err: %w`,
			vgName, pvName, err)
	}

	return cmd.String(), nil
}

func CheckPVHasNoData(pvName string) (bool, string, error) {
	pv, cmdStr, err := GetSinglePV(pvName)
	if err != nil {
		return true, cmdStr, err
	}

	return pv.PVUsed == "0 ", cmdStr, nil
}

func MovePV(pvName string) (string, error) {
	cmd := exec.Command("pvmove", pvName)

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to MovePV, err: %w", err)
	}

	return cmd.String(), nil
}

func RemoveVG(vgName string) (string, error) {
	cmd := exec.Command("vgremove", vgName)

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to RemoveVG, err: %w", err)
	}

	return cmd.String(), nil
}

func unmarshalDevices(out []byte) ([]internal.Device, error) {
	var devices internal.Devices
	if err := json.Unmarshal(out, &devices); err != nil {
		return nil, err
	}

	return devices.BlockDevices, nil
}

func unmarshalPVs(out []byte) ([]internal.PVData, error) {
	var pvR internal.PVReport

	if err := json.Unmarshal(out, &pvR); err != nil {
		return nil, err
	}

	var pvs []internal.PVData

	for _, rep := range pvR.Report {
		for _, pv := range rep.PV {
			pvs = append(pvs, pv)
		}
	}

	return pvs, nil
}

func unmarshalVGs(out []byte) ([]internal.VGData, error) {
	var vgR internal.VGReport

	if err := json.Unmarshal(out, &vgR); err != nil {
		return nil, err
	}

	var vgs []internal.VGData

	for _, rep := range vgR.Report {
		for _, vg := range rep.VG {
			vgs = append(vgs, vg)
		}
	}

	return vgs, nil
}
