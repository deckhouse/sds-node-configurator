package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
)

func GetBlockDevices() ([]internal.Device, string, error) {
	var outs bytes.Buffer
	cmd := exec.Command("lsblk", "-J", "-lpfb", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,FSTYPE,TYPE,WWN,KNAME,PKNAME,ROTA")
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

func GetAllVGs() (data []internal.VGData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	cmd := exec.Command("vgs", "-o", "+uuid,tags,shared", "--units", "B", "--nosuffix", "--reportformat", "json")
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllVGs, err: %w", err)
	}

	data, err = unmarshalVGs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllVGs, err: %w", err)
	}

	return data, cmd.String(), stdErr, nil
}

func GetAllLVs() (data []internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	cmd := exec.Command("lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json")
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllLVs, err: %w", err)
	}

	lvs, err := unmarshalLVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllLVs, err: %w", err)
	}

	return lvs, cmd.String(), stdErr, nil
}

func GetAllPVs() (data []internal.PVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	cmd := exec.Command("pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json")
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	data, err = unmarshalPVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	return data, cmd.String(), stdErr, nil
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

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreatePV, err: %w , stderror = %s", err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGLocal(vgName, lvmName string, pvNames []string) (string, error) {

	tmpStr := fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmName)
	var arg []string
	arg = append(arg, vgName)
	arg = append(arg, pvNames...)
	arg = append(arg, "--addtag", "storage.deckhouse.io/enabled=true", "--addtag", tmpStr)

	cmd := exec.Command("vgcreate", arg...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreateVGLocal, err: %w , stderror = %s", err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGShared(vgName, lvmName string, pvNames []string) (string, error) {
	var arg []string
	arg = append(arg, "--shared")
	arg = append(arg, vgName)
	arg = append(arg, pvNames...)
	arg = append(arg, "--addtag",
		"storage.deckhouse.io/enabled=true",
		"--addtag", fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmName))

	cmd := exec.Command("vgcreate", arg...)

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreateVGShared, err: %w", err)
	}

	return cmd.String(), nil
}

func CreateLV(thinPool v1alpha1.SpecThinPool, VGName string) (string, error) {

	cmd := exec.Command(
		"lvcreate", "-L", thinPool.Size.String(), "-T", fmt.Sprintf("%s/%s", VGName, thinPool.Name))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to CreateLV, err: %w tderr = %s", err, stderr.String())
	}
	return cmd.String(), nil
}

func ExtendVG(vgName string, paths []string) (string, error) {
	var arg []string
	arg = append(arg, vgName)
	arg = append(arg, paths...)
	cmd := exec.Command("vgextend", arg...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to ExtendVG, err: %w stderr = %s", err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendLV(size, vgName, lvName string) (string, error) {
	cmd := exec.Command("lvextend", "-L", size, fmt.Sprintf("/dev/%s/%s", vgName, lvName))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to ExtendLV, err: %w stderr = %s", err, stderr.String())
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
	pvs, cmdStr, _, err := GetAllPVs()
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

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to RemoveVG, err: %w stderr = %s", err, stderr.String())
	}

	return cmd.String(), nil
}

func RemovePV(pvNames []string) (string, error) {
	cmd := exec.Command("pvremove", pvNames...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to RemovePV, err: %w stderr = %s", err, stderr.String())
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

func unmarshalLVs(out []byte) ([]internal.LVData, error) {
	var lvR internal.LVReport

	if err := json.Unmarshal(out, &lvR); err != nil {
		return nil, err
	}

	var lvs []internal.LVData

	for _, rep := range lvR.Report {
		for _, lv := range rep.LV {
			lvs = append(lvs, lv)
		}
	}

	return lvs, nil
}

func VGChangeAddTag(vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	cmd := exec.Command("vgchange", vGName, "--addtag", tag)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to VGChangeAddTag, err: %w , stdErr: %s", err, stdErr.String())
	}
	return cmd.String(), nil
}

func VGChangeDelTag(vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	cmd := exec.Command("vgchange", vGName, "--deltag", tag)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to VGChangeDelTag, err: %w , stdErr: %s", err, stdErr.String())
	}
	return cmd.String(), nil
}

func LVChangeDelTag(lv internal.LVData, tag string) (string, error) {
	tmpStr := fmt.Sprintf("/dev/%s/%s", lv.VGName, lv.LVName)
	var outs, stdErr bytes.Buffer
	cmd := exec.Command("lvchange", tmpStr, "--deltag", tag)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to LVChangeDelTag, err: %w , stdErr: %s", err, stdErr.String())
	}
	return cmd.String(), nil
}
