/*
Copyright 2023 Flant JSC

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

package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"
	"strings"
)

const (
	nsenter = "/usr/bin/nsenter"
)

func GetBlockDevices() ([]internal.Device, string, error) {
	var outs bytes.Buffer
	args := []string{"lsblk", "-J", "-lpfb", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,FSTYPE,TYPE,WWN,KNAME,PKNAME,ROTA"}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	devices, err := UnmarshalDevices(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to unmarshal devices, err: %w", err)
	}

	return devices, cmd.String(), nil
}

func GetAllVGs() (data []internal.VGData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"vgs", "-o", "+uuid,tags,shared", "--units", "B", "--nosuffix", "--reportformat", "json"}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err = cmd.Run(); err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stdErr.String())
	}

	data, err = unmarshalVGs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllVGs, err: %w", err)
	}

	return data, cmd.String(), stdErr, nil
}

func GetAllLVs() (data []internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json"}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err = cmd.Run(); err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stdErr.String())
	}

	lvs, err := unmarshalLVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllLVs, err: %w", err)
	}

	return lvs, cmd.String(), stdErr, nil
}

func GetLV(vgName, lvName string) (lvData internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	lvData = internal.LVData{}
	lvPath := fmt.Sprintf("/dev/%s/%s", vgName, lvName)
	args := []string{"lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json", lvPath}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err = cmd.Run(); err != nil {
		return lvData, cmd.String(), stdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stdErr.String())
	}

	lv, err := unmarshalLVs(outs.Bytes())
	if err != nil {
		return lvData, cmd.String(), stdErr, fmt.Errorf("unable to GetLV %s, err: %w", lvPath, err)
	}
	lvData = lv[0]

	return lvData, cmd.String(), stdErr, nil
}

func GetAllPVs() (data []internal.PVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json"}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err = cmd.Run(); err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stdErr.String())
	}

	data, err = unmarshalPVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stdErr, fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	return data, cmd.String(), stdErr, nil
}

func CreatePV(path string) (string, error) {
	args := []string{"pvcreate", path}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderror = %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGLocal(vgName, lvmName string, pvNames []string) (string, error) {
	tmpStr := fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmName)
	args := []string{"vgcreate", vgName, strings.Join(pvNames, " "), "--addtag", "storage.deckhouse.io/enabled=true", "--addtag", tmpStr}

	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderror: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGShared(vgName, lvmName string, pvNames []string) (string, error) {
	args := []string{"vgcreate", "--shared", vgName, strings.Join(pvNames, " "), "--addtag", "storage.deckhouse.io/enabled=true", "--addtag", fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmName)}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateThinPool(thinPool v1alpha1.SpecThinPool, VGName string) (string, error) {
	args := []string{"lvcreate", "-L", thinPool.Size.String(), "-T", fmt.Sprintf("%s/%s", VGName, thinPool.Name)}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func CreateThinLogicalVolume(vgName, tpName, lvName string, size int64) (string, error) {
	args := []string{"lvcreate", "-T", fmt.Sprintf("%s/%s", vgName, tpName), "-n", lvName, "-V", fmt.Sprintf("%dk", size/1024), "-W", "y", "-y"}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	err := cmd.Run()
	if err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateThickLogicalVolume(vgName, lvName string, size int64) (string, error) {
	args := []string{"lvcreate", "-n", fmt.Sprintf("%s/%s", vgName, lvName), "-L", fmt.Sprintf("%dk", size/1024), "-W", "y", "-y"}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendVG(vgName string, paths []string) (string, error) {
	args := []string{"vgextend", vgName, strings.Join(paths, " ")}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendLV(size int64, vgName, lvName string) (string, error) {
	args := []string{"lvextend", "-L", fmt.Sprintf("%dk", size/1024), fmt.Sprintf("/dev/%s/%s", vgName, lvName)}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ResizePV(pvName string) (string, error) {
	args := []string{"pvresize", pvName}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func RemoveVG(vgName string) (string, error) {
	args := []string{"vgremove", vgName}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func RemovePV(pvNames []string) (string, error) {
	args := []string{"pvremove", strings.Join(pvNames, " ")}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr, %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func RemoveLV(vgName, lvName string) (string, error) {
	args := []string{"lvremove", fmt.Sprintf("/dev/%s/%s", vgName, lvName), "-y"}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func VGChangeAddTag(vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	args := []string{"vgchange", vGName, "--addtag", tag}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stdErr: %s", cmd.String(), err, stdErr.String())
	}
	return cmd.String(), nil
}

func VGChangeDelTag(vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	args := []string{"vgchange", vGName, "--deltag", tag}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stdErr: %s", cmd.String(), err, stdErr.String())
	}
	return cmd.String(), nil
}

func LVChangeDelTag(lv internal.LVData, tag string) (string, error) {
	tmpStr := fmt.Sprintf("/dev/%s/%s", lv.VGName, lv.LVName)
	var outs, stdErr bytes.Buffer
	args := []string{"lvchange", tmpStr, "--deltag", tag}
	extendedArgs := extendArgs(args)
	cmd := exec.Command(nsenter, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stdErr: %s", cmd.String(), err, stdErr.String())
	}
	return cmd.String(), nil
}

func UnmarshalDevices(out []byte) ([]internal.Device, error) {
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

func extendArgs(args []string) []string {
	nsenterArgs := []string{"-t", "1", "-m", "-u", "-i", "-n", "-p"}
	return append(nsenterArgs, args...)
}
