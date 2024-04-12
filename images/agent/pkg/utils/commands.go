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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"sds-node-configurator/api/v1alpha1"
	"sds-node-configurator/internal"

	golog "log"
)

func GetBlockDevices() ([]internal.Device, string, error) {
	var outs bytes.Buffer
	args := []string{"-J", "-lpfb", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,FSTYPE,TYPE,WWN,KNAME,PKNAME,ROTA"}
	cmd := exec.Command(internal.LSBLKCmd, args...)
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
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	filteredStdErr := filterStdErr(cmd.String(), stdErr)
	err = cmd.Run()
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	data, err = unmarshalVGs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetAllVGs, err: %w", err)
	}

	return data, cmd.String(), filteredStdErr, nil
}

func GetVG(vgName string) (vgData internal.VGData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	vgData = internal.VGData{}
	args := []string{"vgs", "-o", "+uuid,tags,shared", "--units", "B", "--nosuffix", "--reportformat", "json", vgName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr)
	if err != nil {
		return vgData, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	data, err := unmarshalVGs(outs.Bytes())
	if err != nil {
		return vgData, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetVG, err: %w", err)
	}
	vgData = data[0]

	return vgData, cmd.String(), filteredStdErr, nil
}

func GetAllLVs() (data []internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr)
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	lvs, err := unmarshalLVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetAllLVs, err: %w", err)
	}

	return lvs, cmd.String(), filteredStdErr, nil
}

func GetLV(vgName, lvName string) (lvData internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	lvData = internal.LVData{}
	lvPath := fmt.Sprintf("/dev/%s/%s", vgName, lvName)
	args := []string{"lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json", lvPath}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr)
	if err != nil {
		return lvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	lv, err := unmarshalLVs(outs.Bytes())
	if err != nil {
		return lvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetLV %s, err: %w", lvPath, err)
	}
	lvData = lv[0]

	return lvData, cmd.String(), filteredStdErr, nil
}

func GetAllPVs() (data []internal.PVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr)
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	data, err = unmarshalPVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	return data, cmd.String(), filteredStdErr, nil
}

func GetPV(pvName string) (pvData internal.PVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	pvData = internal.PVData{}
	args := []string{"pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json", pvName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr)
	if err != nil {
		return pvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	data, err := unmarshalPVs(outs.Bytes())
	if err != nil {
		return pvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetPV, err: %w", err)
	}
	pvData = data[0]

	return pvData, cmd.String(), filteredStdErr, nil
}

func CreatePV(path string) (string, error) {
	args := []string{"pvcreate", path}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderror = %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGLocal(vgName, lvmVolumeGroupName string, pvNames []string) (string, error) {
	tmpStr := fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmVolumeGroupName)
	args := []string{"vgcreate", vgName}
	args = append(args, pvNames...)
	args = append(args, "--addtag", "storage.deckhouse.io/enabled=true", "--addtag", tmpStr)

	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderror: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGShared(vgName, lvmVolumeGroupName string, pvNames []string) (string, error) {
	tmpStr := fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmVolumeGroupName)
	args := []string{"vgcreate", "--shared", vgName}
	args = append(args, pvNames...)
	args = append(args, "--addtag", "storage.deckhouse.io/enabled=true", "--addtag", "--addtag", tmpStr)

	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateThinPool(thinPool v1alpha1.SpecThinPool, VGName string) (string, error) {
	args := []string{"lvcreate", "-L", thinPool.Size.String(), "-T", fmt.Sprintf("%s/%s", VGName, thinPool.Name)}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func CreateThinLogicalVolume(vgName, tpName, lvName string, size int64) (string, error) {
	args := []string{"lvcreate", "-T", fmt.Sprintf("%s/%s", vgName, tpName), "-n", lvName, "-V", fmt.Sprintf("%dk", size/1024), "-W", "y", "-y"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

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
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendVG(vgName string, paths []string) (string, error) {
	args := []string{"vgextend", vgName}
	args = append(args, paths...)
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendLV(size int64, vgName, lvName string) (string, error) {
	args := []string{"lvextend", "-L", fmt.Sprintf("%dk", size/1024), fmt.Sprintf("/dev/%s/%s", vgName, lvName)}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ResizePV(pvName string) (string, error) {
	args := []string{"pvresize", pvName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func RemoveVG(vgName string) (string, error) {
	args := []string{"vgremove", vgName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func RemovePV(pvNames []string) (string, error) {
	args := []string{"pvremove"}
	args = append(args, pvNames...)
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr, %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func RemoveLV(vgName, lvName string) (string, error) {
	args := []string{"lvremove", fmt.Sprintf("/dev/%s/%s", vgName, lvName), "-y"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

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
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
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
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
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
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
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

func lvmStaticExtendedArgs(args []string) []string {
	nsenterArgs := []string{"-t", "1", "-m", "-u", "-i", "-n", "-p"}
	lvmStaticBin := []string{"--", internal.LVMCmd}
	result := append(nsenterArgs, lvmStaticBin...)
	return append(result, args...)
}

// filterStdErr processes a bytes.Buffer containing stderr output and filters out specific
// messages that match a predefined regular expression pattern. In this context, it's used to
// exclude "Regex version mismatch" messages generated due to SELinux checks on a statically
// compiled LVM binary. These messages report a discrepancy between the versions of the regex
// library used inside the LVM binary, but since LVM is statically compiled and doesn't rely on
// system libraries at runtime, these warnings do not impact LVM's functionality or overall
// system security. Therefore, they can be safely ignored to simplify the analysis of command output.
//
// Parameters:
//   - stdErr (bytes.Buffer): The buffer containing the stderr output from a command execution.
//
// Returns:
//   - bytes.Buffer: A new buffer containing the filtered stderr output, excluding lines that
//     match the "Regex version mismatch" pattern.
func filterStdErr(command string, stdErr bytes.Buffer) bytes.Buffer {
	var filteredStdErr bytes.Buffer
	stdErrScanner := bufio.NewScanner(&stdErr)
	regexpPattern := `Regex version mismatch, expected: .+ actual: .+`
	regex, err := regexp.Compile(regexpPattern)
	if err != nil {
		return stdErr
	}

	for stdErrScanner.Scan() {
		line := stdErrScanner.Text()
		if !regex.MatchString(line) {
			filteredStdErr.WriteString(line + "\n")
		} else {
			golog.Printf("WARNING: [filterStdErr] Line filtered from stderr due to matching exclusion pattern. Line: '%s'. Triggered by command: '%s'.", line, command)
		}
	}

	return filteredStdErr
}
