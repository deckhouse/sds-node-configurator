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
	"context"
	"encoding/json"
	"fmt"
	golog "log"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"agent/internal"
	"agent/internal/logger"
	"agent/internal/lvm"
	"agent/internal/monitoring"
)

func GetBlockDevices(ctx context.Context) ([]internal.Device, string, bytes.Buffer, error) {
	var outs bytes.Buffer
	args := []string{"-J", "-lpfb", "-no", "name,MOUNTPOINT,PARTUUID,HOTPLUG,MODEL,SERIAL,SIZE,FSTYPE,TYPE,WWN,KNAME,PKNAME,ROTA"}
	cmd := exec.CommandContext(ctx, internal.LSBLKCmd, args...)
	cmd.Stdout = &outs

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return nil, cmd.String(), stderr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	devices, err := UnmarshalDevices(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stderr, fmt.Errorf("unable to unmarshal devices, err: %w", err)
	}

	return devices, cmd.String(), stderr, nil
}

func GetAllVGs(ctx context.Context) (data []internal.VGData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	cmd := lvm.CommandContext(ctx, "vgs", "-o", "+uuid,tags,shared", "--units", "B", "--nosuffix", "--reportformat", "json")
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
	cmd := lvm.Command("vgs", "-o", "+uuid,tags,shared", "--units", "B", "--nosuffix", "--reportformat", "json", vgName)
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

func GetAllLVs(ctx context.Context) (data []internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	cmd := lvm.CommandContext(ctx, "lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json")
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
	cmd := lvm.Command("lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json", lvPath)
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

func GetAllPVs(ctx context.Context) (data []internal.PVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	cmd := lvm.CommandContext(ctx, "pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json")
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
	cmd := lvm.Command("pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json", pvName)
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
	cmd := lvm.Command("pvcreate", path)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderror = %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGLocal(vgName, lvmVolumeGroupName string, pvNames []string) (string, error) {
	tmpStr := fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmVolumeGroupName)
	args := []string{vgName}
	args = append(args, pvNames...)
	args = append(args, "--addtag", "storage.deckhouse.io/enabled=true", "--addtag", tmpStr)

	cmd := lvm.Command("vgcreate", args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderror: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateVGShared(vgName, lvmVolumeGroupName string, pvNames []string) (string, error) {
	tmpStr := fmt.Sprintf("storage.deckhouse.io/lvmVolumeGroupName=%s", lvmVolumeGroupName)
	args := []string{"--shared", vgName}
	args = append(args, pvNames...)
	args = append(args, "--addtag", "storage.deckhouse.io/enabled=true", "--addtag", "--addtag", tmpStr)

	cmd := lvm.Command("vgcreate", args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func CreateThinPool(thinPoolName, vgName string, size int64) (string, error) {
	cmd := lvm.Command("lvcreate", "-L", fmt.Sprintf("%dk", size/1024), "-T", fmt.Sprintf("%s/%s", vgName, thinPoolName))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func CreateThinPoolFullVGSpace(thinPoolName, vgName string) (string, error) {
	cmd := lvm.Command("lvcreate", "-l", "100%FREE", "-T", fmt.Sprintf("%s/%s", vgName, thinPoolName))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func CreateThinLogicalVolumeFromSource(name string, sourceVgName string, sourceName string) (string, error) {
	return createSnapshotVolume(name, sourceVgName, sourceName, nil)
}

func CreateThinLogicalVolumeSnapshot(name string, sourceVgName string, sourceName string, tags []string) (string, error) {
	return createSnapshotVolume(name, sourceVgName, sourceName, tags)
}

func createSnapshotVolume(name string, sourceVgName string, sourceName string, tags []string) (string, error) {
	args := []string{"-s", "-kn", "-n", name, fmt.Sprintf("%s/%s", sourceVgName, sourceName), "-y"}

	for _, tag := range tags {
		args = append(args, "--addtag")
		args = append(args, tag)
	}

	cmd := lvm.Command("lvcreate", args...)

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

func CreateThinLogicalVolume(vgName, tpName, lvName string, size int64) (string, error) {
	cmd := lvm.Command("lvcreate", "-T", fmt.Sprintf("%s/%s", vgName, tpName), "-n", lvName, "-V", fmt.Sprintf("%dk", size/1024), "-W", "y", "-y")

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

func CreateThickLogicalVolume(vgName, lvName string, size int64, contiguous bool) (string, error) {
	args := []string{"-n", fmt.Sprintf("%s/%s", vgName, lvName), "-L", fmt.Sprintf("%dk", size/1024), "-W", "y", "-y"}
	if contiguous {
		args = append(args, "--contiguous", "y")
	}

	cmd := lvm.Command("lvcreate", args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendVG(vgName string, paths []string) (string, error) {
	args := []string{vgName}
	args = append(args, paths...)
	cmd := lvm.Command("vgextend", args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendLV(size int64, vgName, lvName string) (string, error) {
	cmd := lvm.Command("lvextend", "-L", fmt.Sprintf("%dk", size/1024), fmt.Sprintf("/dev/%s/%s", vgName, lvName))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stderr)
	if err != nil && filteredStdErr.Len() > 0 {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func ExtendLVFullVGSpace(vgName, lvName string) (string, error) {
	cmd := lvm.Command("lvextend", "-l", "100%VG", fmt.Sprintf("/dev/%s/%s", vgName, lvName))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stderr)
	if err != nil && filteredStdErr.Len() > 0 {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	return cmd.String(), nil
}

func ResizePV(pvName string) (string, error) {
	cmd := lvm.Command("pvresize", pvName)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func RemoveVG(vgName string) (string, error) {
	cmd := lvm.Command("vgremove", vgName)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

func RemovePV(pvNames []string) (string, error) {
	cmd := lvm.Command("pvremove", pvNames...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr, %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func RemoveLV(vgName, lvName string) (string, error) {
	cmd := lvm.Command("lvremove", fmt.Sprintf("/dev/%s/%s", vgName, lvName), "-y")

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func VGChangeAddTag(vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	cmd := lvm.Command("vgchange", vGName, "--addtag", tag)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stdErr: %s", cmd.String(), err, stdErr.String())
	}
	return cmd.String(), nil
}

func VGChangeDelTag(vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	cmd := lvm.Command("vgchange", vGName, "--deltag", tag)
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
	cmd := lvm.Command("lvchange", tmpStr, "--deltag", tag)
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

func ReTag(ctx context.Context, log logger.Logger, metrics monitoring.Metrics, ctrlName string) error {
	// thin pool
	log.Debug("[ReTag] start re-tagging LV")
	start := time.Now()
	lvs, cmdStr, _, err := GetAllLVs(ctx)
	metrics.UtilsCommandsDuration(ctrlName, "lvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(ctrlName, "lvs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(ctrlName, "lvs").Inc()
		log.Error(err, "[ReTag] unable to GetAllLVs")
		return err
	}

	for _, lv := range lvs {
		tags := strings.Split(lv.LvTags, ",")
		for _, tag := range tags {
			if strings.Contains(tag, internal.LVMTags[0]) {
				continue
			}

			if strings.Contains(tag, internal.LVMTags[1]) {
				start = time.Now()
				cmdStr, err = LVChangeDelTag(lv, tag)
				metrics.UtilsCommandsDuration(ctrlName, "lvchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(ctrlName, "lvchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(ctrlName, "lvchange").Inc()
					log.Error(err, "[ReTag] unable to LVChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = VGChangeAddTag(lv.VGName, internal.LVMTags[0])
				metrics.UtilsCommandsDuration(ctrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(ctrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(ctrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeAddTag")
					return err
				}
			}
		}
	}
	log.Debug("[ReTag] end re-tagging LV")

	log.Debug("[ReTag] start re-tagging LVM")
	start = time.Now()
	vgs, cmdStr, _, err := GetAllVGs(ctx)
	metrics.UtilsCommandsDuration(ctrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(ctrlName, "vgs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(ctrlName, cmdStr).Inc()
		log.Error(err, "[ReTag] unable to GetAllVGs")
		return err
	}

	for _, vg := range vgs {
		tags := strings.Split(vg.VGTags, ",")
		for _, tag := range tags {
			if strings.Contains(tag, internal.LVMTags[0]) {
				continue
			}

			if strings.Contains(tag, internal.LVMTags[1]) {
				start = time.Now()
				cmdStr, err = VGChangeDelTag(vg.VGName, tag)
				metrics.UtilsCommandsDuration(ctrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(ctrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(ctrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = VGChangeAddTag(vg.VGName, internal.LVMTags[0])
				metrics.UtilsCommandsDuration(ctrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(ctrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(ctrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeAddTag")
					return err
				}
			}
		}
	}
	log.Debug("[ReTag] stop re-tagging LVM")

	return nil
}

func unmarshalPVs(out []byte) ([]internal.PVData, error) {
	var pvR internal.PVReport

	if err := json.Unmarshal(out, &pvR); err != nil {
		return nil, err
	}

	pvs := make([]internal.PVData, 0, len(pvR.Report))
	for _, rep := range pvR.Report {
		pvs = append(pvs, rep.PV...)
	}

	return pvs, nil
}

func unmarshalVGs(out []byte) ([]internal.VGData, error) {
	var vgR internal.VGReport

	if err := json.Unmarshal(out, &vgR); err != nil {
		return nil, err
	}

	vgs := make([]internal.VGData, 0, len(vgR.Report))
	for _, rep := range vgR.Report {
		vgs = append(vgs, rep.VG...)
	}

	return vgs, nil
}

func unmarshalLVs(out []byte) ([]internal.LVData, error) {
	var lvR internal.LVReport

	if err := json.Unmarshal(out, &lvR); err != nil {
		return nil, err
	}

	lvs := make([]internal.LVData, 0, len(lvR.Report))
	for _, rep := range lvR.Report {
		lvs = append(lvs, rep.LV...)
	}

	return lvs, nil
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
//   - command (string): The command that generated the stderr output.
//   - stdErr (bytes.Buffer): The buffer containing the stderr output from a command execution.
//
// Returns:
//   - bytes.Buffer: A new buffer containing the filtered stderr output, excluding lines that
//     match the "Regex version mismatch" pattern.
func filterStdErr(command string, stdErr bytes.Buffer) bytes.Buffer {
	var filteredStdErr bytes.Buffer
	stdErrScanner := bufio.NewScanner(&stdErr)
	regexpPattern := `Regex version mismatch, expected: .+ actual: .+`
	regexpSocketError := `File descriptor .+ leaked on lvm.static invocation. Parent PID .+: /opt/deckhouse/sds/bin/nsenter`
	// this needs as if the controller were restarted and found existing LVG thin-pools with size equals 100%VG space,
	// as the Thin-pool size on the node might be less than Spec one even with delta (because of metadata). So the controller
	// will try to resize the Thin-pool with 100%VG space and will get the error.
	regexpNoSizeChangeError := ` No size change.+`
	regex1, err := regexp.Compile(regexpPattern)
	if err != nil {
		return stdErr
	}
	regex2, err := regexp.Compile(regexpSocketError)
	if err != nil {
		return stdErr
	}
	regex3, err := regexp.Compile(regexpNoSizeChangeError)
	if err != nil {
		return stdErr
	}

	for stdErrScanner.Scan() {
		line := stdErrScanner.Text()
		if regex1.MatchString(line) ||
			regex2.MatchString(line) ||
			regex3.MatchString(line) {
			golog.Printf("WARNING: [filterStdErr] Line filtered from stderr due to matching exclusion pattern. Line: '%s'. Triggered by command: '%s'.", line, command)
		} else {
			filteredStdErr.WriteString(line + "\n")
		}
	}

	return filteredStdErr
}
