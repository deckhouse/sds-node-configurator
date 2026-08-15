/*
	Copyright 2026 Flant JSC

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

//go:generate go tool mockgen -copyright_file ../../../../hack/boilerplate.mockgen.txt -write_source_comment -destination=../mock_utils/$GOFILE -source=$GOFILE

package utils

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	golog "log"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
)

type Commands interface {
	GetBlockDevices(ctx context.Context) ([]internal.Device, string, bytes.Buffer, error)
	GetAllVGs(ctx context.Context) (data []internal.VGData, command string, stdErr bytes.Buffer, err error)
	GetVG(vgName string) (vgData internal.VGData, command string, stdErr bytes.Buffer, err error)
	GetAllLVs(ctx context.Context) (data []internal.LVData, command string, stdErr bytes.Buffer, err error)
	GetLV(vgName, lvName string) (lvData internal.LVData, command string, stdErr bytes.Buffer, err error)
	GetAllPVs(ctx context.Context) (data []internal.PVData, command string, stdErr bytes.Buffer, err error)
	GetPV(pvName string) (pvData internal.PVData, command string, stdErr bytes.Buffer, err error)
	CreatePV(ctx context.Context, path string) (string, error)
	CreateVGLocal(vgName, lvmVolumeGroupName string, pvNames []string) (string, error)
	CreateThinPool(thinPoolName, vgName string, size int64) (string, error)
	CreateThinPoolFullVGSpace(thinPoolName, vgName string) (string, error)
	CreateThinLogicalVolumeFromSource(name string, sourceVgName string, sourceName string) (string, error)
	CreateThinLogicalVolumeSnapshot(name string, sourceVgName string, sourceName string, tags []string) (string, error)
	CreateThinLogicalVolume(vgName, tpName, lvName string, size int64) (string, error)
	CreateThickLogicalVolume(vgName, lvName string, size int64, contiguous bool) (string, error)
	ExtendVG(vgName string, paths []string) (string, error)
	ExtendLV(size int64, vgName, lvName string) (string, error)
	ExtendLVFullVGSpace(vgName, lvName string) (string, error)
	ResizePV(ctx context.Context, pvName string) (string, error)
	RemoveVG(vgName string) (string, error)
	RemoveVGShared(ctx context.Context, vgName string) (string, error)
	ExtendVGShared(ctx context.Context, vgName string, paths []string) (string, error)
	RemovePV(pvNames []string) (string, error)
	RemoveLV(vgName, lvName string) (string, error)
	VGChangeAddTag(ctx context.Context, vGName, tag string) (string, error)
	VGChangeDelTag(ctx context.Context, vGName, tag string) (string, error)
	LVChangeDelTag(ctx context.Context, lv internal.LVData, tag string) (string, error)
	VGActivate(ctx context.Context, vgName string) (string, error)
	VGLockStart(ctx context.Context, vgName string, hostID int) (string, error)
	VGLockStop(ctx context.Context, vgName string) (string, error)
	LockspaceRunning(ctx context.Context, vgName string) (bool, error)
	VGSetPersist(ctx context.Context, vgName string, hostID int) (string, error)
	VGPersistStart(ctx context.Context, vgName string, hostID int) (string, error)
	VGPersistStop(ctx context.Context, vgName string, hostID int) (string, error)
	VGPersistSetting(ctx context.Context, vgName string) (string, error)
	VGSetLockArgsPersist(ctx context.Context, vgName string, hostID int) (string, error)
	MultipathConfiguration(ctx context.Context) (string, error)
	RecordedReservationKey(ctx context.Context) (string, error)
	SetReservationKey(ctx context.Context, mapName, key string) (string, error)
	ReadRegistrationKeys(ctx context.Context, path string) ([]string, string, error)
	PreemptRegistration(ctx context.Context, path, ourKey, theirKey string) (string, error)
	MissingReservationTools(ctx context.Context) ([]string, error)
	ReservationKeyOf(ctx context.Context, mapName string) (string, error)
	CreateVGShared(ctx context.Context, params SharedVGParams) (string, error)
	CreateLVShared(ctx context.Context, vgName, lvName, size string) (string, error)
	RemoveLVShared(ctx context.Context, vgName, lvName string) (string, error)
	SetLVTagShared(ctx context.Context, vgName, lvName, tag string, add bool) (string, error)
	LVExtendShared(ctx context.Context, vgName, lvName, size string) (string, error)
	LVActivateShared(ctx context.Context, vgName string, lvNames []string, shared bool) (string, error)
	RemoveDMDevice(ctx context.Context, dmName string) (string, error)
	RemoveDMDeviceDeferred(ctx context.Context, dmName string) (string, error)
	WipeDMTable(ctx context.Context, dmName string) (string, error)
	LVDeactivateShared(ctx context.Context, vgName string, lvNames []string) (string, error)
	LVActivate(ctx context.Context, vgName, lvName string) (string, error)
	VGScan(ctx context.Context) (string, error)
	PVScan(ctx context.Context) (string, error)
	UdevadmTrigger(ctx context.Context, paths []string) (string, error)
	UnmarshalDevices(out []byte) ([]internal.Device, error)
	ReTag(ctx context.Context, log logger.Logger, metrics *monitoring.Metrics, ctrlName string, cmdTimeout time.Duration) error

	CreateFileDevice(ctx context.Context, path string, sizeBytes int64) (string, error)
	GetFileAllocatedBytes(ctx context.Context, path string) (string, int64, error)
	SetupLoopDevice(ctx context.Context, filePath string) (string, string, error)
	SetLoopDirectIO(ctx context.Context, loopDev string) (string, error)
	SetLoopCapacity(ctx context.Context, loopDev string) (string, error)
	DetachLoopDevice(ctx context.Context, loopDev string) (string, error)
	ListLoopDevices(ctx context.Context) (string, []internal.LoopDeviceEntry, error)
	FindLoopDeviceByFile(ctx context.Context, filePath string) (string, string, error)
	GetLoopBackingFile(ctx context.Context, loopDev string) (string, internal.LoopBackingFile, error)
	RemoveFileDevice(ctx context.Context, path string) (string, error)
	EnsureFileDeviceDirectory(ctx context.Context, directory string) (string, error)
	GetFilesystemSpace(ctx context.Context, directory string) (string, internal.FilesystemSpace, error)
}

type commands struct {
}

func NewCommands() Commands {
	return &commands{}
}

func (c *commands) GetBlockDevices(ctx context.Context) ([]internal.Device, string, bytes.Buffer, error) {
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

	devices, err := c.UnmarshalDevices(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), stderr, fmt.Errorf("unable to unmarshal devices, err: %w", err)
	}

	return devices, cmd.String(), stderr, nil
}

// GetAllVGs lists the node's Volume Groups.
//
// Like the write commands, it goes through errIfNotBenign: lvm.static under
// nsenter prints a leaked file descriptor or a regex version mismatch and exits
// non-zero on an invocation that in fact produced a complete report, and taking
// that at face value is consequential for the callers that decide something on
// the answer — vgExistsOnNode reports VGCheckFailed, which takes an
// LVMVolumeGroup out of service, and ActivateAllManagedVGs refuses to activate
// anything. Silence is still a failure: `vgs` has no known no-op, so a non-zero
// exit with nothing on stderr means nobody knows what happened. An empty or
// truncated report is caught a line later by unmarshalVGs either way.
func (commands) GetAllVGs(ctx context.Context) (data []internal.VGData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"vgs", "-o", "+uuid,tags,shared,vg_lock_type,vg_attr,vg_extent_size", "--units", "B", "--nosuffix", "--reportformat", "json"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	// Filtered after the run, not before it: taken before, stdErr is still empty
	// and the returned buffer never carries anything.
	runErr := cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr, benignAlwaysStdErr)
	if err := errIfNotBenignFiltered(cmd.String(), runErr, stdErr, filteredStdErr, silentExitIsFailure); err != nil {
		return nil, cmd.String(), filteredStdErr, err
	}

	data, err = unmarshalVGs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetAllVGs, err: %w", err)
	}

	return data, cmd.String(), filteredStdErr, nil
}

func (commands) GetVG(vgName string) (vgData internal.VGData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	vgData = internal.VGData{}
	args := []string{"vgs", "-o", "+uuid,tags,shared,vg_lock_type,vg_attr,vg_extent_size", "--units", "B", "--nosuffix", "--reportformat", "json", vgName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr, benignAlwaysStdErr)
	if err != nil {
		return vgData, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	data, err := unmarshalVGs(outs.Bytes())
	if err != nil {
		return vgData, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetVG, err: %w", err)
	}
	vgData, err = theOnlyVG(data, vgName)
	if err != nil {
		return internal.VGData{}, cmd.String(), filteredStdErr, err
	}

	return vgData, cmd.String(), filteredStdErr, nil
}

func (commands) GetAllLVs(ctx context.Context) (data []internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"lvs", "-o", "+vg_uuid,tags,thin_id,metadata_lv,lv_dm_path", "--units", "B", "--nosuffix", "--all", "--reportformat", "json"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr, benignAlwaysStdErr)
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	lvs, err := unmarshalLVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetAllLVs, err: %w", err)
	}

	return lvs, cmd.String(), filteredStdErr, nil
}

func (commands) GetLV(vgName, lvName string) (lvData internal.LVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	lvData = internal.LVData{}
	lvPath := filepath.Join("/dev", vgName, lvName)
	args := []string{"lvs", "-o", "+vg_uuid,tags", "--units", "B", "--nosuffix", "--reportformat", "json", lvPath}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr, benignAlwaysStdErr)
	if err != nil {
		return lvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	lv, err := unmarshalLVs(outs.Bytes())
	if err != nil {
		return lvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetLV %s, err: %w", lvPath, err)
	}
	lvData, err = theOnlyLV(lv, lvPath, vgName)
	if err != nil {
		return internal.LVData{}, cmd.String(), filteredStdErr, err
	}

	return lvData, cmd.String(), filteredStdErr, nil
}

// GetAllPVs lists the node's Physical Volumes.
//
// Filtered through errIfNotBenign for the same reason GetAllVGs is, and with
// more at stake: this listing is what gates every destructive file-device
// decision. A false failure makes cleanupFileDevices refuse to run and leaves
// the LVMVolumeGroup in Terminating with its finalizer, and makes
// rollbackProvisionedFileDevices and pvView fall back or do nothing. See
// silentExitPolicy for why silence still counts as a failure here.
func (commands) GetAllPVs(ctx context.Context) (data []internal.PVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	args := []string{"pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	runErr := cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr, benignAlwaysStdErr)
	if err := errIfNotBenignFiltered(cmd.String(), runErr, stdErr, filteredStdErr, silentExitIsFailure); err != nil {
		return nil, cmd.String(), filteredStdErr, err
	}

	data, err = unmarshalPVs(outs.Bytes())
	if err != nil {
		return nil, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetAllPVs, err: %w", err)
	}

	return data, cmd.String(), filteredStdErr, nil
}

func (commands) GetPV(pvName string) (pvData internal.PVData, command string, stdErr bytes.Buffer, err error) {
	var outs bytes.Buffer
	pvData = internal.PVData{}
	args := []string{"pvs", "-o", "+pv_used,pv_uuid,vg_tags,vg_uuid", "--units", "B", "--nosuffix", "--reportformat", "json", pvName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	err = cmd.Run()
	filteredStdErr := filterStdErr(cmd.String(), stdErr, benignAlwaysStdErr)
	if err != nil {
		return pvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, filteredStdErr.String())
	}

	data, err := unmarshalPVs(outs.Bytes())
	if err != nil {
		return pvData, cmd.String(), filteredStdErr, fmt.Errorf("unable to GetPV, err: %w", err)
	}
	pvData, err = theOnlyPV(data, pvName)
	if err != nil {
		return internal.PVData{}, cmd.String(), filteredStdErr, err
	}

	return pvData, cmd.String(), filteredStdErr, nil
}

// silentExitPolicy says what a non-zero exit with an empty stderr means for a
// particular command. It is per-command because the honest answer differs:
//
//   - For a resize (`lvextend`), silence is tolerated. Some LVM versions report
//     a no-op `-l 100%VG` — which a thin pool sized 100% hits on every single
//     reconcile, since the pool always already fills the VG — without printing
//     anything, and calling that a failure makes a healthy pool flap
//     VGConfigurationApplied. This is the behaviour lvextend had before the
//     file-device work and it is deliberately preserved.
//
//   - For pvcreate and pvresize there is no such known no-op, so silence is an
//     unexplained failure and stays one. Accepting it would have CreatePV report
//     a PV label that was never written; the real error would then surface a
//     command later as a confusing vgcreate/vgextend failure, and — the reason
//     this matters here — a create rollback would run against a device whose
//     state nobody actually knows.
type silentExitPolicy bool

const (
	silentExitIsFailure silentExitPolicy = false
	silentExitIsBenign  silentExitPolicy = true
)

// deletedBackingFileMarker is what losetup (like
// /sys/block/<loop>/loop/backing_file) appends to a backing-file path whose
// inode has been unlinked while the loop device is still attached to it.
const deletedBackingFileMarker = "(deleted)"

// errIfNotBenign decides whether a finished lvm.static invocation actually
// failed. lvm.static run under nsenter routinely emits a benign line —
// "File descriptor N leaked on lvm invocation", a regex version mismatch, a
// no-op resize — and exits non-zero on an operation that in fact succeeded, so
// a bare non-zero exit cannot be taken at face value.
//
// stderr may only be given a vote when lvm itself chose the exit code, which is
// exactly one of the three ways a command can fail:
//
//   - it never started (binary missing, fork failure). There is no diagnostic to
//     filter and nothing ran, so this is always an error.
//   - it was killed by a signal — SIGKILL from the OOM killer, SIGTERM during
//     shutdown. lvm did not decide to fail and may have been cut off mid-write,
//     so whatever it had printed by then says nothing about the outcome. Always
//     an error, even though os/exec reports this as an *exec.ExitError too.
//   - it ran to completion and exited non-zero. Only here is stderr the
//     authority: if everything lvm printed was benign, the operation succeeded.
//
// The earlier `err != nil && filtered.Len() > 0` form collapsed all three, which
// silently turned every diagnostic-less failure into a success — a killed
// pvcreate would have had CreatePV claim a PV label that was never written.
//
// A non-zero exit with nothing at all on stderr is a fourth case, and it is the
// caller's to decide via silence: "everything lvm printed was benign" is a
// statement about output that exists, and an unexplained failure is not the same
// claim. See silentExitPolicy.
//
// allow says which lines count as benign for THIS command; see
// benignAlwaysStdErr for why that is not one global set.
func errIfNotBenign(cmdStr string, err error, stderr bytes.Buffer, allow []*regexp.Regexp, silence silentExitPolicy) error {
	if err == nil {
		return nil
	}
	return errIfNotBenignFiltered(cmdStr, err, stderr, filterStdErr(cmdStr, stderr, allow), silence)
}

// errIfNotBenignFiltered is errIfNotBenign for callers that already hold the
// filtered stderr because they return it as well. Filtering twice would work —
// a bytes.Buffer copy keeps its own read offset — but it would log every
// filtered line twice, which reads as two separate benign lines rather than one.
func errIfNotBenignFiltered(cmdStr string, err error, stderr, filtered bytes.Buffer, silence silentExitPolicy) error {
	if err == nil {
		return nil
	}

	failed := func() error {
		return fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmdStr, err, stderr.String())
	}

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return failed()
	}
	// ExitCode reports -1 when the process was terminated by a signal.
	if exitErr.ExitCode() < 0 {
		return failed()
	}
	if stderr.Len() == 0 && silence == silentExitIsFailure {
		return failed()
	}
	if filtered.Len() > 0 {
		return failed()
	}

	golog.Printf("WARNING: [errIfNotBenign] command '%s' exited with '%v' but printed only benign output; treating it as successful.", cmdStr, err)
	return nil
}

// ErrFileDeviceAbsent reports that a backing file is not there — the command ran,
// looked, and said so. It is deliberately NOT returned when the command never got
// to look, because the two answers lead to opposite decisions: "it is not there"
// permits the create-path rollback to remove the file it is about to create,
// while "I could not check" must not.
var ErrFileDeviceAbsent = errors.New("backing file does not exist")

// ranAndFailed reports whether the process actually ran to completion and chose a
// non-zero exit code, as opposed to never starting (binary missing, fork
// failure), being killed by a signal, or having its context cancelled or time out
// underneath it.
//
// It is the same distinction errIfNotBenign draws before it lets stderr vote,
// stated on its own because GetFileAllocatedBytes needs it for the opposite
// purpose: there, only a command that ran is allowed to conclude anything about
// the filesystem.
func ranAndFailed(err error) bool {
	var exitErr *exec.ExitError
	// ExitCode reports -1 when the process was terminated by a signal, which is
	// how exec.CommandContext reports a deadline or a cancellation.
	return errors.As(err, &exitErr) && exitErr.ExitCode() > 0
}

// CreatePV writes an LVM Physical Volume label onto path.
//
// It takes a context because it is one of the two write commands the
// spec.fileDevices paths depend on (the other is ResizePV), and every other step
// of those sequences — fallocate, losetup, losetup -c — is already bounded by
// CMD_DEADLINE_DURATION. An unbounded pvcreate in the middle of a bounded
// sequence means the reconcile as a whole has no deadline, which is the one
// property the per-command budget exists to provide.
func (commands) CreatePV(ctx context.Context, path string) (string, error) {
	args := []string{"pvcreate", path}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// A benign non-zero exit must not be reported as a failure: pvcreate has
	// already written the PV label by the time lvm.static complains about a
	// leaked file descriptor, and surfacing that as an error wrongly trips the
	// create/extend rollback against a device that is in fact a healthy PV.
	// A failure with nothing on stderr is not benign, though — pvcreate has no
	// known silent no-op, so silence means nobody knows what happened.
	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignAlwaysStdErr, silentExitIsFailure); err != nil {
		return cmd.String(), err
	}

	return cmd.String(), nil
}

// vgCreateArgs builds the vgcreate argv for a Volume Group of this module's.
//
// Shared by the local and shared variants rather than written out twice, because
// written out twice is how the shared one came to pass "--addtag" one time too
// many: vgcreate then read the second "--addtag" as the first one's value and the
// lvmVolumeGroupName tag as a positional device path, so a shared Volume Group was
// never tagged with its owning LVMVolumeGroup — the tag every ownership check in
// the agent, file devices included, reads. The tags are the part worth keeping in
// one place; the `--shared` flag is the only real difference.
// vgCreateArgs builds a vgcreate invocation. extraArgs go in front of the Volume
// Group name, which is where lvm expects mode flags (`--shared`) and injected
// configuration (`--config`); callers that need neither pass nil.
func vgCreateArgs(vgName, lvmVolumeGroupName string, extraArgs []string, pvNames []string) []string {
	args := []string{"vgcreate"}
	args = append(args, extraArgs...)
	args = append(args, vgName)
	args = append(args, pvNames...)
	return append(args,
		"--addtag", internal.LVMTags[0],
		"--addtag", fmt.Sprintf("%s=%s", internal.LVMVolumeGroupTag, lvmVolumeGroupName),
	)
}

// CreateVGLocal assembles a Volume Group out of the given Physical Volumes and
// tags it as this module's.
//
// Filtered through errIfNotBenign for the same reason CreatePV is, and with the
// same stakes the create rollback already assumes: rollbackProvisionedFileDevices
// names "a pvcreate/vgcreate that materially succeeded but returned a non-zero
// status" as a state it has to defend against. Left unfiltered, a leaked file
// descriptor on an invocation that in fact wrote the VG metadata reaches
// reconcileLVGCreateFunc as VGCreationFailed and puts the LVMVolumeGroup in a
// failed state over a Volume Group that exists. Silence stays a failure: vgcreate
// has no known no-op.
func (commands) CreateVGLocal(vgName, lvmVolumeGroupName string, pvNames []string) (string, error) {
	extendedArgs := lvmStaticExtendedArgs(vgCreateArgs(vgName, lvmVolumeGroupName, nil, pvNames))
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignAlwaysStdErr, silentExitIsFailure); err != nil {
		return cmd.String(), err
	}

	return cmd.String(), nil
}

// CreateLVShared creates a thick volume in a shared Volume Group.
//
// Activation is left at lvm's default and -Z y is explicit, and the two go
// together. lvcreate --activate n turns off lvm's own zeroing of the volume
// head, and a volume whose head still carries the previous tenant's superblock
// is a volume blkid identifies as an ext4 that the consumer never made — which
// is how a fresh PersistentVolume comes up already "formatted".
//
// This closes MISIDENTIFICATION, not disclosure. Reading the previous tenant's
// data is stopped by the wipe on deletion, or by the array returning zeroes
// after unmapping — never by zeroing four kilobytes at the front.
//
// The volume is then deactivated by the caller: creating is not attaching, and
// a metadata owner that kept every volume it created would hold the exclusive
// lock of the entire pool.
func (commands) CreateLVShared(ctx context.Context, vgName, lvName, size string) (string, error) {
	// lvm reads a bare number as megabytes, and the size arrives here as a byte
	// count — so passing it through unconverted asks for a volume a million times
	// too large.
	// --setautoactivation n for the same reason the group carries it: a volume
	// this cluster hands to one node at a time must not be activated by anything
	// that merely sees the disk. The group's setting is not inherited by volumes
	// created in it, so it is repeated here.
	args := []string{
		"lvcreate", "-n", fmt.Sprintf("%s/%s", vgName, lvName),
		"-L", lvmSize(size), "-W", "y", "-y", "--setautoactivation", "n",
	}
	extendedArgs, err := lvmStaticLockdArgs(args, 0)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// RemoveLVShared destroys a volume of a shared Volume Group.
func (commands) RemoveLVShared(ctx context.Context, vgName, lvName string) (string, error) {
	args := []string{"lvremove", "-y", fmt.Sprintf("%s/%s", vgName, lvName)}
	extendedArgs, err := lvmStaticLockdArgs(args, 0)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// LVExtendShared grows a volume of a shared Volume Group.
//
// It must run on the node that holds the volume's lock, which is the node the
// volume is attached to and not the metadata owner. That is not a preference:
// lvextend takes the LV lock, and under lvmlockd the lock is held exclusively by
// the activating node — the owner's attempt would simply be refused.
//
// The size is the requested one, not a delta, so a retry after a partial failure
// asks for the same end state rather than adding twice.
func (commands) LVExtendShared(ctx context.Context, vgName, lvName, size string) (string, error) {
	args := []string{"lvextend", "-L", lvmSize(size), fmt.Sprintf("%s/%s", vgName, lvName)}
	extendedArgs, err := lvmStaticLockdArgs(args, 0)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// A volume that is already the requested size makes lvextend exit non-zero
	// with "matches existing size", and for an idempotent caller that is a
	// success — the same benign-stderr treatment the local path already gives it.
	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignResizeStdErr, silentExitIsBenign); err != nil {
		return cmd.String(), err
	}
	return cmd.String(), nil
}

// SetLVTagShared adds or removes a tag on a volume of a shared group.
//
// Tags are where the "not cleaned yet" marker lives, and the reason is
// ownership rather than taste: the marker has to survive the metadata owner
// changing between the moment a wipe starts and the moment it finishes. A
// marker in the resource status would be written by the old owner and never
// read by the new one, which leaves a volume that was never wiped looking
// exactly like one that was.
func (commands) SetLVTagShared(ctx context.Context, vgName, lvName, tag string, add bool) (string, error) {
	flag := "--deltag"
	if add {
		flag = "--addtag"
	}

	args := []string{"lvchange", flag, tag, fmt.Sprintf("%s/%s", vgName, lvName)}
	extendedArgs, err := lvmStaticLockdArgs(args, 0)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// SharedVGParams is everything vgcreate needs for a shared Volume Group, and
// every field of it is irreversible once the group exists.
type SharedVGParams struct {
	VGName                string
	SharedVolumeGroupName string
	PVPaths               []string
	HostID                int
	// PhysicalExtentSize and MetadataSize are passed verbatim to lvm, which
	// accepts suffixed sizes.
	PhysicalExtentSize string
	MetadataSize       string
	// SanlockAlignSizeMiB is 1, 2, 4 or 8. It fixes the size of the lease area
	// and the ceiling on host_id — 250 hosts per MiB of alignment, roughly —
	// and cannot be changed on an existing group.
	SanlockAlignSizeMiB int
}

// CreateVGShared creates the Volume Group of a pool.
//
// The command is a bootstrap as much as a creation: vgcreate --shared starts
// the lockspace itself and enables the global lock, so the node running it ends
// up a member of a pool that did not exist a moment earlier.
//
// host_id and the lease alignment go in through --config rather than being left
// to the daemon's configuration, and that is a version requirement rather than
// a preference. Since lvm2 2.03.27 the CLIENT checks local/host_id against the
// ceiling implied by the alignment, so a client that says nothing about either
// is refused in production while passing on an older stand. That mismatch is
// invisible until the day it is not.

// lvmSize turns a size written the way Kubernetes writes sizes into the way lvm
// reads them.
//
// The two notations look alike and are not, in both directions. "4Mi" is a valid
// quantity in every API of this module and a usage error to lvm, which exits 3
// and prints its help without naming the argument it disliked. And "4m" means
// four mebibytes to lvm and four thousandths of a byte to Kubernetes — so a
// value that parses to less than a kibibyte is left exactly as written rather
// than converted to the zero it would become.
//
// Anything else unparseable is passed through too: lvm accepts its own spelling
// and rejects nonsense with a better message than this could.
func lvmSize(size string) string {
	quantity, err := resource.ParseQuantity(size)
	if err != nil || quantity.Value() < 1024 {
		return size
	}
	return fmt.Sprintf("%dk", quantity.Value()/1024)
}

func (commands) CreateVGShared(ctx context.Context, params SharedVGParams) (string, error) {
	config := LVMGlobalFilterForOwnedLoops() + " " + internal.SharedLVMNoArchive + " global/use_lvmlockd=1"
	if params.HostID > 0 {
		config += " local/host_id=" + strconv.Itoa(params.HostID)
	}
	if params.SanlockAlignSizeMiB > 0 {
		config += " global/sanlock_align_size=" + strconv.Itoa(params.SanlockAlignSizeMiB)
	}

	// Autoactivation off, and it is not a preference. lvm2 on a host that has it
	// installed activates the volumes of a group it can see — on boot, and on
	// every appearance of a physical volume. For a pool that is the one thing
	// the whole design forbids: a node with no attachment taking a lock and
	// mapping somebody else's volume. The nodes measured here ship no lvm at
	// all, so nothing happens on them today; a pool is not a promise about one
	// distribution.
	extra := []string{"--config", config, "--shared", "--setautoactivation", "n"}
	if params.PhysicalExtentSize != "" {
		extra = append(extra, "--physicalextentsize", lvmSize(params.PhysicalExtentSize))
	}
	if params.MetadataSize != "" {
		extra = append(extra, "--metadatasize", lvmSize(params.MetadataSize))
	}

	args := vgCreateArgs(params.VGName, params.SharedVolumeGroupName, extra, params.PVPaths)
	argv, err := sharedLVMArgs(args...)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignCreateStdErr, silentExitIsFailure); err != nil {
		return cmd.String(), err
	}

	return cmd.String(), nil
}

func (commands) CreateThinPool(thinPoolName, vgName string, size int64) (string, error) {
	args := []string{"lvcreate", "-ay", "-L", fmt.Sprintf("%dk", size/1024), "-T", fmt.Sprintf("%s/%s", vgName, thinPoolName)}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func (commands) CreateThinPoolFullVGSpace(thinPoolName, vgName string) (string, error) {
	args := []string{"lvcreate", "-ay", "-l", "100%FREE", "-T", fmt.Sprintf("%s/%s", vgName, thinPoolName)}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func (commands) CreateThinLogicalVolumeFromSource(name string, sourceVgName string, sourceName string) (string, error) {
	return createSnapshotVolume(name, sourceVgName, sourceName, nil)
}

func (commands) CreateThinLogicalVolumeSnapshot(name string, sourceVgName string, sourceName string, tags []string) (string, error) {
	return createSnapshotVolume(name, sourceVgName, sourceName, tags)
}

func createSnapshotVolume(name string, sourceVgName string, sourceName string, tags []string) (string, error) {
	args := []string{"lvcreate", "-ay", "-s", "-kn", "-n", name, fmt.Sprintf("%s/%s", sourceVgName, sourceName), "-y"}

	for _, tag := range tags {
		args = append(args, "--addtag")
		args = append(args, tag)
	}

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

func (commands) CreateThinLogicalVolume(vgName, tpName, lvName string, size int64) (string, error) {
	args := []string{"lvcreate", "-ay", "-T", fmt.Sprintf("%s/%s", vgName, tpName), "-n", lvName, "-V", fmt.Sprintf("%dk", size/1024), "-W", "y", "-y"}
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

func (commands) CreateThickLogicalVolume(vgName, lvName string, size int64, contiguous bool) (string, error) {
	args := []string{"lvcreate", "-ay", "-n", fmt.Sprintf("%s/%s", vgName, lvName), "-L", fmt.Sprintf("%dk", size/1024), "-W", "y", "-y"}
	if contiguous {
		args = append(args, "--contiguous", "y")
	}

	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	return cmd.String(), nil
}

// ExtendVG adds devices to an existing Volume Group.
//
// Filtered through errIfNotBenign like CreatePV and CreateVGLocal, and this is
// the one where an unfiltered non-zero exit costs the most. Its error reaches
// reconcileLVGUpdateFunc, which writes VGConfigurationApplied=False with reason
// VGExtendFailed — a reason deliberately absent from the conditions watcher's
// acceptableReasons, so the aggregate Ready condition goes False and the
// scheduler stops placing volumes on a Volume Group that is serving every volume
// it has. A leaked file descriptor printed after vgextend had already written the
// metadata is not a reason to take storage out of service. Silence stays a
// failure: vgextend has no known no-op.
func (commands) ExtendVG(vgName string, paths []string) (string, error) {
	args := []string{"vgextend", vgName}
	args = append(args, paths...)
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignAlwaysStdErr, silentExitIsFailure); err != nil {
		return cmd.String(), err
	}

	return cmd.String(), nil
}

func (commands) ExtendLV(size int64, vgName, lvName string) (string, error) {
	args := []string{"lvextend", "-L", fmt.Sprintf("%dk", size/1024), filepath.Join("/dev", vgName, lvName)}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// A resize that changes nothing is the normal state of a thin pool sized as
	// a percentage of the VG, and not every LVM version explains itself when it
	// exits non-zero over one; see silentExitPolicy.
	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignResizeStdErr, silentExitIsBenign); err != nil {
		return cmd.String(), err
	}

	return cmd.String(), nil
}

func (commands) ExtendLVFullVGSpace(vgName, lvName string) (string, error) {
	args := []string{"lvextend", "-l", "100%VG", filepath.Join("/dev", vgName, lvName)}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// The no-op case this tolerates is the rule rather than the exception here:
	// a 100% thin pool always already fills the VG. See silentExitPolicy.
	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignResizeStdErr, silentExitIsBenign); err != nil {
		return cmd.String(), err
	}

	return cmd.String(), nil
}

// ResizePV makes a Physical Volume take up the current size of its device. It
// is bounded by a context for the same reason CreatePV is: it is the last step
// of the in-place growth sequence, whose other two steps already are.
func (commands) ResizePV(ctx context.Context, pvName string) (string, error) {
	args := []string{"pvresize", pvName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// pvresize is filtered like CreatePV and ExtendLV: under nsenter lvm.static
	// prints "File descriptor N leaked on lvm.static invocation" and exits
	// non-zero on an operation that in fact succeeded, and treating that as a
	// failure is what once made the create rollback delete a live PV's backing
	// file and double the VG. Silence is still a failure, as for pvcreate: a
	// pvresize that did nothing and said nothing must not be reported as growth
	// that happened.
	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignAlwaysStdErr, silentExitIsFailure); err != nil {
		return cmd.String(), err
	}

	return cmd.String(), nil
}

// RemoveVGShared removes the Volume Group of a pool, and it is the last step of
// a cluster-wide operation rather than a command that stands alone.
//
// Three things have to be true before it can succeed, and each of them was
// learned by watching it fail on a live pool:
//
//   - the archive has to be off. lvm writes /etc/lvm/archive before changing
//     metadata, the lock daemons' image has a read-only rootfs, and the refusal
//     to create that directory is what fails the command;
//   - every OTHER member must have stopped its lockspace, or lvm answers
//     "Lockspace for ... not stopped on other hosts". Stopping them is not
//     something this node can do — each member stops its own;
//   - sanlock must have waited out its own interval afterwards, or it answers
//     "unknown host state (wait and retry)": it will not vouch for the absence
//     of other owners until then. That answer is not a failure, it is the
//     protocol asking for time, which is why the caller retries rather than
//     giving up.
//
// It runs in the lock daemons' mount namespace like every other shared command,
// because the lvm that can speak to lvmlockd lives there.
// ExtendVGShared adds devices to the Volume Group of a pool.
//
// It runs in the lock daemons' mount namespace, like every command that changes
// the metadata of a shared group: the lvm that can take the group's lock lives
// there, and lvmlockd is what serialises this against the other members. With
// the module's own static lvm it would either be refused or — worse — go through
// without a lock.
//
// The archive is off for the same reason vgremove needs it off: the daemons'
// image has a read-only rootfs, and lvm writes /etc/lvm/archive before touching
// metadata.
func (commands) ExtendVGShared(ctx context.Context, vgName string, paths []string) (string, error) {
	args := append([]string{"vgextend", "--config", internal.SharedLVMNoArchive, vgName}, paths...)
	argv, err := sharedLVMArgs(args...)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignAlwaysStdErr, silentExitIsFailure); err != nil {
		// A device that is already in the group is the outcome this was called
		// for. It happens for a window after every extension, because the lvm
		// that changes a shared group runs in the lock daemons' mount namespace
		// and the lvm that reads it here runs in the host's: the two keep
		// separate caches, and the reader takes a minute or two to catch up.
		// Measured on a live pool — the extension worked and the next pass ran
		// it again against a device already added.
		if rePVAlreadyInVG.Match(stderr.Bytes()) {
			return cmd.String(), nil
		}
		return cmd.String(), err
	}
	return cmd.String(), nil
}

func (commands) RemoveVGShared(ctx context.Context, vgName string) (string, error) {
	args := []string{"vgremove", "--config", internal.SharedLVMNoArchive, "-y", vgName}
	argv, err := sharedLVMArgs(args...)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// SharedVGRemovalNeedsTime reports whether a failed removal is the protocol
// asking to be waited out rather than a fault. Both answers resolve on their
// own: the first once the other members stop, the second once sanlock has sat
// out its interval.
func SharedVGRemovalNeedsTime(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not stopped on other hosts") ||
		strings.Contains(text, "unknown host state") ||
		strings.Contains(text, "global lock failed")
}

func (commands) RemoveVG(vgName string) (string, error) {
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

func (commands) RemovePV(pvNames []string) (string, error) {
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

func (commands) RemoveLV(vgName, lvName string) (string, error) {
	args := []string{"lvremove", filepath.Join("/dev", vgName, lvName), "-y"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.Command(internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func (commands) VGChangeAddTag(ctx context.Context, vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	args := []string{"vgchange", vGName, "--addtag", tag}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stdErr: %s", cmd.String(), err, stdErr.String())
	}
	return cmd.String(), nil
}

func (commands) VGChangeDelTag(ctx context.Context, vGName, tag string) (string, error) {
	var outs, stdErr bytes.Buffer
	args := []string{"vgchange", vGName, "--deltag", tag}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stdErr: %s", cmd.String(), err, stdErr.String())
	}
	return cmd.String(), nil
}

func (commands) LVChangeDelTag(ctx context.Context, lv internal.LVData, tag string) (string, error) {
	tmpStr := filepath.Join("/dev/%s/%s", lv.VGName, lv.LVName)
	var outs, stdErr bytes.Buffer
	args := []string{"lvchange", tmpStr, "--deltag", tag}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)
	cmd.Stdout = &outs
	cmd.Stderr = &stdErr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stdErr: %s", cmd.String(), err, stdErr.String())
	}
	return cmd.String(), nil
}

// VGActivate activates a Volume Group exclusively on this node.
//
// There is deliberately no shared-activation mode. `vgchange -asy` lets several
// nodes hold the same Volume Group at once, which is safe only for a filesystem
// that expects it; on a plain one it is two writers on the same extents. Shared
// Volume Groups are not activated by this module at all — see SkipSharedVGs.
func (commands) VGActivate(ctx context.Context, vgName string) (string, error) {
	args := []string{"vgchange", "-ay", vgName}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// VGLockStart joins this node to the sanlock lockspace of a shared Volume
// Group, which is what makes the group's volumes lockable here at all.
//
// It can take minutes rather than seconds, and the caller must budget for that
// instead of treating a slow return as a hang. Taking a free host_id costs up
// to 5 x io_timeout; taking back an id whose own delta lease is still alive —
// a quick reboot, a restarted daemon pod, an OnDelete update — costs
// 14 x io_timeout + 60, which is 200 s on the defaults. lvm prints
// "Waiting for sanlock may take a few seconds to 3 min" and means it.
//
// host_id reaches lvmlockd through its --host-id-file and is passed here as
// well because the CLIENT checks it too: since lvm2 2.03.27 vgcreate --shared
// refuses a host_id outside the range implied by the lease alignment, and a
// client that says nothing about it is rejected in production while passing on
// an older stand.
func (commands) VGLockStart(ctx context.Context, vgName string, hostID int) (string, error) {
	args := []string{"vgchange", "--lock-start", vgName}
	extendedArgs, err := lvmStaticLockdArgs(args, hostID)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// LockspaceRunning asks the lock manager whether the lockspace of this Volume
// Group is started on this node.
//
// It exists because the alternative is believing an annotation this module wrote
// itself. lvmlockd and sanlock restart together and take every lockspace with
// them; the annotation stays, so the node goes on reporting itself a member of
// a pool it holds no lease in — and the attachment side, comparing generation
// stamps that both still say the same number, goes on believing its volume is
// locked. Measured on a live pool after restarting the lock daemons: the
// lockspace was gone, `lvmlockctl --info` printed nothing at all, and every
// piece of bookkeeping in the cluster still said the node was in the pool.
//
// An error is not an answer: a caller that cannot ask must not conclude the
// lockspace is down and start it again over a lockspace that is running.
func (commands) LockspaceRunning(ctx context.Context, vgName string) (bool, error) {
	// Both daemons are asked, and both have to say yes.
	//
	// lvmlockd answers whether the lockspace is registered with it, and that
	// survives the lease being lost: on the stand a node whose registration had
	// been taken off the array still had "LS sanlock lvm_vghw" in lvmlockctl
	// --info while sanlock had dropped the lockspace entirely and lvm answered
	// "lock skipped: storage errors for sanlock leases" to everything. The node
	// published a running lockspace it did not have.
	//
	// sanlock answers whether this host holds the lease, which is the fact the
	// rest of the module means by "started": it decides whether volumes may be
	// activated here and whether a LUN may be taken away from this node.
	registered, err := lockspaceRegisteredWithLVMLockd(ctx, vgName)
	if err != nil || !registered {
		return false, err
	}
	return sanlockHoldsLockspace(ctx, vgName)
}

func lockspaceRegisteredWithLVMLockd(ctx context.Context, vgName string) (bool, error) {
	argv, err := sharedNamespaceArgs(internal.SharedLockCtlCmd, "--info")
	if err != nil {
		return false, err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return false, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}

	// lvmlockd names a started lockspace "lvm_<vg>" on a line of its own.
	return lockspaceListed(out.String(), vgName), nil
}

func sanlockHoldsLockspace(ctx context.Context, vgName string) (bool, error) {
	argv, err := sharedNamespaceArgs(internal.SharedSanlockCmd, "status")
	if err != nil {
		return false, err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return false, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return SanlockHoldsLockspace(out.String(), vgName), nil
}

// lockspaceListed reports whether lvmlockctl's answer contains the lockspace of
// this Volume Group.
func lockspaceListed(info, vgName string) bool {
	want := "lvm_" + vgName
	for _, line := range strings.Split(info, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "LS" {
			for _, f := range fields[1:] {
				if f == want {
					return true
				}
			}
		}
	}
	return false
}

// MultipathToolsVersion is the version of the tools in the lock daemons' image,
// which is the only one that matters: lvmpersist prepends the system
// MissingReservationTools names the reservation tooling that is not in the lock
// daemons' image, and it is a build check rather than a node check.
//
// Every reservation command runs from that image: lvm2 executes
// /sbin/lvmpersist by a path compiled into it, and lvmpersist runs sg_persist
// per path of the map. A pool asked to switch with either of them missing fails
// in the middle of the one-way door — `vgchange --setpersist require` has
// already made the group unusable by then — so it is established by looking
// first.
func (commands) MissingReservationTools(ctx context.Context) ([]string, error) {
	var missing []string
	for _, tool := range []string{internal.SharedSgPersistCmd, internal.SharedLvmPersistCmd} {
		argv, err := sharedNamespaceArgs("/bin/test", "-x", tool)
		if err != nil {
			return nil, err
		}
		cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			if cmd.ProcessState == nil {
				// The namespace itself could not be entered: that is not an
				// answer about the tooling.
				return nil, fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
			}
			missing = append(missing, tool)
		}
	}
	return missing, nil
}

// MultipathConfiguration is what the host's multipathd is actually running with,
// after its defaults, its drop-ins and its per-map sections have been merged.
//
// It is the only place the reservation key can be established before a pool is
// switched. `getprkey` answers "none" for every map until something registers a
// key, so a readiness check built on it could never pass for a pool that has not
// been switched yet — and the switch is what it was supposed to gate.
func (commands) MultipathConfiguration(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd,
		hostNamespaceArgs(internal.SharedMultipathdCmd, "show", "config")...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return out.String(), nil
}

// ReservationKeyOf asks the host's multipathd for the reservation key of a map.
// An empty or "none" answer means every reservation command will be refused
// before it reaches the array, and that a path which comes back will not be
// re-registered.
func (commands) ReservationKeyOf(ctx context.Context, mapName string) (string, error) {
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd,
		hostNamespaceArgs(internal.SharedMultipathdCmd, "getprkey", "map", mapName)...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return strings.TrimSpace(out.String()), nil
}

// hostNamespaceArgs runs a command of the host in the host's mount namespace.
//
// Only multipathd is asked this way, and only questions. The maps, the keys in
// /etc/multipath/prkeys and the re-registration of a returning path all belong
// to the host's multipathd; the daemons' image carries no multipathd at all.
func hostNamespaceArgs(command string, args ...string) []string {
	return append([]string{"-t", "1", "-m", "--", command}, args...)
}

// sharedNamespaceArgs runs a command of the lock daemons' image in their mount
// namespace, the way every shared-pool command runs.
func sharedNamespaceArgs(command string, args ...string) ([]string, error) {
	pid, err := SharedLVMNamespacePID()
	if err != nil {
		return nil, err
	}
	return append([]string{"-t", strconv.Itoa(pid), "-m", "--", command}, args...), nil
}

// VGSetPersist declares that the group requires SCSI-3 persistent reservations.
//
// It is the one-way door: from the moment it succeeds the group answers
// "Persistent reservation is not started" to every command until
// VGPersistStart does. It also needs THIS node's lockspace running — without one
// it fails with "Cannot access VG ... due to failed lock" — which is the
// opposite of what "stop the lockspace everywhere" sounds like, and the
// everywhere means the neighbours.
func (commands) VGSetPersist(ctx context.Context, vgName string, hostID int) (string, error) {
	return runSharedLockdLVM(ctx, hostID, "vgchange", "--setpersist", "require", vgName)
}

// VGPersistStart registers this node with the array and takes the reservation.
// lvm2 runs lvmpersist for it, which is why the multipath-tools version in the
// daemons' image decides whether this works at all.
func (commands) VGPersistStart(ctx context.Context, vgName string, hostID int) (string, error) {
	return runSharedLockdLVM(ctx, hostID, "vgchange", "--persist", "start", vgName)
}

// VGPersistStop gives up this node's registration, which is what a member does
// to let the executor take the group over.
func (commands) VGPersistStop(ctx context.Context, vgName string, hostID int) (string, error) {
	return runSharedLockdLVM(ctx, hostID, "vgchange", "--persist", "stop", vgName)
}

// VGSetLockArgsPersist records in the group's metadata that its lockspaces run
// under reservations, so a node starting one later does the same.
//
// It is the last step because of what it checks: keys still on the array, and
// sanlock not yet convinced the neighbours have gone. Both pass with time.
func (commands) VGSetLockArgsPersist(ctx context.Context, vgName string, hostID int) (string, error) {
	return runSharedLockdLVM(ctx, hostID, "vgchange", "--setlockargs", "persist", vgName)
}

// VGPersistSetting reads what the group itself says about reservations: whether
// it requires them, and what its lock args are. Both are readable when nothing
// else about the group is.
//
// This is what makes the switch resumable. Once `--setpersist require` has
// succeeded the group answers "Cannot access VG due to failed lock" to every
// command that takes a lock — including `--setpersist` itself — so a procedure
// that always starts from the beginning can never finish what it started. The
// setting lives in the metadata and `vgs` reads it without a lock, saying so.
func (commands) VGPersistSetting(ctx context.Context, vgName string) (string, error) {
	argv, err := sharedLVMArgs("vgs", "--noheadings", "-o", "vg_persist,vg_lock_args", vgName)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return strings.TrimSpace(out.String()), nil
}

// runSharedLockdLVM runs an lvm command that speaks to lvmlockd, with this
// node's identity attached.
//
// Every persistent-reservation command needs it: lvm2 derives the key a node
// registers with from its host id, and without one `vgchange --setpersist` stops
// with "A local pr_key or host_id is required to use PR (see lvmlocal.conf)".
// The id cannot live in that file — it is baked into the image, and it is the
// one thing that differs per node — so it is passed the same way lock-start
// passes it.
func runSharedLockdLVM(ctx context.Context, hostID int, args ...string) (string, error) {
	extendedArgs, err := lvmStaticLockdArgs(args, hostID)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// runSharedLVM runs an lvm command in the lock daemons' namespace, which is
// where the lvm that can speak to lvmlockd lives.
func runSharedLVM(ctx context.Context, args ...string) (string, error) {
	argv, err := sharedLVMArgs(args...)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := errIfNotBenign(cmd.String(), cmd.Run(), stderr, benignAlwaysStdErr, silentExitIsFailure); err != nil {
		return cmd.String(), err
	}
	return cmd.String(), nil
}

// ReadRegistrationKeys lists the keys registered on a path of a LUN.
//
// Read through sg_persist on the path rather than through mpathpersist on the
// map: the reading works either way, and using one tool for both halves keeps
// the keys this module compares in the same spelling as the keys it preempts.
func (commands) ReadRegistrationKeys(ctx context.Context, path string) ([]string, string, error) {
	argv, err := sharedNamespaceArgs(internal.SharedSgPersistCmd, "--in", "--read-keys", path)
	if err != nil {
		return nil, "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return ParseRegistrationKeys(out.String()), cmd.String(), nil
}

// RecordedReservationKey reads the key this node last registered with, as
// lvmpersist wrote it down.
//
// Nothing here can derive it: lvm2 computes it from the sanlock host id and the
// lockspace generation, and a node that restarted its lockspace comes back with
// a different one. An empty answer means this node has not registered yet, which
// is what a pool that has not been switched looks like.
func (commands) RecordedReservationKey(ctx context.Context) (string, error) {
	argv, err := sharedNamespaceArgs("/bin/cat", internal.SharedReservationKeyFile)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// No file yet is not a fault: it is a node that has not registered.
		if strings.Contains(stderr.String(), "No such file") {
			return "", nil
		}
		return "", fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return strings.TrimSpace(out.String()), nil
}

// SetReservationKey tells the host's multipathd which key a map is registered
// with.
//
// Without it multipathd knows nothing of the registration — we register with
// sg_persist, not through the library multipathd shares with mpathpersist — and
// a path that comes back after a failure is left unregistered. Under a Write
// Exclusive, all registrants reservation that path then refuses this node's
// writes, which looks like a flapping path healing and quietly not working.
func (commands) SetReservationKey(ctx context.Context, mapName, key string) (string, error) {
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd,
		// "setprkey map $map key $key" — the word "key" is part of the command,
		// and without it multipathd answers "not found" followed by its entire
		// CLI reference, which is easy to read as a version difference rather
		// than a typo.
		hostNamespaceArgs(internal.SharedMultipathdCmd, "setprkey", "map", mapName, "key", key)...)

	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	// multipathd answers "ok" on success and prints its refusal on the same
	// channel with exit status 0, so the exit status alone proves nothing.
	if answer := out.String(); !MultipathdAccepted(answer) {
		return cmd.String(), fmt.Errorf("multipathd refused the key of %s: %s", mapName, strings.TrimSpace(answer))
	}
	return cmd.String(), nil
}

// PreemptRegistration takes a key off a LUN, on one path.
//
// This is the operation the whole reservation branch exists for: a node that
// cannot be asked to stop — cut off from the API, or hung — is denied by the
// array itself, because its neighbours remove its registration and the array
// stops accepting its writes.
//
// It is done with sg_persist and not with lvm2's own lvmpersist remove, and that
// is not a preference. On a multipath map every preempt is refused with
// "configured reservation key doesn't match: 0x0" — libmpathpersist compares the
// key given to it against one it reads itself and gets zero — regardless of how
// the key is configured, on either version tried, from the container and from
// the host. The same operation through sg_persist on a single path completes in
// a third of a second.
func (commands) PreemptRegistration(ctx context.Context, path, ourKey, theirKey string) (string, error) {
	argv, err := sharedNamespaceArgs(internal.SharedSgPersistCmd,
		"--out", "--preempt-abort",
		"--param-rk="+ourKey,
		"--param-sark="+theirKey,
		// Write Exclusive, all registrants: the type the pool's reservation is
		// held under, and a preempt has to name the type that is in force.
		"--prout-type=7",
		path,
	)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, argv...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// VGLockStop leaves the lockspace of a shared Volume Group.
//
// Order matters and is not enforced here: every logical volume of the group has
// to be deactivated on this node first. Stopping the lockspace under an active
// volume leaves the volume writable with no lock behind it, which is the one
// state the whole design exists to prevent. The caller checks; this only runs
// the command.
func (commands) VGLockStop(ctx context.Context, vgName string) (string, error) {
	args := []string{"vgchange", "--lock-stop", vgName}
	extendedArgs, err := lvmStaticLockdArgs(args, 0)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// LVActivateShared activates volumes of a shared Volume Group, and takes a
// LIST rather than one name because that is the whole point of it.
//
// A batch takes the Volume Group lock once for the entire list. Measured on a
// 32-node pool: sixteen volumes in one command give 68 activations per second
// against 13 for a loop over the same sixteen, and the lock — not the disk — is
// what the difference is made of. Every mass event goes through here: a node
// returning after a reboot, a pool coming back after an outage, a node starting
// with dozens of volumes. The single-volume path stays where it is natural, in
// NodeStageVolume for one volume.
//
// The price of one command is one exit code for the whole list, so the CALLER
// must compare the set of active volumes against what it asked for instead of
// trusting the return.
//
// shared selects -asy over -aey. It is correct only for a volume whose consumer
// arbitrates access itself — a block volume with ReadWriteMany — and on an
// ordinary filesystem it means two writers on the same extents.
func (commands) LVActivateShared(ctx context.Context, vgName string, lvNames []string, shared bool) (string, error) {
	if len(lvNames) == 0 {
		return "", nil
	}

	mode := "-aey"
	if shared {
		mode = "-asy"
	}

	args := append([]string{"lvchange", mode}, lvPaths(vgName, lvNames)...)
	extendedArgs, err := lvmStaticLockdArgs(args, 0)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// LVDeactivateShared releases volumes of a shared Volume Group, which is what
// hands their locks to whoever wants them next. Same batching, same caveat
// about the single exit code.
// WipeDMTable replaces a device-mapper table with an error target: the barrier.
//
// It is the same command the fencing handler runs, and it is here so that a node
// the pool has removed can raise the barrier over its own volumes instead of
// waiting for somebody to do it over SSH. The difference between this and
// removing the device is the whole point: a write in flight has to FAIL rather
// than find nothing, because the volume it was aimed at may already belong to
// another node.
//
// --force replaces the table of a device that is open, which is exactly the case
// this exists for; --noudevsync because udev cannot be waited on when the node
// is being taken out of a pool it can no longer talk to.
func (commands) WipeDMTable(ctx context.Context, dmName string) (string, error) {
	args := nsentrerExpendedArgs(internal.DMSetupCmd, "wipe_table", "--force", "--noudevsync", dmName)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// RemoveDMDevice tears down a device-mapper device by name, and it exists
// because lvchange cannot.
//
// lvm decides whether a volume is active HERE from the lock it holds, not from
// device-mapper: with the lock gone, "lvchange -an" finds nothing to do, exits
// zero, and leaves the mapping standing. That mapping is the residue of a
// lock-daemon restart — a device with no lease behind it — and dmsetup is the
// only tool that addresses the kernel directly enough to remove it.
//
// It is refused for an open device, which is the safety net kept deliberately:
// a mapping something is still using is not residue.
func (commands) RemoveDMDevice(ctx context.Context, dmName string) (string, error) {
	args := nsentrerExpendedArgs(internal.DMSetupCmd, "remove", dmName)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// A device that is not there is the outcome this was called for, not a
		// failure to report. Callers used to check first and then remove, and
		// the check was the weak part: on the stand a node read /sys/block,
		// found nothing, and skipped the removal of a mapping dmsetup listed at
		// the same moment. One command, one authority.
		if reNoSuchDMDevice.Match(stderr.Bytes()) {
			return cmd.String(), nil
		}
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// RemoveDMDeviceDeferred asks device-mapper to drop the mapping when its last
// opener closes it.
//
// It exists for one case: a map that has to go and is held open by a process
// that will let go on its own. Retrying the plain removal there produces the
// same "Device or resource busy" every thirty seconds for as long as the node
// lives, which reads as an agent doing nothing. The deferred form turns that
// into a decision recorded in the kernel — the mapping is gone the moment it is
// no longer in use, with nobody watching for the moment.
//
// It is not a stronger removal. A map with a live opener stays usable until the
// close, so this must never stand in for the barrier: an error target under a
// writer is what stops the writes, and this only cleans up afterwards.
func (commands) RemoveDMDeviceDeferred(ctx context.Context, dmName string) (string, error) {
	args := nsentrerExpendedArgs(internal.DMSetupCmd, "remove", "--deferred", dmName)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func (commands) LVDeactivateShared(ctx context.Context, vgName string, lvNames []string) (string, error) {
	if len(lvNames) == 0 {
		return "", nil
	}

	args := append([]string{"lvchange", "-an"}, lvPaths(vgName, lvNames)...)
	extendedArgs, err := lvmStaticLockdArgs(args, 0)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func lvPaths(vgName string, lvNames []string) []string {
	paths := make([]string, 0, len(lvNames))
	for _, lvName := range lvNames {
		paths = append(paths, vgName+"/"+lvName)
	}
	return paths
}

func (commands) LVActivate(ctx context.Context, vgName, lvName string) (string, error) {
	lvPath := filepath.Join("/dev", vgName, lvName)
	args := []string{"lvchange", "-ay", lvPath}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func (commands) VGScan(ctx context.Context) (string, error) {
	args := []string{"vgscan", "--cache"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

func (commands) PVScan(ctx context.Context) (string, error) {
	args := []string{"pvscan", "--cache"}
	extendedArgs := lvmStaticExtendedArgs(args)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// UdevadmTrigger sends a "change" uevent for the given device paths so that
// the host udev re-probes them and updates its database. This is necessary
// because lvm.static is built without udev integration: after pvcreate/vgcreate
// the udev DB stays stale and lsblk never reports LVM2_member as fstype.
//
// Defensive contract:
//   - When paths is empty the function returns ("", nil) without invoking
//     udevadm. Running `udevadm trigger --action=change` with no positional
//     arguments would otherwise trigger ALL matching devices on the host,
//     producing a burst of uevents that can disturb other udev consumers
//     (multipathd, sds-replicated-volume, etc.).
//   - "--" end-of-options is inserted before the path list so that any path
//     starting with '-' (an unexpected but cheap-to-defend case) is treated
//     as a positional argument rather than as an udevadm flag.
func (commands) UdevadmTrigger(ctx context.Context, paths []string) (string, error) {
	if len(paths) == 0 {
		return "", nil
	}

	extendedArgs := udevadmTriggerExtendedArgs(paths)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, extendedArgs...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to run cmd: %s, err: %w, stderr: %s", cmd.String(), err, stderr.String())
	}
	return cmd.String(), nil
}

// udevadmTriggerExtendedArgs builds the argv passed to nsenter+udevadm for
// the change-uevent trigger after pvcreate/vgcreate. It is split out from
// UdevadmTrigger so that argv construction can be unit-tested without
// invoking exec.
//
// The first three argv slots are fixed (`udevadm`, `trigger`,
// `--action=change`); a literal `--` end-of-options separator is appended
// before the path list to guarantee that any path beginning with '-' is
// treated as a positional argument rather than an udevadm flag. Callers
// MUST guarantee len(paths) > 0; passing an empty slice would otherwise
// trigger ALL block devices on the host (udevadm-trigger(8) default).
func udevadmTriggerExtendedArgs(paths []string) []string {
	args := []string{"udevadm", "trigger", "--action=change", "--"}
	args = append(args, paths...)
	return nsentrerExpendedArgs(args[0], args[1:]...)
}

func (commands) UnmarshalDevices(out []byte) ([]internal.Device, error) {
	var devices internal.Devices
	if err := json.Unmarshal(out, &devices); err != nil {
		return nil, err
	}

	return devices.BlockDevices, nil
}

// ReTag walks managed LVs/VGs and replaces the legacy linstor tag with the
// current LVMTag. Each underlying LVM command is executed with its own
// timeout (derived from CMD_DEADLINE_DURATION) and the caller's context, so a
// stuck nsenter-backed command cannot block the agent indefinitely and a
// SIGTERM from kubelet propagates immediately to the child process.
//
// Volume Groups that live entirely on loop devices the agent does not own are
// skipped. This function WRITES LVM metadata — and worse than that, its write
// makes a Volume Group the module's own, since it replaces the legacy tag with
// storage.deckhouse.io/enabled=true, after which the discoverer will adopt it.
// Its only gate is the legacy tag, which a guest running LINSTOR inside a
// file-backed disk carries too; until spec.fileDevices removed `loop` from
// LVMGlobalFilter such a Volume Group was invisible to lvm.static and the
// question never arose. See utils/loopvg.go.
func (c *commands) ReTag(ctx context.Context, log logger.Logger, metrics *monitoring.Metrics, ctrlName string, cmdTimeout time.Duration) error {
	return reTag(ctx, c, log, metrics, ctrlName, cmdTimeout)
}

// reTag is ReTag's body, taking the command set as a parameter so the ownership
// gate below can be tested. The method reads through the same interface it
// implements, which is otherwise impossible to stand in for.
func reTag(ctx context.Context, c Commands, log logger.Logger, metrics *monitoring.Metrics, ctrlName string, cmdTimeout time.Duration) error {
	log.Debug("[ReTag] start establishing which VGs are the module's own")
	start := time.Now()
	type vgsResult struct {
		data   []internal.VGData
		cmdStr string
	}
	vgsRes, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (vgsResult, error) {
		data, cmdStr, _, err := c.GetAllVGs(ctx)
		return vgsResult{data: data, cmdStr: cmdStr}, err
	})
	metrics.UtilsCommandsDuration(ctrlName, "vgs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(ctrlName, "vgs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", vgsRes.cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(ctrlName, "vgs").Inc()
		log.Error(err, "[ReTag] unable to GetAllVGs")
		return err
	}

	start = time.Now()
	type pvsResult struct {
		data   []internal.PVData
		cmdStr string
	}
	pvsRes, pvsErr := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (pvsResult, error) {
		data, cmdStr, _, err := c.GetAllPVs(ctx)
		return pvsResult{data: data, cmdStr: cmdStr}, err
	})
	pvs, pvsCmd := pvsRes.data, pvsRes.cmdStr
	metrics.UtilsCommandsDuration(ctrlName, "pvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(ctrlName, "pvs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", pvsCmd))
	if pvsErr != nil {
		// Retagging is a one-off migration of a legacy tag; not doing it costs
		// nothing until the next restart. Doing it to the wrong Volume Group cannot
		// be undone, because the tag it replaces is gone afterwards.
		metrics.UtilsCommandsErrorsCount(ctrlName, "pvs").Inc()
		log.Error(pvsErr, "[ReTag] unable to GetAllPVs to establish which VGs are the module's own")
		return pvsErr
	}

	verdicts := ClassifyLoopVGs(ctx, log, c, cmdTimeout, vgsRes.data, pvs)
	ownVGs := SkipUnownedLoopVGs(log, "re-tag", vgsRes.data, verdicts)
	// An LV names its Volume Group but not its UUID, so LVs are matched by name.
	// A name shared by an owned and a foreign Volume Group therefore skips both,
	// which is the harmless direction: the legacy tag simply stays until the
	// duplicate is resolved.
	ownVGNames := make(map[string]struct{}, len(ownVGs))
	for _, vg := range ownVGs {
		ownVGNames[vg.VGName] = struct{}{}
	}

	// thin pool
	log.Debug("[ReTag] start re-tagging LV")
	start = time.Now()
	type lvsResult struct {
		data   []internal.LVData
		cmdStr string
	}
	lvsRes, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (lvsResult, error) {
		data, cmdStr, _, err := c.GetAllLVs(ctx)
		return lvsResult{data: data, cmdStr: cmdStr}, err
	})
	metrics.UtilsCommandsDuration(ctrlName, "lvs").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.UtilsCommandsExecutionCount(ctrlName, "lvs").Inc()
	log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", lvsRes.cmdStr))
	if err != nil {
		metrics.UtilsCommandsErrorsCount(ctrlName, "lvs").Inc()
		log.Error(err, "[ReTag] unable to GetAllLVs")
		return err
	}

	for _, lv := range lvsRes.data {
		if _, own := ownVGNames[lv.VGName]; !own {
			continue
		}
		tags := strings.Split(lv.LvTags, ",")
		for _, tag := range tags {
			if strings.Contains(tag, internal.LVMTags[0]) {
				continue
			}

			if strings.Contains(tag, internal.LVMTags[1]) {
				start = time.Now()
				cmdStr, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
					return c.LVChangeDelTag(ctx, lv, tag)
				})
				metrics.UtilsCommandsDuration(ctrlName, "lvchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(ctrlName, "lvchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(ctrlName, "lvchange").Inc()
					log.Error(err, "[ReTag] unable to LVChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
					return c.VGChangeAddTag(ctx, lv.VGName, internal.LVMTags[0])
				})
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
	for _, vg := range ownVGs {
		tags := strings.Split(vg.VGTags, ",")
		for _, tag := range tags {
			if strings.Contains(tag, internal.LVMTags[0]) {
				continue
			}

			if strings.Contains(tag, internal.LVMTags[1]) {
				start = time.Now()
				cmdStr, err := RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
					return c.VGChangeDelTag(ctx, vg.VGName, tag)
				})
				metrics.UtilsCommandsDuration(ctrlName, "vgchange").Observe(metrics.GetEstimatedTimeInSeconds(start))
				metrics.UtilsCommandsExecutionCount(ctrlName, "vgchange").Inc()
				log.Debug(fmt.Sprintf("[ReTag] exec cmd: %s", cmdStr))
				if err != nil {
					metrics.UtilsCommandsErrorsCount(ctrlName, "vgchange").Inc()
					log.Error(err, "[ReTag] unable to VGChangeDelTag")
					return err
				}

				start = time.Now()
				cmdStr, err = RunWithTimeout(ctx, cmdTimeout, func(ctx context.Context) (string, error) {
					return c.VGChangeAddTag(ctx, vg.VGName, internal.LVMTags[0])
				})
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

func (commands) CreateFileDevice(ctx context.Context, path string, sizeBytes int64) (string, error) {
	args := nsentrerExpendedArgs(internal.FallocateCmd, "-l", strconv.FormatInt(sizeBytes, 10), path)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to create file device %s: %w, stderr: %s", path, err, stderr.String())
	}
	return cmd.String(), nil
}

// GetFileAllocatedBytes returns how many bytes path actually occupies on its
// filesystem: `%b` blocks of `%B` bytes each.
//
// Allocated, not apparent (`%s`), because the only caller asks this in order to
// work out how much more `fallocate` still has to reserve. For a file the agent
// itself created the two agree — `fallocate -l` allocates everything it
// declares. They part company for a sparse file that turns up under a managed
// path (restored with `cp --sparse`, pre-created with `truncate`, copied by
// hand), where the apparent size is the full one while nothing is on disk:
// trusting it would compute "nothing left to allocate", skip the free-space
// guard entirely, and let fallocate fill the node's filesystem — the DiskPressure
// eviction that guard exists to prevent.
//
// Failure comes back in two flavours, and the difference decides whether a
// rollback is later allowed to `rm` the file:
//
//   - ErrFileDeviceAbsent — stat ran, looked, and exited non-zero. For a path the
//     agent is about to create this is ENOENT in all but pathological cases, and
//     the pathological ones (EACCES, EIO, ESTALE) make the fallocate that follows
//     fail too, so nothing is ever removed on their account.
//   - any other error — stat never got to look: it could not be started, was
//     killed, or the per-command deadline expired. Nothing at all is known about
//     the path.
//
// Collapsing the two is what let a transient timeout be read as "the file is not
// there", after which the create-path rollback removed a backing file that
// carried a live PV. Distinguishing them by exit status rather than by parsing
// stderr keeps the check out of locale-dependent strerror text.
func (commands) GetFileAllocatedBytes(ctx context.Context, path string) (string, int64, error) {
	args := nsentrerExpendedArgs(internal.StatCmd, "-c", "%b %B", path)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ranAndFailed(err) {
			return cmd.String(), 0, fmt.Errorf("%w: %q: %w, stderr: %s", ErrFileDeviceAbsent, path, err, stderr.String())
		}
		return cmd.String(), 0, fmt.Errorf("unable to stat %q: %w, stderr: %s", path, err, stderr.String())
	}

	size, err := parseStatAllocatedBytes(stdout.String())
	if err != nil {
		return cmd.String(), 0, fmt.Errorf("unable to parse the allocated size of %q: %w", path, err)
	}
	return cmd.String(), size, nil
}

// parseStatAllocatedBytes parses the "<blocks> <block-size>" output of
// `stat -c "%b %B"` into the number of bytes the file occupies.
func parseStatAllocatedBytes(out string) (int64, error) {
	fields := strings.Fields(out)
	if len(fields) != 2 {
		return 0, fmt.Errorf("expected 2 fields, got %q", strings.TrimSpace(out))
	}
	blocks, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid block count %q: %w", fields[0], err)
	}
	blockSize, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid block size %q: %w", fields[1], err)
	}
	if blocks < 0 || blockSize < 0 {
		return 0, fmt.Errorf("negative block count %d or block size %d", blocks, blockSize)
	}
	return blocks * blockSize, nil
}

func (commands) SetupLoopDevice(ctx context.Context, filePath string) (string, string, error) {
	// --nooverlap makes losetup reuse an already-attached loop for this
	// backing file (printing it via --show) instead of binding a second
	// minor to the same file. Without it, a race between the startup
	// reattach and the reconciler's provision step — or an existing
	// attachment that FindLoopDeviceByFile reported only under an alias —
	// would leak an extra loop device that nothing ever reaps.
	//
	// Direct I/O is deliberately NOT requested here; see SetLoopDirectIO for
	// why it has to be a separate, best-effort call.
	args := nsentrerExpendedArgs(internal.LosetupCmd, "--find", "--nooverlap", "--show", filePath)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), "", fmt.Errorf("unable to setup loop device for %s: %w, stderr: %s", filePath, err, stderr.String())
	}

	loopDev := strings.TrimSpace(stdout.String())
	// Registered here rather than at the call sites, because the very next thing
	// every caller does is run an LVM command against this device — pvcreate,
	// vgcreate, vgextend — and internal.LVMGlobalFilter rejects /dev/loop* until
	// the device is known to be ours. A call site that forgot to register would
	// hand lvm a device it has just been told to ignore, and the failure would read
	// as "pvcreate refused the device" with nothing pointing at the filter.
	RememberLoopIfManaged(filePath, loopDev)

	return cmd.String(), loopDev, nil
}

// SetLoopDirectIO asks the loop driver to open the backing file with O_DIRECT so
// reads and writes bypass the backing filesystem's page cache. Our stack layers
// a filesystem on top of LVM on top of the loop on top of the node's
// filesystem; without direct I/O every page is cached twice (once for the
// volume's filesystem, once for the backing file), doubling the RAM footprint
// and throttling throughput.
//
// It is a separate command, and its failure must be treated as a warning rather
// than an error, because `LOOP_SET_DIRECT_IO` is not best-effort in the kernel:
// loop_set_dio() returns -EINVAL when direct I/O could not be enabled — which is
// the case for any backing filesystem without an ->direct_IO implementation
// (tmpfs is the obvious one) or with an incompatible alignment. losetup applies
// --direct-io AFTER attaching the device, so folding it into SetupLoopDevice
// produced a non-zero exit with the loop already bound and nothing printed on
// stdout: the caller saw a failure, removed the backing file it had just
// created, and left the minor stranded on a deleted inode holding disk space
// that the filesystem could never reclaim — once per reconcile, forever.
//
// Buffered I/O is a performance regression. It is not a reason to refuse to
// provision storage.
func (commands) SetLoopDirectIO(ctx context.Context, loopDev string) (string, error) {
	args := nsentrerExpendedArgs(internal.LosetupCmd, "--direct-io=on", loopDev)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to enable direct I/O on loop device %s: %w, stderr: %s", loopDev, err, stderr.String())
	}
	return cmd.String(), nil
}

// SetLoopCapacity makes the loop driver re-read the size of its backing file
// (the LOOP_SET_CAPACITY ioctl). It is what turns a grown backing file into a
// grown block device, and it works on a live device: the filesystems and
// logical volumes stacked on top stay mounted and in use throughout.
//
// Shrinking is not the mirror image of this and is not supported anywhere in the
// growth path. Note that `fallocate -l` cannot shrink a file — with mode 0 it
// only ever allocates and, if needed, extends — so a smaller requested size is a
// no-op rather than a truncation. The reason shrinking is refused is not the
// file: it is that giving capacity back requires shrinking the Volume Group
// (pvmove + vgreduce), which the module does not do for block devices either.
func (commands) SetLoopCapacity(ctx context.Context, loopDev string) (string, error) {
	args := nsentrerExpendedArgs(internal.LosetupCmd, "-c", loopDev)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to refresh capacity of loop device %s: %w, stderr: %s", loopDev, err, stderr.String())
	}
	return cmd.String(), nil
}

func (commands) DetachLoopDevice(ctx context.Context, loopDev string) (string, error) {
	args := nsentrerExpendedArgs(internal.LosetupCmd, "-d", loopDev)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to detach loop device %s: %w, stderr: %s", loopDev, err, stderr.String())
	}

	// The minor goes back to the kernel's pool the moment this succeeds, and what
	// it is handed to next may well be a virtual machine's disk. An exemption left
	// behind here is an exemption for somebody else's storage.
	ForgetOwnedLoop(loopDev)

	return cmd.String(), nil
}

// ListLoopDevices enumerates the node's loop devices together with the files
// behind them.
//
// One command for the whole table rather than GetLoopBackingFile per device: this
// runs on every cache fill (utils.RefreshOwnedLoops) and a hypervisor carries a
// loop device per block-mode volume of every virtual machine on it — a hundred and
// fifty of them is ordinary, and that many nsenter calls per scan is not.
func (commands) ListLoopDevices(ctx context.Context) (string, []internal.LoopDeviceEntry, error) {
	args := nsentrerExpendedArgs(internal.LosetupCmd, "--noheadings", "--output", "NAME,BACK-FILE")
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), nil, fmt.Errorf("unable to list loop devices: %w, stderr: %s", err, stderr.String())
	}

	return cmd.String(), parseLoopDeviceTable(stdout.String()), nil
}

// parseLoopDeviceTable reads `losetup --noheadings --output NAME,BACK-FILE`.
//
// The device is the first field and the backing file is everything after it: a
// path may contain spaces, and the " (deleted)" marker losetup appends is itself
// separated by one. Splitting on all whitespace would silently truncate both, and
// a truncated path fails the ownership check — which would hide the agent's own
// file-backed device from lvm rather than admit somebody else's.
func parseLoopDeviceTable(out string) []internal.LoopDeviceEntry {
	lines := strings.Split(out, "\n")
	entries := make([]internal.LoopDeviceEntry, 0, len(lines))

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		device, backing, found := strings.Cut(trimmed, " ")
		if !found {
			// A loop device with no backing file at all: losetup lists it while it is
			// being torn down. Nothing to own.
			continue
		}
		entries = append(entries, internal.LoopDeviceEntry{
			Device:  device,
			Backing: parseBackingFile(backing),
		})
	}

	return entries
}

func (commands) FindLoopDeviceByFile(ctx context.Context, filePath string) (string, string, error) {
	args := nsentrerExpendedArgs(internal.LosetupCmd, "-j", filePath)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), "", fmt.Errorf("unable to find loop device for %s: %w, stderr: %s", filePath, err, stderr.String())
	}

	devices := parseLoopDeviceListing(stdout.String())
	switch len(devices) {
	case 0:
		return cmd.String(), "", nil
	case 1:
		// Already attached, so nothing registered it: this is the startup path after
		// a restart, where the loops of a file-backed Volume Group survived and the
		// in-process registry did not. Without this the LVM filter would hide the
		// node's own Volume Group until the first cache fill refreshed the set.
		RememberLoopIfManaged(filePath, devices[0])
		return cmd.String(), devices[0], nil
	default:
		// Two loop devices over one backing file are two Physical Volumes of the
		// same size over the same blocks — the state in which a file-backed Volume
		// Group silently doubled on a real cluster, with half of it on a loop whose
		// file had been unlinked. Returning the first device and moving on would let
		// every caller act on half the picture: provisioning would report "already
		// attached", cleanup would detach one of the two and `rm` the file the other
		// is still reading. So it is reported instead. The first device is still
		// returned, for the callers that only log it.
		return cmd.String(), devices[0], fmt.Errorf("%s is attached to %d loop devices (%s); refusing to act on one of them",
			filePath, len(devices), strings.Join(devices, ", "))
	}
}

// parseLoopDeviceListing extracts the loop device names from `losetup -j <file>`
// output, which prints one `/dev/loopN: <offset> <backing-file>` line per loop
// device bound to the file and nothing at all when there is none.
//
// Blank lines are dropped rather than turned into an empty device name: an empty
// name would be indistinguishable from "not attached" for a caller counting the
// result, which is what decides whether provisioning creates a second file.
func parseLoopDeviceListing(out string) []string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	devices := make([]string, 0, len(lines))
	for _, line := range lines {
		dev, _, _ := strings.Cut(line, ":")
		if dev = strings.TrimSpace(dev); dev != "" {
			devices = append(devices, dev)
		}
	}
	return devices
}

// GetLoopBackingFile returns the backing file loopDev is currently attached to.
// An empty Path means the device is not attached.
//
// Implemented via `losetup --noheadings --output BACK-FILE <loopDev>` so
// the agent does not have to spawn `cat /sys/block/<loop>/loop/backing_file`
// in the host PID namespace just to read a single value. Goes through the
// same nsenter wrapper as every other host command in this package so the
// argv stays unit-testable.
//
// The result carries the Deleted flag separately from the path rather than
// collapsing the two, because the two callers need opposite things from it:
// cleanup has to recognise an unlinked file as ours in order to detach the
// minor, while provisioning has to know the file is gone in order NOT to create
// a second one at the same path.
func (commands) GetLoopBackingFile(ctx context.Context, loopDev string) (string, internal.LoopBackingFile, error) {
	args := nsentrerExpendedArgs(internal.LosetupCmd, "--noheadings", "--output", "BACK-FILE", loopDev)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), internal.LoopBackingFile{}, fmt.Errorf("unable to read backing file for %s: %w, stderr: %s", loopDev, err, stderr.String())
	}
	return cmd.String(), parseBackingFile(stdout.String()), nil
}

// parseBackingFile splits the backing-file path losetup reports from the
// " (deleted)" marker it appends when the file has been unlinked while the loop
// is still attached, e.g. "/data/sds-vg-a.d0.img (deleted)".
//
// The marker has to come off the path: leaving it in makes
// IsManagedFileDevicePath miss the basename, cleanup refuse to detach the loop
// and the minor stay on the node forever. It also has to be reported, because a
// loop reading from a file nobody can open again is not a healthy file device
// and must not be published as one.
func parseBackingFile(out string) internal.LoopBackingFile {
	trimmed := strings.TrimSpace(out)
	path := strings.TrimSuffix(trimmed, deletedBackingFileMarker)
	return internal.LoopBackingFile{
		Path:    strings.TrimSpace(path),
		Deleted: len(path) != len(trimmed),
	}
}

func (commands) RemoveFileDevice(ctx context.Context, path string) (string, error) {
	args := nsentrerExpendedArgs(internal.RmCmd, "-f", path)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to remove file device %s: %w, stderr: %s", path, err, stderr.String())
	}
	return cmd.String(), nil
}

// EnsureFileDeviceDirectory creates directory (and any missing parents) on the
// host so the backing file can be allocated into it. The agent runs in PID 1's
// mount namespace, so this is `mkdir -p` against the node's root filesystem.
// `mkdir -p` is idempotent (no error if the directory already exists) and fails
// only when the path is genuinely unusable — a read-only filesystem, or a
// non-directory component along the way.
func (commands) EnsureFileDeviceDirectory(ctx context.Context, directory string) (string, error) {
	args := nsentrerExpendedArgs(internal.MkdirCmd, "-p", directory)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), fmt.Errorf("unable to create directory %q on the node: %w, stderr: %s", directory, err, stderr.String())
	}
	return cmd.String(), nil
}

// GetFilesystemSpace measures the filesystem holding directory: how many bytes
// can still be allocated in it, and how large it is in total.
//
// The available figure lets the caller refuse an oversized backing file before
// `fallocate` fails halfway. The total is what turns that into a guard against a
// node-level outage rather than merely against ENOSPC: the caller keeps a share
// of the filesystem free (see ReconcilerConfig.FileDevicesMinFreeSpacePercent),
// because kubelet starts evicting at `nodefs.available<10%` by default, long
// before the filesystem is actually full.
//
// It reads both with one `stat -f` in PID 1's mount namespace (the agent itself
// does not share the host mount namespace, so a Go syscall.Statfs would measure
// the wrong filesystem): %S is the fundamental block size, %b the total block
// count and %a the count of blocks available to a non-superuser. Using %a rather
// than %f deliberately leaves the filesystem's own superuser reserve untouched
// on top of the configured share.
func (commands) GetFilesystemSpace(ctx context.Context, directory string) (string, internal.FilesystemSpace, error) {
	args := nsentrerExpendedArgs(internal.StatCmd, "-f", "-c", "%S %b %a", directory)
	cmd := exec.CommandContext(ctx, internal.NSENTERCmd, args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return cmd.String(), internal.FilesystemSpace{}, fmt.Errorf("unable to stat filesystem for %q: %w, stderr: %s", directory, err, stderr.String())
	}

	space, err := parseStatfsSpace(stdout.String())
	if err != nil {
		return cmd.String(), internal.FilesystemSpace{}, fmt.Errorf("unable to parse free space for %q: %w", directory, err)
	}
	return cmd.String(), space, nil
}

// parseStatfsSpace parses the "<block-size> <total-blocks> <available-blocks>"
// output of `stat -f -c "%S %b %a"` into a FilesystemSpace.
//
// A non-positive block size or total block count is rejected rather than passed
// on: no real filesystem reports one, and the caller reads TotalBytes <= 0 as
// "size unknown, skip the reserve" — a value that must never be reachable from a
// successful stat.
func parseStatfsSpace(out string) (internal.FilesystemSpace, error) {
	fields := strings.Fields(out)
	if len(fields) != 3 {
		return internal.FilesystemSpace{}, fmt.Errorf("expected 3 fields, got %q", strings.TrimSpace(out))
	}
	blockSize, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return internal.FilesystemSpace{}, fmt.Errorf("invalid block size %q: %w", fields[0], err)
	}
	totalBlocks, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return internal.FilesystemSpace{}, fmt.Errorf("invalid total block count %q: %w", fields[1], err)
	}
	availBlocks, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return internal.FilesystemSpace{}, fmt.Errorf("invalid available block count %q: %w", fields[2], err)
	}
	if blockSize <= 0 || totalBlocks <= 0 {
		return internal.FilesystemSpace{}, fmt.Errorf("non-positive block size %d or total block count %d", blockSize, totalBlocks)
	}
	if availBlocks < 0 {
		return internal.FilesystemSpace{}, fmt.Errorf("negative available block count %d", availBlocks)
	}
	return internal.FilesystemSpace{
		AvailableBytes: blockSize * availBlocks,
		TotalBytes:     blockSize * totalBlocks,
	}, nil
}

// lvmAdvisoryLine matches a line lvm prints on STDOUT instead of stderr — one of
// its log_print/log_warn advisories, e.g.
//
//	Consider pruning vg-1 VG archive with more then 1032 MiB in 11272 files (check archiving is needed in lvm.conf).
//
// Under --reportformat json these land wherever lvm happened to emit them, which
// is in the MIDDLE of the report and not only in front of it, so skipping a
// prefix is not enough — the line has to be dropped wherever it appears.
//
// Keying on "starts with a letter" is safe for an lvm report and only for an lvm
// report: every line of one begins with a structural character or a quoted key,
// never with a bare word.
//
// (?m) is what lets the same pattern answer both questions asked of it: "does
// this buffer contain such a line at all" (the cheap pre-check over the whole
// report) and "is this line one" (per line, after the scanner has stripped the
// newline). Without it `^` would only ever match at byte zero and the pre-check
// would miss every advisory that is not the very first thing lvm printed — which
// is all of them, since the report opens with `{`. The character class is spelled
// out rather than [[:space:]] so that it cannot span a line break.
var lvmAdvisoryLine = regexp.MustCompile(`(?m)^[ \t]*[A-Za-z]`)

// maxReportLine caps one line of an lvm JSON report. lvm puts a whole object on
// a single line, so the longest line grows with the number of columns, not with
// the number of Volume Groups or Logical Volumes.
const maxReportLine = 1 << 20

// reportJSON returns out without the advisory lines lvm mixes into its JSON
// report on stdout.
//
// One such line is enough to take a node out of service: unmarshalVGs fails,
// scanner.fillTheCache returns an error on every pass, and neither the
// BlockDevice nor the LVMVolumeGroup discoverer runs on that node again — while
// the report itself is complete and correct both before and after the advisory.
// That is what happened on four nodes of a 20-node cluster once /etc/lvm/archive
// had grown past lvm's pruning threshold.
func reportJSON(out []byte) []byte {
	if !lvmAdvisoryLine.Match(out) {
		return out
	}

	var kept bytes.Buffer
	kept.Grow(len(out))
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 0, 64*1024), maxReportLine)
	for scanner.Scan() {
		line := scanner.Bytes()
		if lvmAdvisoryLine.Match(line) {
			golog.Printf("WARNING: [reportJSON] dropping a non-JSON line lvm printed on stdout inside its report. Line: '%s'.", bytes.TrimSpace(line))
			continue
		}
		kept.Write(line)
		kept.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		// The filtered copy is truncated and would fail to parse for a second,
		// unrelated reason. Hand back the raw bytes so the error the caller reports
		// describes what lvm actually printed.
		golog.Printf("WARNING: [reportJSON] unable to scan the report, using it verbatim: %v.", err)
		return out
	}

	return kept.Bytes()
}

// reportParseError explains a failed report parse in terms of what lvm printed.
//
// Without the head of stdout the log says only `invalid character 'C' looking
// for beginning of value` and drops the buffer that named byte came from, so the
// only way to learn what lvm said is to reproduce the command on the node by
// hand — which is not something the error's reader can do a day later.
func reportParseError(err error, raw []byte) error {
	const headLen = 240

	head := bytes.TrimSpace(raw)
	truncated := ""
	if len(head) > headLen {
		head, truncated = head[:headLen], "..."
	}

	return fmt.Errorf("%w; lvm printed on stdout: %q%s", err, head, truncated)
}

// theOnlyVG, theOnlyLV and theOnlyPV turn a targeted lvm report into the one row
// the caller asked for, or into an error saying why there is no such row.
//
// Separated from the commands so the three decisions can be tested — the commands
// themselves shell out through nsenter and cannot be — and because they encode the
// same rule from three angles: an object's NAME is not its identity.
//
// Neither an empty nor a multi-row report may be indexed into. Zero rows used to be
// an index-out-of-range panic, and lvm exits 0 with an empty report often enough
// (the object went away between two commands, or devices/global_filter hid it) that
// the agent must not die on it. More than one row means the name the caller used
// does not identify one object, and picking a row hands them somebody else's
// storage under their own name.
func theOnlyVG(rows []internal.VGData, vgName string) (internal.VGData, error) {
	// Two Volume Groups may share a name — that is what lvm's "VG name %s is used by
	// VGs %s and %s" warning is about, and on a hypervisor it takes nothing more than
	// a guest creating a `vg-1` of its own inside a disk this node can see. Neither
	// answer is this function's to pick, so the caller is told to resolve it by UUID.
	switch len(rows) {
	case 1:
		return rows[0], nil
	case 0:
		return internal.VGData{}, fmt.Errorf("unable to GetVG %s: lvm reported no such VG", vgName)
	default:
		uuids := make([]string, 0, len(rows))
		for _, vg := range rows {
			uuids = append(uuids, vg.VGUUID)
		}
		return internal.VGData{}, fmt.Errorf("unable to GetVG %s: the name is used by %d VGs (UUIDs: %s), so it does not identify one", vgName, len(rows), strings.Join(uuids, ", "))
	}
}

func theOnlyLV(rows []internal.LVData, lvPath, vgName string) (internal.LVData, error) {
	// Here the path itself is ambiguous: /dev/<vg>/<lv> names a Volume Group by name,
	// so a duplicate VG name makes lvm report an LV per candidate. Every caller treats
	// an error as "warn and retry", which is the right outcome — picking a row would
	// report a foreign LV's size as our volume's.
	switch len(rows) {
	case 1:
		return rows[0], nil
	case 0:
		return internal.LVData{}, fmt.Errorf("unable to GetLV %s: lvm reported no such LV", lvPath)
	default:
		vgUUIDs := make([]string, 0, len(rows))
		for _, lv := range rows {
			vgUUIDs = append(vgUUIDs, lv.VGUuid)
		}
		return internal.LVData{}, fmt.Errorf("unable to GetLV %s: the path matches %d LVs across VGs sharing the name %s (VG UUIDs: %s)", lvPath, len(rows), vgName, strings.Join(vgUUIDs, ", "))
	}
}

func theOnlyPV(rows []internal.PVData, pvName string) (internal.PVData, error) {
	// A device path does identify one PV, so the multi-row case is not a thing here —
	// but the empty one is: this is the only listing that runs against a device the
	// agent may have just detached.
	if len(rows) == 0 {
		return internal.PVData{}, fmt.Errorf("unable to GetPV %s: lvm reported no such PV", pvName)
	}
	return rows[0], nil
}

func unmarshalPVs(out []byte) ([]internal.PVData, error) {
	var pvR internal.PVReport

	if err := json.Unmarshal(reportJSON(out), &pvR); err != nil {
		return nil, reportParseError(err, out)
	}

	pvs := make([]internal.PVData, 0, len(pvR.Report))
	for _, rep := range pvR.Report {
		pvs = append(pvs, rep.PV...)
	}

	return pvs, nil
}

func unmarshalVGs(out []byte) ([]internal.VGData, error) {
	var vgR internal.VGReport

	if err := json.Unmarshal(reportJSON(out), &vgR); err != nil {
		return nil, reportParseError(err, out)
	}

	vgs := make([]internal.VGData, 0, len(vgR.Report))
	for _, rep := range vgR.Report {
		vgs = append(vgs, rep.VG...)
	}

	return vgs, nil
}

func unmarshalLVs(out []byte) ([]internal.LVData, error) {
	var lvR internal.LVReport

	if err := json.Unmarshal(reportJSON(out), &lvR); err != nil {
		return nil, reportParseError(err, out)
	}

	lvs := make([]internal.LVData, 0, len(lvR.Report))
	for _, rep := range lvR.Report {
		lvs = append(lvs, rep.LV...)
	}

	return lvs, nil
}

func nsentrerExpendedArgs(cmd string, args ...string) []string {
	nsenterArgs := []string{"-t", "1", "-m", "-u", "-i", "-n", "-p"}
	cmdArgs := []string{"--", cmd}
	nsenterArgs = append(nsenterArgs, cmdArgs...)
	return append(nsenterArgs, args...)
}

// lvmStaticExtendedArgs builds the argv passed to nsenter+lvm.static for
// every LVM subcommand the agent runs.
//
// In addition to plumbing args through nsentrerExpendedArgs, it injects a
// --config override immediately after the LVM subcommand name (`vgs`,
// `pvs`, `lvs`, `vgchange`, ...). The override does two things:
//
//   - rejects foreign-storage canonical paths from LVM's device scan
//     (devices/global_filter, see internal.LVMGlobalFilter);
//   - caps the size of /etc/lvm/archive on new metadata operations
//     (backup/retain_min, backup/retain_days; see
//     internal.LVMArchiveRetention).
//
// The --config flag must come AFTER the subcommand in lvm.static >=
// 2.03.41; placing it before the subcommand makes lvm refuse to parse
// the command line with "Specify options after a command".
//
// If args is empty (e.g. `lvm.static version`) the override is skipped
// to keep the no-arg form working.
func lvmStaticExtendedArgs(args []string) []string {
	if len(args) == 0 {
		return nsentrerExpendedArgs(internal.LVMCmd, args...)
	}

	configValue := LVMGlobalFilterForOwnedLoops() + " " + internal.LVMArchiveRetention
	withConfig := make([]string, 0, len(args)+2)
	withConfig = append(withConfig, args[0], "--config", configValue)
	withConfig = append(withConfig, args[1:]...)

	return nsentrerExpendedArgs(internal.LVMCmd, withConfig...)
}

// lvmStaticLockdArgs is lvmStaticExtendedArgs plus the two settings a command
// against a shared Volume Group needs from the client side.
//
// use_lvmlockd is not a property of the node here but of the command: the agent
// runs against both local and shared groups with the same binary, and turning
// the setting on globally would make every local command talk to a daemon that
// has nothing to say about it.
//
// host_id is passed only when it is known and non-zero. It is the client's own
// copy of what lvmlockd reads from its host-id file, and lvm2 >= 2.03.27 checks
// it against the ceiling implied by the lease alignment.
func lvmStaticLockdArgs(args []string, hostID int) ([]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no command given")
	}

	configValue := LVMGlobalFilterForOwnedLoops() + " " + internal.SharedLVMNoArchive + " global/use_lvmlockd=1"
	if hostID > 0 {
		configValue += " local/host_id=" + strconv.Itoa(hostID)
	}

	withConfig := make([]string, 0, len(args)+2)
	withConfig = append(withConfig, args[0], "--config", configValue)
	withConfig = append(withConfig, args[1:]...)

	return sharedLVMArgs(withConfig...)
}

// sharedLVMArgs builds the argv for a command against a shared Volume Group.
//
// It enters the mount namespace of the lock daemons rather than the host's, and
// runs their lvm rather than this module's. The host's namespace has no lvm that
// can speak to lvmlockd — this module's is compiled without the support and a
// node carries none of its own — so the same command that works for a local
// group answers "Using a shared lock type requires lvmlockd" for a shared one.
//
// Only the mount namespace is entered. The daemons run with the node's pid and
// network namespaces already, so asking for those again would be a no-op with
// two more ways to fail.
func sharedLVMArgs(args ...string) ([]string, error) {
	pid, err := SharedLVMNamespacePID()
	if err != nil {
		return nil, err
	}

	argv := []string{"-t", strconv.Itoa(pid), "-m", "--", internal.SharedLVMCmd}
	return append(argv, args...), nil
}

// The benign-stderr allowlists. A line matched here is one lvm.static prints
// that says nothing about whether the operation succeeded, so it must not make
// the caller treat a non-zero exit as a failure (see errIfNotBenign).
//
// They are deliberately SPLIT PER COMMAND rather than kept as one global set.
// What counts as "says nothing about the outcome" is a property of the
// subcommand, not of lvm: a resize that changed nothing is a normal state for
// `lvextend -l 100%VG`, and is not a thing `pvs` or `pvcreate` can report at
// all. Keeping one set meant that adding a benign pattern for a write command
// silently widened what counts as success for the LISTING commands too — and
// the PV listing is the sole gate in front of every destructive file-device
// decision (cleanupFileDevices unlinks backing files on the strength of it,
// rollbackProvisionedFileDevices tears loops down). A `pvs` that exits non-zero
// after emitting a partial report must stay a failure, whatever patterns a
// future resize needs.
var (
	// Artefacts of running a statically linked lvm.static under nsenter. They
	// appear on any subcommand, read or write, and never carry information about
	// the outcome: the regex version mismatch comes from the SELinux check inside
	// the static binary, and the leaked descriptor is reported after the operation
	// has already been applied.
	reRegexVersionMismatch = regexp.MustCompile(`Regex version mismatch, expected: .+ actual: .+`)
	reLeakedFileDescriptor = regexp.MustCompile(`File descriptor .+ leaked on lvm(\.static)? invocation\. Parent PID .+: /opt/deckhouse/sds/bin/nsenter`)

	// lvm says this when /dev/<vg> is already a directory — a leftover from a
	// group of the same name that existed on this node before. It is a remark
	// about the device node, printed after the Volume Group has been created,
	// and reading it as a failure is expensive: the group is on the LUN, the
	// caller believes it is not, and the pool it belongs to stays Pending while
	// everything it needs already exists. Measured on the stand with a pool
	// named after a group an earlier experiment had left behind.
	reDevDirExists = regexp.MustCompile(`/dev/[^:]+: already exists in filesystem`)

	// rePVAlreadyInVG is lvm refusing to add a physical volume that is in the
	// group already. For an extension that is success spelled as an error.
	rePVAlreadyInVG = regexp.MustCompile(`is already in volume group`)

	// A resize that changed nothing. This is the normal state of a thin pool sized
	// as a percentage of the VG: the pool always already fills it, up to thin-pool
	// metadata, so the controller re-requests a size the LV already has on every
	// reconcile. Different LVM versions word it differently — older ones print
	// "No size change.", newer ones (2.03.x) "New size (<n> extents) matches
	// existing size (<n> extents)." — and both mean the same no-op.
	//
	// Anchored, unlike the two above: this one is specific enough that a line
	// merely quoting it (an error message embedding another command's output)
	// should not be swallowed.
	reNoSizeChange = regexp.MustCompile(`^\s*(No size change\..*|New size \(.+\) matches existing size \(.+\)\.)$`)

	// Two Volume Groups on the node share a name. lvm prints this pair on EVERY
	// invocation, whatever object the command asked about, because it is a property
	// of the node's device scan and not of the argument: `vgs vg-1`, `pvs
	// /dev/nvme0n1` and `lvs /dev/vg-1/pvc-...` all print it.
	//
	// That makes it the one thing a per-object diagnostic must never keep. The
	// discoverer asks lvm about one Volume Group at a time and records whatever
	// stderr comes back as that Volume Group's health; with these lines in it, a
	// `data` inside some guest's disk colliding with another guest's `data` marked
	// this node's own vg-1 NonOperational and took its LVMVolumeGroup to NotReady.
	// The condition it deserves is set elsewhere, by name, with the UUIDs of the
	// actual duplicates in the message (see the discoverer's duplicate handling).
	reDuplicateVGName     = regexp.MustCompile(`^\s*WARNING: VG name .+ is used by VGs .+\.$`)
	reDuplicateVGNameHint = regexp.MustCompile(`^\s*Fix duplicate VG names with vgrename uuid, a device filter, or system IDs\.$`)

	// The node has lvmlockd configured in some way lvm wants to comment on. Like
	// the duplicate-name pair above, these lines describe the node's locking
	// setup, not the object the command asked about: `use_lvmlockd = 1` in
	// lvm.conf makes EVERY invocation print them, whatever its argument, and a
	// Local Volume Group belonging to another module gets them just as much as a
	// shared one.
	//
	// Keeping them would mean that switching the flag on turns every
	// LVMVolumeGroup on the node NonOperational at once, because the discoverer
	// records non-empty stderr as the object's health regardless of exit code.
	// The state they describe belongs in a node-level condition, set by whoever
	// owns the locking daemons.
	//
	// Three classes, all observed:
	//   - the flag is on and the daemon is not running (lvm2 2.03.16 and 2.03.42);
	//   - the lock was skipped for this command (global or per-VG);
	//   - the Volume Group was read without a lock, which lvm reports by name.
	reLockdNotRunning = regexp.MustCompile(`^\s*WARNING: lvmlockd process is not running\.$`)
	reLockdNotUsed    = regexp.MustCompile(`^\s*(WARNING: )?lvmlockd is not being used on the host\.$`)
	reLockSkipped     = regexp.MustCompile(`^\s*(WARNING: )?(Reading without shared global lock\.|Skipping global lock: .+|WARNING: skipping VG lock in lvmlockd\.|Skipping volume group .+|VG .+ lock skipped: .+)$`)
	reReadingNoLock   = regexp.MustCompile(`^\s*Reading VG .+ without a lock\.$`)
	reLockspaceStart  = regexp.MustCompile(`^\s*VG .+ (starting|stopping) .+ lockspace$`)

	// benignAlwaysStdErr is the set every lvm invocation may ignore.
	benignAlwaysStdErr = []*regexp.Regexp{reRegexVersionMismatch, reLeakedFileDescriptor}
	// benignCreateStdErr additionally tolerates the leftover device-node
	// directory. Only vgcreate may use it: elsewhere that line would be about a
	// directory nobody asked this command to make.
	benignCreateStdErr = []*regexp.Regexp{reRegexVersionMismatch, reLeakedFileDescriptor, reDevDirExists}
	// reNoSuchDMDevice is how device-mapper says the mapping a removal was aimed
	// at is already gone. For a removal that is success spelled as an error.
	reNoSuchDMDevice = regexp.MustCompile(`(?i)(No such device or address|Device does not exist)`)
	// benignResizeStdErr additionally tolerates the no-op resize. Only lvextend
	// and its full-VG-space variant may use it.
	benignResizeStdErr = []*regexp.Regexp{reRegexVersionMismatch, reLeakedFileDescriptor, reNoSizeChange}
	// notAboutTheQueriedObject is what ObjectDiagnostics drops. It is NOT a
	// benign-stderr set: these lines do report a real problem with the node, they
	// just do not report one with the object that was asked about, and only the
	// former is a reason to keep them.
	notAboutTheQueriedObject = []*regexp.Regexp{
		reDuplicateVGName, reDuplicateVGNameHint,
		reLockdNotRunning, reLockdNotUsed, reLockSkipped, reReadingNoLock, reLockspaceStart,
	}
)

// ObjectDiagnostics returns the part of stdErr that says something about the
// specific VG, PV or LV the command asked about.
//
// Use it wherever lvm's stderr is about to be attributed to one object —
// recorded as its health, written into its condition, shown next to its name.
// Everything lvm prints about the node as a whole belongs in the log and in the
// conditions that describe the node, not pinned on whichever object happened to
// be the argument.
func ObjectDiagnostics(command string, stdErr bytes.Buffer) bytes.Buffer {
	return filterStdErr(command, stdErr, notAboutTheQueriedObject)
}

// filterStdErr returns the lines of stdErr that actually say something about the
// outcome of command, dropping the ones matched by allow.
//
// An empty result means "everything lvm printed was benign", which is what lets
// errIfNotBenign turn a non-zero exit into a success. A non-empty one is a real
// diagnostic and is reported verbatim.
//
// allow must be one of benignAlwaysStdErr / benignResizeStdErr — see their doc
// comment for why the choice belongs to the call site.
func filterStdErr(command string, stdErr bytes.Buffer, allow []*regexp.Regexp) bytes.Buffer {
	var filteredStdErr bytes.Buffer
	stdErrScanner := bufio.NewScanner(&stdErr)

	for stdErrScanner.Scan() {
		line := stdErrScanner.Text()
		benign := false
		for _, re := range allow {
			if re.MatchString(line) {
				benign = true
				break
			}
		}
		if benign {
			golog.Printf("WARNING: [filterStdErr] Line filtered from stderr due to matching exclusion pattern. Line: '%s'. Triggered by command: '%s'.", line, command)
		} else {
			filteredStdErr.WriteString(line + "\n")
		}
	}

	return filteredStdErr
}
