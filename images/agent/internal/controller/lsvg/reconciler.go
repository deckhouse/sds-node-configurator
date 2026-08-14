/*
Copyright 2025 Flant JSC

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

// Package lsvg keeps this node's membership in the sanlock lockspaces of the
// shared Volume Groups it belongs to.
//
// It does two things and deliberately not a third. It hands lvmlockd the
// host_id the controller allocated, and it starts or stops the lockspace as the
// node enters or leaves the group. It does not activate anything: which volumes
// live here is decided by the attachment resources, and a reconciler that
// activated volumes on lockspace start would activate the whole group on every
// node of it.
package lsvg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const ReconcilerName = "lvm-shared-volume-group-controller"

const (
	// SanlockHostIDAnnotation is written by the controller and read here. The
	// agent never picks an id of its own: two nodes on one id renew the same
	// delta lease and each concludes the other is dead.
	SanlockHostIDAnnotation = "storage.deckhouse.io/sanlock-host-id"

	// LockspaceStartedAnnotationPrefix, plus the group name, marks that this
	// node holds that group's lockspace. It is a fact about the node read by
	// another module — which is why it lives on the Node and not in a status
	// this module owns — and it gates two things: whether the node may be given
	// attachments, and whether its LUN may be taken away.
	LockspaceStartedAnnotationPrefix = "storage.deckhouse.io/lockspace-started-"

	// hostIDFileName is read by lvmlockd through --host-id-file on every
	// lockspace start, so writing it is enough and no daemon needs restarting.
	hostIDFileName = "host-id"

	// leaseAreaLVName is the hidden volume lvm creates for sanlock's leases. It
	// is counted apart from the volumes of the pool: it is not one of them, and a
	// reader who saw it in the count would think a pool with no volumes had one.
	leaseAreaLVName = "lvmlock"

	// phaseCreated is the group's state once it exists and can serve volumes.
	phaseCreated = "Created"
)

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	sdsCache *cache.Cache
	commands utils.Commands
	cfg      ReconcilerConfig
}

type ReconcilerConfig struct {
	NodeName string
	// HostIDDir is shared with the lock daemons through a hostPath, which is
	// the only channel between them: the daemons have no API access, by design.
	HostIDDir string
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	sdsCache *cache.Cache,
	commands utils.Commands,
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{cl: cl, log: log, sdsCache: sdsCache, commands: commands, cfg: cfg}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

// MaxConcurrentReconciles is 1 on purpose. Every lockspace of a node shares one
// host_id file and one lvmlockd, and lock-start blocks for as long as sanlock
// needs — up to 14 x io_timeout + 60 when reclaiming an id whose lease is still
// alive. Running two of those at once buys nothing and makes the ordering of
// the file write against the command unpredictable.
func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

func (r *Reconciler) ShouldReconcileCreate(_ *v1alpha1.LVMSharedVolumeGroup) bool {
	return true
}

func (r *Reconciler) ShouldReconcileUpdate(objectOld, objectNew *v1alpha1.LVMSharedVolumeGroup) bool {
	return r.isMember(objectOld) != r.isMember(objectNew) ||
		objectOld.Spec.ActualVGNameOnTheNode != objectNew.Spec.ActualVGNameOnTheNode
}

func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMSharedVolumeGroup],
) (controller.Result, error) {
	lsvg := req.Object

	if !r.isMember(lsvg) {
		return r.leave(ctx, lsvg)
	}

	return r.join(ctx, lsvg)
}

func (r *Reconciler) isMember(lsvg *v1alpha1.LVMSharedVolumeGroup) bool {
	return slices.Contains(lsvg.Spec.Nodes, r.cfg.NodeName)
}

// join makes this node a participant of the group's lockspace.
func (r *Reconciler) join(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	node, err := r.node(ctx)
	if err != nil {
		return controller.Result{}, err
	}

	hostID, err := hostIDOf(node)
	if err != nil {
		return controller.Result{}, err
	}
	if hostID == 0 {
		// The allocator has not got here yet. This is the ordinary state right
		// after a node is added to a pool, not a failure.
		r.log.Info(fmt.Sprintf("[%s] node %s has no sanlock host id yet, waiting for the allocator", ReconcilerName, r.cfg.NodeName))
		return controller.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Before anything else, including vgcreate: creating a shared group starts
	// its lockspace, and lvmlockd reads the id from this file when it does.
	if err := r.writeHostIDFile(hostID); err != nil {
		return controller.Result{}, err
	}

	if lsvg.Spec.MetadataOwner == r.cfg.NodeName {
		created, res, err := r.ensureGroup(ctx, lsvg, hostID)
		if err != nil || created || res.RequeueAfter > 0 {
			// A group that was just created already holds its lockspace here —
			// vgcreate --shared starts it — so the readiness fact is published
			// and the reconcile ends.
			if created {
				if err := r.setLockspaceStarted(ctx, lsvg.Name, true); err != nil {
					return controller.Result{}, err
				}
				return r.publishGroup(ctx, lsvg)
			}
			return res, err
		}
	}

	if r.lockspaceStarted(node, lsvg.Name) {
		return r.publishGroup(ctx, lsvg)
	}

	r.log.Info(fmt.Sprintf("[%s] starting the lockspace of %s with host id %d", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, hostID))
	cmd, err := r.commands.VGLockStart(ctx, lsvg.Spec.ActualVGNameOnTheNode, hostID)
	if err != nil {
		// A LUN that is not visible yet, a lease still held by this node's
		// previous incarnation, a pool being created — all of them look like
		// this and all of them pass. Retry rather than escalate.
		r.log.Warning(fmt.Sprintf("[%s] unable to start the lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if err := r.setLockspaceStarted(ctx, lsvg.Name, true); err != nil {
		return controller.Result{}, err
	}

	r.log.Info(fmt.Sprintf("[%s] the lockspace of %s is started", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	return r.publishGroup(ctx, lsvg)
}

// publishGroup states what this node observes about the group, and only the
// metadata owner does it.
//
// One writer, because a status written by every member would be a status whose
// last writer wins and whose readers cannot tell which node's view they got.
// The owner is the node that creates the group, so it is the one node whose
// answer to "is it there" is not a guess.
//
// Until something says the group exists, every reader downstream — the pool
// above all — has only the existence of this object to go on, and an object is
// not a volume group. That was exactly the defect this closes: a pool reported
// itself ready while nothing had been created on the LUN.
func (r *Reconciler) publishGroup(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	if lsvg.Spec.MetadataOwner != r.cfg.NodeName {
		return controller.Result{}, nil
	}

	// Asked, not remembered. The group is read from lvm directly rather than from
	// the scan cache: the cache is refreshed by a scanner that has no schedule of
	// its own, so a group created between two scans could stay unpublished
	// indefinitely — and the whole point of this is to stop a reader upstream
	// from believing something nobody has checked.
	vg, cmd, _, err := r.commands.GetVG(lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil || vg.VGUUID == "" {
		// Ordinary right after vgcreate on a busy node, and equally ordinary while
		// the LUN is still settling. Neither is worth an error on the object.
		r.log.Info(fmt.Sprintf("[%s] %s cannot be read yet (cmd: %s), will publish it later",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd))
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	status := &v1alpha1.LVMSharedVolumeGroupStatus{
		Phase:              phaseCreated,
		ObservedGeneration: lsvg.Generation,
		VGUUID:             vg.VGUUID,
		VGSize:             vg.VGSize.String(),
		VGFree:             vg.VGFree.String(),
		ExtentSize:         vg.VGExtentSize.String(),
	}

	lvs, _ := r.sdsCache.GetLVs()
	for i := range lvs {
		if lvs[i].VGName != lsvg.Spec.ActualVGNameOnTheNode {
			continue
		}
		if lvs[i].LVName == leaseAreaLVName {
			status.LeaseAreaSize = lvs[i].LVSize.String()
			continue
		}
		status.LogicalVolumeCount++
	}

	if lsvg.Status != nil &&
		lsvg.Status.Phase == status.Phase &&
		lsvg.Status.VGUUID == status.VGUUID &&
		lsvg.Status.VGSize == status.VGSize &&
		lsvg.Status.VGFree == status.VGFree &&
		lsvg.Status.ExtentSize == status.ExtentSize &&
		lsvg.Status.LeaseAreaSize == status.LeaseAreaSize &&
		lsvg.Status.LogicalVolumeCount == status.LogicalVolumeCount &&
		lsvg.Status.ObservedGeneration == status.ObservedGeneration {
		// Nothing changed. Writing anyway would wake every watcher of this object
		// on a timer for no reason.
		return controller.Result{}, nil
	}

	patch := client.MergeFrom(lsvg.DeepCopy())
	if lsvg.Status != nil {
		status.Conditions = lsvg.Status.Conditions
	}
	lsvg.Status = status
	if err := r.cl.Status().Patch(ctx, lsvg, patch); err != nil {
		return controller.Result{}, fmt.Errorf("publish the status of %s: %w", lsvg.Name, err)
	}

	return controller.Result{}, nil
}

// ensureGroup creates the Volume Group if it is not there yet. Only the
// metadata owner runs it: LVM metadata has one writer by construction, and two
// nodes racing to create the same group on the same LUN is not a race that ends
// well.
//
// Absence is proved by reading the labels of the physical volumes rather than
// by asking vgs. Under lvmlockd a group whose lockspace this node has not
// started is skipped and looks exactly like a group that does not exist, so
// "vgs found nothing" is not evidence of anything — and acting on it would mean
// creating a second group over an existing one.
func (r *Reconciler) ensureGroup(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	hostID int,
) (created bool, res controller.Result, err error) {
	wwids := make([]string, 0, len(lsvg.Spec.Devices))
	for _, device := range lsvg.Spec.Devices {
		wwids = append(wwids, device.WWID)
	}

	devices, missing, err := utils.ResolveWWIDs(wwids)
	if err != nil {
		return false, controller.Result{}, fmt.Errorf("resolve the devices of %s: %w", lsvg.Name, err)
	}
	if len(missing) > 0 {
		// The array has not presented these LUNs to this node yet, or multipath
		// has not assembled them. Both resolve themselves.
		r.log.Info(fmt.Sprintf("[%s] %s is waiting for %d device(s): %s",
			ReconcilerName, lsvg.Name, len(missing), strings.Join(missing, ", ")))
		return false, controller.Result{RequeueAfter: 20 * time.Second}, nil
	}

	exists, foreign := r.groupState(devices, lsvg.Spec.ActualVGNameOnTheNode)
	if foreign != "" {
		// Someone else's data. Creating over it would destroy it, and adopting it
		// would put this module in charge of a group whose extent size, lease area
		// and membership were decided elsewhere — so neither is done, and the LUN
		// is left exactly as it was found.
		return false, controller.Result{}, fmt.Errorf(
			"%s cannot be created: the devices already carry the volume group %q, and this pool asks for %q",
			lsvg.Name, foreign, lsvg.Spec.ActualVGNameOnTheNode)
	}
	if exists {
		return false, controller.Result{}, nil
	}

	ordered := make([]utils.SharedDevice, 0, len(devices))
	for _, wwid := range wwids {
		ordered = append(ordered, devices[wwid])
	}
	if err := utils.CheckSharedDeviceInvariants(ordered, extentSizeBytes(lsvg)); err != nil {
		// Neither invariant can be repaired after the group exists, so this is a
		// hard stop rather than a retry.
		return false, controller.Result{}, fmt.Errorf("%s cannot be created: %w", lsvg.Name, err)
	}

	params := utils.SharedVGParams{
		VGName:                lsvg.Spec.ActualVGNameOnTheNode,
		SharedVolumeGroupName: lsvg.Name,
		PVPaths:               utils.SortedPaths(devices),
		HostID:                hostID,
		SanlockAlignSizeMiB:   alignSizeMiB(lsvg),
	}
	if lsvg.Spec.LVM != nil {
		params.PhysicalExtentSize = lsvg.Spec.LVM.PhysicalExtentSize
		params.MetadataSize = lsvg.Spec.LVM.MetadataSize
	}

	r.log.Info(fmt.Sprintf("[%s] creating the shared volume group %s on %s",
		ReconcilerName, params.VGName, strings.Join(params.PVPaths, ", ")))
	cmd, err := r.commands.CreateVGShared(ctx, params)
	if err != nil {
		return false, controller.Result{}, fmt.Errorf("create the shared volume group %s (cmd: %s): %w",
			params.VGName, cmd, err)
	}

	r.log.Info(fmt.Sprintf("[%s] the shared volume group %s is created", ReconcilerName, params.VGName))
	return true, controller.Result{}, nil
}

// groupState reads the physical volume labels this node has already scanned and
// says whether the wanted group is there — and whether something else is.
//
// The name has to be compared, not just its presence. A label naming any volume
// group used to count as proof that this group existed, which is only true while
// the LUN has never been used for anything else. On a LUN that carries an
// unrelated group the agent would skip creation, report the group ready, and
// then work against a name that is not on the device — or, worse, hand out
// volumes from a group that belongs to someone else.
func (r *Reconciler) groupState(
	devices map[string]utils.SharedDevice,
	wantVGName string,
) (exists bool, foreign string) {
	pvs, _ := r.sdsCache.GetPVs()
	for _, pv := range pvs {
		for _, device := range devices {
			if pv.PVName != device.Path || pv.VGName == "" {
				continue
			}
			if pv.VGName == wantVGName {
				exists = true
				continue
			}
			// Reported rather than returned immediately: the first foreign group
			// found is the one named, and the loop stays deterministic.
			if foreign == "" {
				foreign = pv.VGName
			}
		}
	}
	return exists, foreign
}

// extentSizeBytes parses the requested extent size for the granularity check.
// An unparseable value simply skips the check rather than blocking creation:
// lvm validates the string itself and says so better than this could.
func extentSizeBytes(lsvg *v1alpha1.LVMSharedVolumeGroup) int {
	if lsvg.Spec.LVM == nil || lsvg.Spec.LVM.PhysicalExtentSize == "" {
		return 0
	}
	quantity, err := resource.ParseQuantity(lsvg.Spec.LVM.PhysicalExtentSize)
	if err != nil {
		return 0
	}
	return int(quantity.Value())
}

// alignSizeMiB turns the lease alignment into the integer lvm expects. It is
// the same number the host_id allocator derives its ceiling from, and the two
// must not disagree.
func alignSizeMiB(lsvg *v1alpha1.LVMSharedVolumeGroup) int {
	if lsvg.Spec.LVM == nil || lsvg.Spec.LVM.SanlockAlignSize == "" {
		return 0
	}
	quantity, err := resource.ParseQuantity(lsvg.Spec.LVM.SanlockAlignSize)
	if err != nil {
		return 0
	}
	return int(quantity.Value() / (1024 * 1024))
}

// leave takes this node out of the group's lockspace, in the one order that is
// safe: the readiness fact goes first so that nothing new is scheduled here,
// then the lockspace stops — and only if this node holds no active volume of
// the group. Stopping it under an active volume would leave the volume
// writable with no lock behind it.
func (r *Reconciler) leave(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
) (controller.Result, error) {
	node, err := r.node(ctx)
	if err != nil {
		return controller.Result{}, err
	}

	if !r.lockspaceStarted(node, lsvg.Name) {
		return controller.Result{}, nil
	}

	if active := r.activeLVs(lsvg.Spec.ActualVGNameOnTheNode); len(active) > 0 {
		// Not an error: the attachment reconciler is deactivating them, or the
		// controller removed the node while volumes were still in use. Say what
		// is holding the node and come back.
		r.log.Warning(fmt.Sprintf("[%s] cannot stop the lockspace of %s: %d volume(s) still active here (%s)",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(active), strings.Join(active, ", ")))
		return controller.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.setLockspaceStarted(ctx, lsvg.Name, false); err != nil {
		return controller.Result{}, err
	}

	cmd, err := r.commands.VGLockStop(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to stop the lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	r.log.Info(fmt.Sprintf("[%s] the lockspace of %s is stopped", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	return controller.Result{}, nil
}

// activeLVs names the volumes of the group that are active on this node.
func (r *Reconciler) activeLVs(vgName string) []string {
	active := make([]string, 0)
	lvs, _ := r.sdsCache.GetLVs()
	for _, lv := range lvs {
		if lv.VGName != vgName {
			continue
		}
		// The fifth attribute character is 'a' for an active volume. Reading it
		// rather than asking lvm again keeps this off the shared VG lock, which
		// is the pool's scarcest resource.
		if len(lv.LVAttr) > 4 && lv.LVAttr[4] == 'a' {
			active = append(active, lv.LVName)
		}
	}
	return active
}

func (r *Reconciler) node(ctx context.Context) (*corev1.Node, error) {
	node := &corev1.Node{}
	if err := r.cl.Get(ctx, client.ObjectKey{Name: r.cfg.NodeName}, node); err != nil {
		return nil, fmt.Errorf("get own Node %s: %w", r.cfg.NodeName, err)
	}
	return node, nil
}

func (r *Reconciler) lockspaceStarted(node *corev1.Node, groupName string) bool {
	return node.Annotations[LockspaceStartedAnnotationPrefix+groupName] == "true"
}

func (r *Reconciler) setLockspaceStarted(ctx context.Context, groupName string, started bool) error {
	node, err := r.node(ctx)
	if err != nil {
		return err
	}

	key := LockspaceStartedAnnotationPrefix + groupName
	patch := client.MergeFrom(node.DeepCopy())
	if started {
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		node.Annotations[key] = "true"
	} else {
		if _, ok := node.Annotations[key]; !ok {
			return nil
		}
		delete(node.Annotations, key)
	}

	if err := r.cl.Patch(ctx, node, patch); err != nil {
		return fmt.Errorf("publish %s=%t on Node %s: %w", key, started, r.cfg.NodeName, err)
	}
	return nil
}

// writeHostIDFile puts the id where lvmlockd reads it. The file is rewritten
// only when its content differs: lvmlockd re-reads it on every lockspace start,
// and rewriting it under a running lockspace would be a change nobody asked for.
func (r *Reconciler) writeHostIDFile(hostID int) error {
	path := filepath.Join(r.cfg.HostIDDir, hostIDFileName)
	want := fmt.Sprintf("host_id = %d\n", hostID)

	if current, err := os.ReadFile(path); err == nil && string(current) == want {
		return nil
	}

	if err := os.MkdirAll(r.cfg.HostIDDir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", r.cfg.HostIDDir, err)
	}
	if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	r.log.Info(fmt.Sprintf("[%s] wrote sanlock host id %d to %s", ReconcilerName, hostID, path))
	return nil
}

// hostIDOf reads the id the controller allocated to this node.
func hostIDOf(node *corev1.Node) (int, error) {
	raw, ok := node.Annotations[SanlockHostIDAnnotation]
	if !ok || raw == "" {
		return 0, nil
	}
	id, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("annotation %s of node %s is %q, not a number", SanlockHostIDAnnotation, node.Name, raw)
	}
	if id < 1 {
		return 0, fmt.Errorf("annotation %s of node %s is %d; sanlock host ids start at 1", SanlockHostIDAnnotation, node.Name, id)
	}
	return id, nil
}
