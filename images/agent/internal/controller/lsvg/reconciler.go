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
	"sort"
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
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
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

	// LockspaceGenerationAnnotationPrefix, plus the group name, counts how many
	// times this node has started that lockspace.
	//
	// It is the answer to a question a device cannot answer: whether the lock
	// behind it is still held. lvmlockd and sanlock restart together and lose
	// every lease; the kernel keeps every mapping. A volume activated under an
	// earlier incarnation is still mapped and no longer locked, so the number is
	// what makes the attachment reconciler activate it again rather than stop at
	// the sight of the device.
	LockspaceGenerationAnnotationPrefix = "storage.deckhouse.io/lockspace-generation-"

	// hostIDFileName is read by lvmlockd through --host-id-file on every
	// lockspace start, so writing it is enough and no daemon needs restarting.
	hostIDFileName = "host-id"

	// leaseAreaLVName is the hidden volume lvm creates for sanlock's leases. It
	// is counted apart from the volumes of the pool: it is not one of them, and a
	// reader who saw it in the count would think a pool with no volumes had one.
	leaseAreaLVName = "lvmlock"

	// phaseCreated is the group's state once it exists and can serve volumes.
	phaseCreated = "Created"

	// unknownVGName is what lvm prints when it cannot name the group a physical
	// volume belongs to. It is an admission, not an answer.
	unknownVGName = "[unknown]"
)

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	sdsCache *cache.Cache
	commands utils.Commands
	metrics  *monitoring.Metrics
	cfg      ReconcilerConfig

	// prVerdicts caches what this node established about the reservation
	// channel of each pool. Nothing it looks at changes without somebody
	// changing it, so asking on every pass would be two commands a minute for
	// an answer that is the same until an image or a drop-in changes.
	prVerdicts map[string]prVerdict

	// prStates is where this node says it stands in a switch to reservations,
	// per pool. It lives here rather than being derived because the steps that
	// set it are the ones that ran the commands, and nothing else can tell
	// "stopped for the switch" from "stopped for any other reason".
	prStates map[string]string
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
	metrics *monitoring.Metrics,
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{
		cl: cl, log: log, sdsCache: sdsCache, commands: commands, metrics: metrics, cfg: cfg,
		prVerdicts: map[string]prVerdict{},
		prStates:   map[string]string{},
	}
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
	// A pool being deleted is the update that matters most, and it is the one
	// this filter used to drop: nothing about membership or the group's name
	// changes when a deletion timestamp appears, so the event was skipped and
	// the unwinding never began. Found on the stand, where a group sat in
	// deletion for hours with its lockspace up and no agent looking at it.
	if (objectOld.DeletionTimestamp == nil) != (objectNew.DeletionTimestamp == nil) {
		return true
	}

	return r.isMember(objectOld) != r.isMember(objectNew) ||
		objectOld.Spec.ActualVGNameOnTheNode != objectNew.Spec.ActualVGNameOnTheNode
}

func (r *Reconciler) Reconcile(
	ctx context.Context,
	req controller.ReconcileRequest[*v1alpha1.LVMSharedVolumeGroup],
) (res controller.Result, err error) {
	lsvg := req.Object

	// Every pass asks for the next one, whatever it decided.
	//
	// The states this reconciler repairs produce no event of their own — a
	// lockspace the lock manager lost, a mapping nobody is asking for, a
	// deletion that arrived while nothing was watching — and the events that do
	// arrive are filtered down to membership and the group's name. A branch that
	// returned an empty result stopped the clock for that group until something
	// unrelated happened to it, which is how a deleted pool waited hours to be
	// unwound.
	defer func() {
		if err == nil && res.RequeueAfter == 0 {
			res.RequeueAfter = groupRecheckInterval
		}
	}()

	// A deleted pool is unwound rather than forgotten: the Volume Group is on a
	// LUN that outlives this object, and so are the lockspaces every member is
	// holding over it.
	if lsvg.DeletionTimestamp != nil {
		return r.teardown(ctx, lsvg)
	}

	if !r.isMember(lsvg) {
		return r.leave(ctx, lsvg)
	}

	if err := r.addFinalizer(ctx, lsvg); err != nil {
		return controller.Result{}, err
	}

	// The eviction ban is checked before anything that touches the array.
	//
	// It is the cheapest question here — an annotation — and it has to be asked
	// first: the switch's own rejoin path registers this node, so an evicted
	// node caught mid-switch would put its key back on the LUNs before the ban
	// below had a chance to stop it.
	if standDown, standingDown := r.standDownIfEvicted(ctx, lsvg); standingDown {
		return standDown, nil
	}

	// A pool being taken over by persistent reservations is not reconciled the
	// ordinary way while that lasts: the members give up their lockspaces on
	// purpose, and the ordinary pass would start them again.
	if res, switching := r.switchToPersistentReservations(ctx, lsvg); switching {
		return res, nil
	}

	// An eviction a person asked for, before the ordinary pass: it takes a
	// node's access away, and everything below assumes the members it can see
	// are the members that may write.
	r.evictRequestedNode(ctx, lsvg)

	res, err = r.join(ctx, lsvg)
	if err != nil {
		// Said out loud and retried on this reconciler's own cadence, rather
		// than handed back as an error.
		//
		// controller-runtime ignores RequeueAfter when an error is returned and
		// falls back to its rate limiter — and on the stand that meant a member
		// whose vgcreate failed once got two passes in fifteen minutes and then
		// none at all. The pool it belonged to sat Pending with its volume group
		// already on the LUN, needing exactly one more pass to notice.
		//
		// The period is the whole design of this reconciler: nothing here
		// produces events, so a pass that stops happening is a repair that stops
		// happening. An error is a reason to look again in a minute, not a reason
		// to stop looking.
		r.log.Error(err, fmt.Sprintf("[%s] %s did not reconcile cleanly, retrying in %s",
			ReconcilerName, lsvg.Name, groupRecheckInterval))
		// And on the object, because a log line on one node of a pool is not
		// where anybody looks. The lockspace fact is carried over rather than
		// overwritten: whether this node is in the pool is a different question
		// from whether its last pass went through, and answering the first with
		// the second would be a lie in the direction that matters.
		r.publishNodeState(ctx, lsvg, r.lockspaceStartedInStatus(lsvg), ReasonReconcileFailed, err.Error())
		return controller.Result{RequeueAfter: groupRecheckInterval}, nil
	}
	if res.RequeueAfter > 0 {
		return res, nil
	}

	// A member of a pool looks at itself again on a schedule, and this is the
	// difference between a reconciler that fixes things and one that happens to
	// have been called at the right moment.
	//
	// Everything this pass repairs — a mapping left by a lock-daemon restart, a
	// node coming back from a barrier, a lockspace nobody counted — is a STATE
	// of the node, and none of it produces an event on the object being watched.
	// The watch fires on membership and on the group's name, which is to say
	// almost never. On the stand a node released its orphan once, finished the
	// pass, and then sat with the mapping still there for hours: nothing was
	// broken, nobody was going to call it again.
	//
	// The cost of the period is a sysfs scan and two reads from a cache the
	// manager already keeps; commands run only when there is something to fix.
	return controller.Result{RequeueAfter: groupRecheckInterval}, nil
}

// groupRecheckInterval is how often a member re-examines its own state. It is
// short enough that residue does not outlive an interesting window and long
// enough that a hundred nodes are not a load on anything.
const groupRecheckInterval = time.Minute

// lockspaceReallyRunning asks the lock manager rather than this module's own
// bookkeeping. An error is not an answer: a node that cannot ask keeps its
// belief, because restarting a lockspace that is running would drop the leases
// under the volumes it holds.
func (r *Reconciler) lockspaceReallyRunning(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) bool {
	running, err := r.commands.LockspaceRunning(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to ask the lock manager about %s: %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, err.Error()))
		return true
	}
	return running
}

// lockspaceStartedInStatus is what this node last said about its own
// participation. Used when reporting something else about the node, so that the
// unrelated fact is carried over instead of being invented.
func (r *Reconciler) lockspaceStartedInStatus(lsvg *v1alpha1.LVMSharedVolumeGroup) bool {
	if lsvg.Status == nil {
		return false
	}
	for _, node := range lsvg.Status.Nodes {
		if node.Name == r.cfg.NodeName {
			return node.LockspaceStarted
		}
	}
	return false
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

	// A node that was fenced comes back on its own. Until the error targets the
	// barrier left are gone, there is nothing to start a lockspace over — and
	// starting one while the paths are still broken only earns another barrier.
	if waiting := r.recoverFromBarrier(ctx, lsvg); waiting {
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// The identity of the group this node is about to hold a lockspace of. Read
	// before anything is decided, because both the check below and the fact
	// published afterwards are about THIS group and not about its name.
	vgUUID := r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode)

	// And on local disk, where the fencing handler will look for it. It runs
	// when the storage is already gone, so it cannot ask lvm: the identity has
	// to be there before it is needed, on every pass that knows it.
	r.rememberVGUUID(lsvg.Spec.ActualVGNameOnTheNode, vgUUID)

	if lsvg.Spec.MetadataOwner == r.cfg.NodeName {
		created, res, err := r.ensureGroup(ctx, lsvg, hostID)
		if err != nil || created || res.RequeueAfter > 0 {
			// A group that was just created already holds its lockspace here —
			// vgcreate --shared starts it — so the readiness fact is published
			// and the reconcile ends.
			if created {
				// Created a moment ago, so its identity is only known now.
				if err := r.setLockspaceStarted(ctx, lsvg.Name, r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode), true); err != nil {
					return controller.Result{}, err
				}
				return r.publishGroup(ctx, lsvg)
			}
			return res, err
		}
	}

	// The annotation says what this node believes; the lock manager says what is
	// true. They diverge every time lvmlockd and sanlock restart — the lockspaces
	// go with them and nothing rewrites the annotation, so the node reports
	// itself a member of a pool it holds no lease in, and the attachment side,
	// comparing two generation stamps that still agree, believes its volume is
	// locked. That is the two-writer state with every piece of bookkeeping in the
	// cluster saying otherwise. Measured after restarting the lock daemons on a
	// live pool.
	if r.lockspaceStarted(node, lsvg.Name, vgUUID) && !r.lockspaceReallyRunning(ctx, lsvg) {
		r.log.Warning(fmt.Sprintf("[%s] this node is recorded in the lockspace of %s and the lock manager does not have it: starting it again",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
		r.clearStaleLockspaceRegistration(ctx, lsvg)
		if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
			return controller.Result{}, err
		}
		// And the local copy goes with it, rather than being re-read. A Get here
		// comes from the manager's cache, which has not seen the write yet, so
		// the branch below would take the node's word for a lockspace that was
		// just disowned — and the start would wait for the next pass. Measured
		// on the stand: the repair took two minutes instead of one for exactly
		// this reason.
		delete(node.Annotations, LockspaceStartedAnnotationPrefix+lsvg.Name)
	}

	if r.lockspaceStarted(node, lsvg.Name, vgUUID) {
		// Started before this agent could count it — an upgrade, or a restart of
		// the agent alone. The incarnation is unknown, and an unknown incarnation
		// is indistinguishable from a lost one: everything mapped here may or may
		// not still be locked. So the node adopts a number now.
		if node.Annotations[LockspaceGenerationAnnotationPrefix+lsvg.Name] == "" {
			r.log.Info(fmt.Sprintf("[%s] the lockspace of %s was started before this agent counted it, taking stock",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
			if err := r.setLockspaceStarted(ctx, lsvg.Name, vgUUID, true); err != nil {
				return controller.Result{}, err
			}
		}

		// Every pass, not only the first: residue is a STATE, not an event. It
		// appears whenever the lock daemons restart, which this reconciler does
		// not witness — tying the cleanup to "the first time a started lockspace
		// is seen" left the stand with mappings nobody released, because the
		// agent had already seen it once. The check itself is a sysfs scan and
		// two cached reads; only an actual orphan costs a command.
		r.releaseOrphanActivations(ctx, lsvg)

		r.publishNodeState(ctx, lsvg, true, ReasonLockspaceStarted, "")

		return r.publishGroup(ctx, lsvg)
	}

	r.log.Info(fmt.Sprintf("[%s] starting the lockspace of %s with host id %d", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, hostID))
	cmd, err := r.startLockspaceUnderReservations(ctx, lsvg, hostID)
	if err != nil {
		// A LUN that is not visible yet, a lease still held by this node's
		// previous incarnation, a pool being created — all of them look like
		// this and all of them pass. Retry rather than escalate.
		r.log.Warning(fmt.Sprintf("[%s] unable to start the lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))

		// One of them never resolves by itself: the lease volume of a pool that
		// was removed and created again under the same name still has its
		// device-mapper device here, under the same name and a different UUID,
		// and the new lockspace cannot be created over it.
		r.dropLeaseMappingOfAnotherIncarnation(ctx, lsvg)

		// Retrying is right, and retrying SILENTLY is not: one of these causes
		// never resolves by itself, and a node that keeps trying every thirty
		// seconds with nothing published looks identical to a node that is fine.
		r.publishLockStartFailure(ctx, lsvg, err)

		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if err := r.setLockspaceStarted(ctx, lsvg.Name, vgUUID, true); err != nil {
		return controller.Result{}, err
	}

	r.log.Info(fmt.Sprintf("[%s] the lockspace of %s is started", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))

	// The leases are fresh now, so anything still mapped from before the start
	// is mapped without a lock behind it.
	r.releaseOrphanActivations(ctx, lsvg)

	r.publishNodeState(ctx, lsvg, true, ReasonLockspaceStarted, "")

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

		// And the per-node entries, which belong to nobody here: every member
		// applies its own with a field manager of its own, and this pass is the
		// metadata owner publishing what it read from lvm. Left out, the merge
		// patch computed against the old object carries a removal of the whole
		// list — the entries come back one by one as each agent reconciles, and
		// in between the pool reads as a pool almost nobody is in. Seen on the
		// stand: two healthy members reported outside the pool for a minute.
		status.Nodes = lsvg.Status.Nodes
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
		// The group is there. What may have changed is its spec: a pool grows by
		// gaining LUNs, and nothing else in this reconciler would ever notice.
		//
		// It returns nothing on purpose. Joining the lockspace has nothing to do
		// with the size of the pool, and a member that failed to join because a
		// `pvs` call hiccuped would be a node out of its pool for an unrelated
		// reason. Whatever the extension could not do, the next pass does — the
		// member re-examines itself every minute anyway.
		r.extendGroup(ctx, lsvg, devices, wwids)
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

// groupState asks lvm whether the wanted group is there — and whether something
// else is on the pool's devices.
//
// Asked, not remembered, for the reason the whole shared path is: the scan cache
// is refreshed by a scanner with no schedule of its own, and this question is
// always about a moment ago. Believing it ran vgcreate against a group that had
// just been created, which fails with "/dev/<vg>: already exists in filesystem"
// and leaves a healthy pool reporting an error for as long as the cache is cold.
//
// The name is compared, not just the presence of some group. A label naming any
// volume group used to count as proof that this group existed, which is only
// true while the LUN has never been used for anything else.
// extendGroup adds to the group the devices its spec has gained.
//
// Only the metadata owner reaches this, and only through the lock daemons' lvm:
// vgextend changes the group's metadata, which under lvmlockd means taking the
// group's lock, which is what keeps two members from extending it at once.
//
// Devices are never removed here, and that is a decision rather than an
// omission. vgreduce moves extents off a physical volume before dropping it —
// on a pool that is somebody's data being relocated under a live workload, and
// if the extents do not fit it fails halfway. A LUN that has to leave a pool is
// an operation with a plan, not a consequence of editing a list, so the pool
// says what it sees and does nothing.
func (r *Reconciler) extendGroup(
	ctx context.Context,
	lsvg *v1alpha1.LVMSharedVolumeGroup,
	devices map[string]utils.SharedDevice,
	wwids []string,
) {
	if lsvg.Spec.MetadataOwner != r.cfg.NodeName {
		return
	}

	inGroup, known := r.devicesOfGroup(lsvg.Spec.ActualVGNameOnTheNode)
	if !known {
		// Not knowing what the group already holds is not permission to add to
		// it: every device would look missing, and vgextend would be run against
		// the ones already in. The next pass knows.
		return
	}
	missing := make([]utils.SharedDevice, 0, len(wwids))
	for _, wwid := range wwids {
		device, resolved := devices[wwid]
		if !resolved || inGroup[device.Path] {
			continue
		}
		missing = append(missing, device)
	}
	if len(missing) == 0 {
		return
	}

	// The same invariants the group was created under. A device with a different
	// sector size or one that does not divide by the extent size cannot be
	// repaired after it joins, so it is refused before it does.
	if err := utils.CheckSharedDeviceInvariants(missing, extentSizeBytes(lsvg)); err != nil {
		r.log.Error(err, fmt.Sprintf("[%s] %s cannot be extended", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
		r.publishNodeState(ctx, lsvg, true, ReasonExtensionRefused,
			"the volume group "+lsvg.Spec.ActualVGNameOnTheNode+" was not extended: "+err.Error())
		return
	}

	paths := make([]string, 0, len(missing))
	for _, device := range missing {
		paths = append(paths, device.Path)
	}
	sort.Strings(paths)

	r.log.Info(fmt.Sprintf("[%s] extending %s with %d device(s): %s",
		ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(paths), strings.Join(paths, ", ")))
	if cmd, err := r.commands.ExtendVGShared(ctx, lsvg.Spec.ActualVGNameOnTheNode, paths); err != nil {
		r.log.Error(err, fmt.Sprintf("[%s] unable to extend %s (cmd: %s)", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd))
	}
}

// devicesOfGroup is the set of device paths lvm currently counts as part of the
// group. Asked of lvm rather than of the scan cache: the cache is filled by a
// scanner with no schedule of its own, and a stale answer here means adding a
// device that is already in — which lvm refuses, loudly, every pass.
func (r *Reconciler) devicesOfGroup(vgName string) (map[string]bool, bool) {
	pvs, _, _, err := r.commands.GetAllPVs(context.Background())
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to list the devices of %s: %s", ReconcilerName, vgName, err.Error()))
		return nil, false
	}
	if len(pvs) == 0 {
		// A group that exists has physical volumes. An empty list is lvm not
		// answering rather than a group made of nothing.
		return nil, false
	}

	inGroup := make(map[string]bool, len(pvs))
	for _, pv := range pvs {
		if pv.VGName == vgName {
			inGroup[pv.PVName] = true
		}
	}
	return inGroup, true
}

func (r *Reconciler) groupState(
	devices map[string]utils.SharedDevice,
	wantVGName string,
) (exists bool, foreign string) {
	if vg, _, _, err := r.commands.GetVG(wantVGName); err == nil && vg.VGName == wantVGName {
		return true, ""
	}

	for _, device := range devices {
		pv, _, _, err := r.commands.GetPV(device.Path)
		if err != nil {
			continue
		}
		// "[unknown]" is not a name, it is lvm saying it cannot tell — which is
		// what a physical volume looks like after a vgcreate that labelled it and
		// then failed. Reading it as someone else's group turns this module's own
		// debris into a permanent refusal to create anything.
		if pv.VGName == "" || pv.VGName == unknownVGName || pv.VGName == wantVGName {
			continue
		}
		if foreign == "" {
			foreign = pv.VGName
		}
	}

	return false, foreign
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

	// Presence, not identity: whatever group the annotation was written for, the
	// node is leaving this one, and a lockspace that is not running makes
	// vgchange --lock-stop a no-op anyway.
	if !r.lockspaceStarted(node, lsvg.Name, "") {
		// Out of the lockspace, and possibly still registered with the array:
		// the two are separate facts. A node that registered and then failed to
		// start its lockspace keeps a key nobody asks it to give up, and
		// `vgremove` refuses while it is there — "Found 2 PR keys ... Stop PR for
		// VG on other hosts", with nobody left to stop it. Found on the stand,
		// on a pool that could not be removed for an hour.
		r.stopReservationsOnLeaving(ctx, lsvg)
		return controller.Result{}, nil
	}

	if active := r.activeLVs(lsvg.Spec.ActualVGNameOnTheNode); len(active) > 0 {
		// The volumes are still here and the pool has already decided this node
		// is out of it. Ordinarily the attachment reconciler is deactivating
		// them and one more pass is all that is needed, so that is tried first.
		if r.attachmentsRemain(ctx, lsvg.Name) {
			r.log.Warning(fmt.Sprintf("[%s] cannot stop the lockspace of %s yet: %d volume(s) still active here (%s)",
				ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, len(active), strings.Join(active, ", ")))
			return controller.Result{RequeueAfter: 15 * time.Second}, nil
		}

		// Nothing asks for them any more, and they are still mapped. That is the
		// live-stuck node: kubelet is gone, its pods were never removed, their
		// mounts hold the volumes, and sanlock will renew the lease for as long
		// as this node breathes. Waiting here is waiting for a human.
		//
		// So the node raises the barrier over its own volumes — the same error
		// target the fencing handler writes — and only then stops the lockspace.
		// The order is the whole safety of it: leases must not expire while a
		// write can still reach the array, because by then the volume may belong
		// to somebody else. Data is not damaged either way; writes fail instead
		// of landing somewhere they should not.
		r.log.Warning(fmt.Sprintf("[%s] %d volume(s) of %s are mapped here with nothing asking for them and the node is out of the pool: raising the barrier",
			ReconcilerName, len(active), lsvg.Spec.ActualVGNameOnTheNode))

		blocked := false
		for _, lvName := range active {
			dmName := utils.DMName(lsvg.Spec.ActualVGNameOnTheNode, lvName)
			if cmd, err := r.commands.WipeDMTable(ctx, dmName); err != nil {
				r.log.Error(err, fmt.Sprintf("[%s] the barrier over %s failed (cmd: %s)", ReconcilerName, dmName, cmd))
				blocked = true
			}
		}
		if blocked {
			// One map short of a barrier is not a barrier: a write could still
			// reach the array through it, so the leases stay where they are.
			return controller.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	if err := r.setLockspaceStarted(ctx, lsvg.Name, "", false); err != nil {
		return controller.Result{}, err
	}

	cmd, err := r.commands.VGLockStop(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to stop the lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
		return controller.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// And its registration goes with its lockspace when the group is held under
	// reservations.
	//
	// Not tidiness: `vgremove` on such a group refuses while anybody else is
	// still registered — "Found 3 PR keys on /dev/mapper/mpathi. Stop PR for VG
	// vghw on other hosts (vgchange --persist stop)" — so a member that leaves
	// without giving its key up leaves a pool that cannot be removed. Found on
	// the stand, taking a switched pool down.
	//
	// After the lockspace, in that order: lvm2 refuses the other way round with
	// "locking should be stopped before PR".
	r.stopReservationsOnLeaving(ctx, lsvg)

	// The node is out of the pool by its own account now, and leaving its old
	// answer standing would be a lie readers act on.
	r.retractNodeState(ctx, lsvg)

	r.log.Info(fmt.Sprintf("[%s] the lockspace of %s is stopped", ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	return controller.Result{}, nil
}

// activeLVs names the volumes of the group that are active on this node.
//
// Device-mapper, not the scan cache: the question is about right now — whether
// anything is still mapped here — and the cache is filled by a scanner with no
// schedule of its own. Asking lvm instead would spend the pool's group lock on
// a question rather than on work.
func (r *Reconciler) activeLVs(vgName string) []string {
	active, err := utils.ActiveLVsOfGroupHere(vgName)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to list the active volumes of %s: %s",
			ReconcilerName, vgName, err.Error()))
		return nil
	}
	return active
}

// releaseOrphanActivations deactivates volumes of the group that are mapped on
// this node with nothing asking for them.
//
// It runs right after a lockspace start, which is the moment the node's leases
// are known to be fresh and everything mapped from before is known to be
// unlocked. Those mappings are the residue of a lock-daemon restart: the kernel
// kept them while sanlock lost every lease, so they are devices no lock stands
// behind — and a device like that is what lets a second node write to a volume
// this one still shows as active.
//
// Only volumes with no attachment for this node are touched, and lvchange
// refuses to deactivate a volume that is open, which is the safety net: a
// mapping that is genuinely in use survives and is reported instead.
func (r *Reconciler) releaseOrphanActivations(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) {
	active := r.activeLVs(lsvg.Spec.ActualVGNameOnTheNode)
	if len(active) == 0 {
		r.reportUnlockedMappings(lsvg.Spec.ActualVGNameOnTheNode, 0)
		return
	}

	wanted, err := r.attachedHere(ctx, lsvg.Name)
	if err != nil {
		// Without the list there is no way to tell residue from work in
		// progress, and deactivating on a guess is the one thing that must not
		// happen here.
		r.log.Warning(fmt.Sprintf("[%s] unable to tell which volumes of %s belong here: %s",
			ReconcilerName, lsvg.Name, err.Error()))
		return
	}

	orphans := make([]string, 0, len(active))
	for _, lvName := range active {
		if !wanted[lvName] {
			orphans = append(orphans, lvName)
		}
	}
	r.reportUnlockedMappings(lsvg.Spec.ActualVGNameOnTheNode, len(orphans))
	if len(orphans) == 0 {
		return
	}

	r.log.Info(fmt.Sprintf("[%s] releasing %d volume(s) of %s mapped here with no attachment: %s",
		ReconcilerName, len(orphans), lsvg.Spec.ActualVGNameOnTheNode, strings.Join(orphans, ", ")))
	if cmd, err := r.commands.LVDeactivateShared(ctx, lsvg.Spec.ActualVGNameOnTheNode, orphans); err != nil {
		// Not an error of this reconcile: a volume that is open refuses to go,
		// and that refusal is the safety net rather than a fault.
		r.log.Warning(fmt.Sprintf("[%s] some volumes of %s could not be released (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
	}

	// And then check, because lvchange cannot be taken at its word here. It
	// decides whether a volume is active on this node from the lock it holds,
	// not from device-mapper: with the lease gone it finds nothing to do, exits
	// zero, and leaves the mapping standing. Measured on the stand — the command
	// was silent and the device was still there afterwards.
	r.removeLeftoverMappings(ctx, lsvg.Spec.ActualVGNameOnTheNode, orphans)
}

// reportUnlockedMappings publishes the invariant of a shared pool as a number:
// volumes mapped on this node with nothing here asking for them. It is set on
// every pass, zero included — a gauge nobody clears is a gauge nobody believes.
func (r *Reconciler) reportUnlockedMappings(vgName string, count int) {
	if r.metrics == nil {
		return
	}
	r.metrics.SharedPoolUnlockedMappings(vgName).Set(float64(count))
}

// removeLeftoverMappings tears down the device-mapper devices of volumes that
// were just released and did not go, which is the only way residue of a
// lock-daemon restart goes.
//
// It asks device-mapper to remove each of those volumes by name and does not
// look first. Looking first is what failed on the stand: the node logged
// "releasing 1 volume(s)" every minute for hours and never once reached this
// removal, because a scan of /sys/block came back empty while dmsetup listed
// the mapping at the same moment, with the same minor number and an open count
// of zero. Asking about a name in a directory listing and acting on a name are
// two different questions asked of two different authorities, and only one of
// them owns the answer. Now there is one command, and a device that is already
// gone is a success rather than an error.
//
// dmsetup refuses an open device, and that refusal is kept: a mapping something
// is still using is not residue, whatever the lock state says.
func (r *Reconciler) removeLeftoverMappings(ctx context.Context, vgName string, released []string) {
	for _, lvName := range released {
		dmName := utils.DMName(vgName, lvName)
		if cmd, err := r.commands.RemoveDMDevice(ctx, dmName); err != nil {
			r.log.Warning(fmt.Sprintf("[%s] %s could not be removed (cmd: %s): %s",
				ReconcilerName, dmName, cmd, err.Error()))
		}
	}
}

// attachmentsRemain reports whether anything still asks this node for a volume
// of the group. It is the difference between "the release is in progress" and
// "nobody is coming", and those two want opposite things.
func (r *Reconciler) attachmentsRemain(ctx context.Context, groupName string) bool {
	wanted, err := r.attachedHere(ctx, groupName)
	if err != nil {
		// Unknown, so treated as "someone may still want them": waiting costs a
		// requeue, raising a barrier on a guess costs a running workload.
		r.log.Warning(fmt.Sprintf("[%s] unable to tell whether anything still needs the volumes of %s: %s",
			ReconcilerName, groupName, err.Error()))
		return true
	}
	return len(wanted) > 0
}

// attachedHere is the set of volumes this node has an attachment for.
func (r *Reconciler) attachedHere(ctx context.Context, groupName string) (map[string]bool, error) {
	attachments := &v1alpha1.LVMSharedLogicalVolumeAttachmentList{}
	if err := r.cl.List(ctx, attachments); err != nil {
		return nil, fmt.Errorf("list attachments: %w", err)
	}

	volumes := &v1alpha1.LVMSharedLogicalVolumeList{}
	if err := r.cl.List(ctx, volumes); err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}

	lvNameOf := make(map[string]string, len(volumes.Items))
	for i := range volumes.Items {
		volume := &volumes.Items[i]
		if volume.Spec.LVMSharedVolumeGroupName == groupName {
			lvNameOf[volume.Name] = volume.Spec.ActualLVNameOnTheNode
		}
	}

	wanted := make(map[string]bool, len(attachments.Items))
	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.NodeName != r.cfg.NodeName {
			continue
		}
		if lvName, ok := lvNameOf[attachment.Spec.LVMSharedLogicalVolumeName]; ok {
			wanted[lvName] = true
		}
	}
	return wanted, nil
}

func (r *Reconciler) node(ctx context.Context) (*corev1.Node, error) {
	node := &corev1.Node{}
	if err := r.cl.Get(ctx, client.ObjectKey{Name: r.cfg.NodeName}, node); err != nil {
		return nil, fmt.Errorf("get own Node %s: %w", r.cfg.NodeName, err)
	}
	return node, nil
}

// lockspaceStarted reports whether this node holds the lockspace OF THIS GROUP,
// and the emphasis is the point.
//
// The value is the volume group's uuid rather than "true", because a group can
// be destroyed and created again under the same name — which is exactly what
// happens while a pool is being commissioned. A boolean keyed by name survives
// that and lies: the node claims a lockspace of a group that no longer exists,
// never starts the one that does, and the pool looks healthy on paper with one
// member actually holding leases.
// nextGeneration is the successor of whatever is written, and 1 when nothing is.
// An unparseable value is treated as nothing: the point of the number is that it
// differs from the previous one, not that it counts anything in particular.
func nextGeneration(current string) string {
	n, err := strconv.ParseInt(current, 10, 64)
	if err != nil || n < 0 {
		n = 0
	}
	return strconv.FormatInt(n+1, 10)
}

// vgUUID is the group's identity, or an empty string when it cannot be read —
// which is the ordinary state before the group exists.
func (r *Reconciler) vgUUID(vgName string) string {
	vg, _, _, err := r.commands.GetVG(vgName)
	if err != nil {
		return ""
	}
	return vg.VGUUID
}

func (r *Reconciler) lockspaceStarted(node *corev1.Node, groupName, vgUUID string) bool {
	value := node.Annotations[LockspaceStartedAnnotationPrefix+groupName]
	if value == "" {
		return false
	}
	if vgUUID == "" {
		// The group cannot be read right now. Whatever is written was written by
		// this node about this group, so it is the best answer available.
		return true
	}
	return value == vgUUID
}

func (r *Reconciler) setLockspaceStarted(ctx context.Context, groupName, vgUUID string, started bool) error {
	node, err := r.node(ctx)
	if err != nil {
		return err
	}

	key := LockspaceStartedAnnotationPrefix + groupName
	generationKey := LockspaceGenerationAnnotationPrefix + groupName
	patch := client.MergeFrom(node.DeepCopy())
	if started {
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		// The generation moves only when the lockspace is actually started, and
		// it moves every time — including a start that follows a daemon restart,
		// which is the case it exists for. Everything activated under the old
		// number is mapped and unlocked, and the attachment reconciler treats a
		// mismatch as a reason to activate again.
		//
		// The absence of the fact is what marks a fresh start, and comparing the
		// UUIDs alone was not enough to notice it: a lockspace started at a
		// moment when lvm cannot be asked for the group's identity records an
		// empty UUID, which equals the empty value a cleared annotation reads
		// back as. The generation then stood still across a restart, and every
		// attachment went on vouching for a lock that no longer existed.
		_, recorded := node.Annotations[key]
		if !recorded || node.Annotations[key] != vgUUID || node.Annotations[generationKey] == "" {
			node.Annotations[generationKey] = nextGeneration(node.Annotations[generationKey])
		}
		node.Annotations[key] = vgUUID
	} else {
		if _, ok := node.Annotations[key]; !ok {
			return nil
		}
		delete(node.Annotations, key)
		// The generation is deliberately NOT deleted. It counts incarnations of
		// this node's lockspace, and a counter that restarts at one would let a
		// stale attachment match a fresh lockspace by coincidence.
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

// clearStaleLockspaceRegistration stops a lockspace that only lvmlockd still
// believes in.
//
// When lvmlockd has the lockspace and sanlock does not, `vgchange --lock-start`
// succeeds and does nothing: lvmlockd considers it already started, sanlock goes
// on answering "invalid lockspace" to every lease request, and the node repeats
// the start every pass forever. Found on the stand after an eviction, with a
// volume that could not be activated on a node that reported itself healthy.
//
// Stopping first makes the start real. A lockspace sanlock does not hold has no
// lease to give up, so nothing is lost by stopping it.
func (r *Reconciler) clearStaleLockspaceRegistration(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) {
	registered, held, err := r.commands.LockspaceState(ctx, lsvg.Spec.ActualVGNameOnTheNode)
	if err != nil || !registered || held {
		return
	}

	r.log.Warning(fmt.Sprintf("[%s] lvmlockd still holds a registration for the lockspace of %s that sanlock has dropped: stopping it so it can be started for real",
		ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode))
	if cmd, err := r.commands.VGLockStop(ctx, lsvg.Spec.ActualVGNameOnTheNode); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to stop the stale lockspace of %s (cmd: %s): %s",
			ReconcilerName, lsvg.Spec.ActualVGNameOnTheNode, cmd, err.Error()))
	}
}

// dropLeaseMappingOfAnotherIncarnation removes a lease mapping left behind by a
// pool that no longer exists.
//
// It runs only after a lockspace has failed to start, and only when the mapping
// belongs to a different volume group than the one this pool has now — the same
// name, a different UUID. On the stand that state cost a member of a recreated
// pool its place in it: "device-mapper: create ioctl on
// e2e--shared--pool-lvmlock failed: Device or resource busy", once a minute,
// with the group otherwise healthy on the other two nodes.
func (r *Reconciler) dropLeaseMappingOfAnotherIncarnation(ctx context.Context, lsvg *v1alpha1.LVMSharedVolumeGroup) {
	name, stale := utils.LeaseMappingOfOtherIncarnation(
		lsvg.Spec.ActualVGNameOnTheNode, r.vgUUID(lsvg.Spec.ActualVGNameOnTheNode))
	if !stale {
		return
	}

	r.log.Warning(fmt.Sprintf("[%s] the lease mapping %s belongs to a volume group that no longer exists; removing it so the lockspace can start",
		ReconcilerName, name))
	if cmd, err := r.commands.RemoveDMDeviceDeferred(ctx, name); err != nil {
		r.log.Warning(fmt.Sprintf("[%s] unable to remove the stale lease mapping %s (cmd: %s): %s",
			ReconcilerName, name, cmd, err.Error()))
	}
}
