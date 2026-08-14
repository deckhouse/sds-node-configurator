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

package lvg

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/cache"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/controller"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/monitoring"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/repository"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/utils"
)

const ReconcilerName = "lvm-volume-group-watcher-controller"

// udevadmTriggerTimeout caps a single best-effort `udevadm trigger
// --action=change <paths>` invocation that the reconciler issues after
// pvcreate/vgcreate. The trigger only enqueues uevents in the kernel and
// normally returns within ~100ms; 10s leaves a wide safety margin for
// nodes under heavy stress without blocking a graceful shutdown for long.
const udevadmTriggerTimeout = 10 * time.Second

// udevadmTriggerCmdLabel is the metric label used for UdevadmTrigger in
// UtilsCommandsDuration / UtilsCommandsExecutionCount / UtilsCommandsErrorsCount.
// Keep it in sync with how operators query Prometheus.
const udevadmTriggerCmdLabel = "udevadm-trigger"

// defaultRequeueInterval is the retry cadence used when VolumeGroupScanInterval
// carries no value. It exists because a zero RequeueAfter means "do not requeue"
// to controller-runtime, so an unset interval would silently turn every
// deliberate retry in this reconciler into a dropped one. config.NewConfig always
// sets a positive value; the fallback is for the zero-value Config a caller can
// still construct.
const defaultRequeueInterval = 5 * time.Second

// The backoff for retrying an LVMVolumeGroup whose reconcile made no progress and
// may never make any: a spec.fileDevices entry the node could not bring up, or a
// node whose Volume Groups cannot be read at all. See
// Reconciler.noProgressRequeueAfter for why the retry has to slow down.
const (
	// noProgressRetryMaxShift bounds the doubling. With the default 5s base the
	// intervals run 5s, 10s, 20s, 40s, 80s, 160s and then hit the ceiling below,
	// so an LVMVolumeGroup settles onto the ceiling on its seventh round, roughly
	// five minutes after the first round that made no progress.
	noProgressRetryMaxShift = 6
	// noProgressRetryMaxInterval is the ceiling. Deliberately far below
	// controller-runtime's resync — the LVMVolumeGroup has to keep being retried on
	// its own — and short enough that an operator who has just freed space or fixed
	// the node sees it recover rather than assuming it is stuck.
	noProgressRetryMaxInterval = 5 * time.Minute
)

// lvmDefaultPhysicalExtent is LVM's default PE when vgcreate is run without --physicalextentsize.
var lvmDefaultPhysicalExtent = resource.MustParse("4Mi")

// extentSizeForThinPoolAlign returns a positive extent size for AlignSizeToExtent.
// Status.ExtentSize stays zero until the discoverer runs; it is also zero before the VG exists.
func extentSizeForThinPoolAlign(lvg *v1alpha1.LVMVolumeGroup, vg *internal.VGData) resource.Quantity {
	if lvg != nil && lvg.Status.ExtentSize.Value() > 0 {
		return lvg.Status.ExtentSize
	}
	if vg != nil && vg.VGExtentSize.Value() > 0 {
		return vg.VGExtentSize
	}
	return lvmDefaultPhysicalExtent
}

// alignThinPoolSizeForValidation aligns a thin-pool's requested size to the
// extent boundary for the create-time capacity check.
//
// Absolute sizes are rounded UP — that is the real number of extents LVM will
// consume, so an oversized absolute pool must still be rejected.
//
// Percentage sizes (e.g. "100%") are rounded DOWN instead. During create
// validation the VG size is the raw block-device/file sum, which is not
// extent-aligned, so a "100%" request rounded up lands one extent past the VG
// and would wrongly fail the capacity check — even though the pool is created
// with %FREE and fits. A percentage of the VG can never legitimately exceed
// it, so flooring is always safe and also makes split percentages (e.g.
// "50%"+"50%") sum to at most the VG size.
func alignThinPoolSizeForValidation(specSize string, requested, extentSize resource.Quantity) (resource.Quantity, error) {
	if utils.IsPercentSize(specSize) {
		extentBytes := extentSize.Value()
		if extentBytes <= 0 {
			return resource.Quantity{}, fmt.Errorf("extent size must be positive, got %d", extentBytes)
		}
		floored := (requested.Value() / extentBytes) * extentBytes
		return *resource.NewQuantity(floored, resource.BinarySI), nil
	}
	return utils.AlignSizeToExtent(requested, extentSize)
}

type Reconciler struct {
	cl       client.Client
	log      logger.Logger
	lvgCl    *repository.LVGClient
	bdCl     *repository.BDClient
	metrics  *monitoring.Metrics
	sdsCache *cache.Cache
	cfg      ReconcilerConfig
	commands utils.Commands
	// resolver maps a /dev/* PV name to its canonical block device. It is
	// only used to recognise a managed loop PV that lvm.static reported
	// under a /dev/disk/by-id or /dev/block/MAJ:MIN alias. Defaults to
	// utils.HostNsenterCanonicalResolver; overridable in tests.
	resolver utils.CanonicalPathResolver

	// aliasResolveFailures counts, per LVG name, how many consecutive
	// extendFileDevicesIfNeeded rounds made no progress purely because
	// alias-form PV names could not be resolved. It escalates the condition
	// from a generic "Updating" retry to ReasonAliasResolutionFailed once the
	// failures look persistent, so a stuck resolver is alertable instead of
	// looking like an ordinary in-flight update. Reset on any round that makes
	// progress or genuinely has nothing to do. The reconciler runs with
	// MaxConcurrentReconciles==1, but the mutex keeps the map safe if that ever
	// changes.
	aliasResolveFailuresMu sync.Mutex
	aliasResolveFailures   map[string]int

	// noProgressRetries counts, per LVG name, how many consecutive reconciles ended
	// without making progress. It exists to slow the retry down, because every such
	// state has two very different causes with the same shape:
	//
	//   - a spec.fileDevices entry the node could not bring up — a filesystem that
	//     will have room in a minute, or an entry that will never fit on this node;
	//   - a node whose Volume Groups cannot be read (ReasonVGCheckFailed) — a
	//     transient nsenter hiccup, or an lvm.static that is simply gone.
	//
	// Only the transient half is worth polling at VolumeGroupScanInterval. The
	// permanent half is a fleet of nodes each running a `stat -f`, a `losetup -j`
	// per entry and a live `lvm pvs`/`vgs` every few seconds, forever, over a
	// question whose answer cannot change until somebody intervenes. Nothing tells
	// the two apart from the inside, so the interval backs off instead.
	//
	// Reset whenever a round leaves nothing unapplied or the answer becomes
	// knowable, so a transient shortage does not leave the LVMVolumeGroup on a long
	// interval afterwards. Same locking rationale as aliasResolveFailures.
	noProgressRetriesMu sync.Mutex
	noProgressRetries   map[string]int
}

type ReconcilerConfig struct {
	NodeName                string
	BlockDeviceScanInterval time.Duration
	VolumeGroupScanInterval time.Duration
	CmdDeadlineDuration     time.Duration
	// FileDevicesDirectory is the base directory backing files are confined
	// to; spec.fileDevices[].directory must be this path or a subdirectory of
	// it. Empty means "no restriction" (used by unit tests that do not care).
	FileDevicesDirectory string
	// FileDevicesMinFreeSpacePercent is the share of the backing-file filesystem
	// that must remain free after a backing file is created or grown.
	//
	// It is a share rather than an absolute size because that is the only form
	// that travels: the same setting has to be sensible on a 30Gi node root and
	// on a 4Ti data disk, and kubelet's own eviction thresholds are percentages
	// for the same reason. Zero disables the reserve, which is only defensible
	// for a filesystem nothing else on the node depends on.
	FileDevicesMinFreeSpacePercent int
}

func NewReconciler(
	cl client.Client,
	log logger.Logger,
	metrics *monitoring.Metrics,
	sdsCache *cache.Cache,
	commands utils.Commands,
	cfg ReconcilerConfig,
) *Reconciler {
	return &Reconciler{
		cl:  cl,
		log: log,
		lvgCl: repository.NewLVGClient(
			cl,
			log,
			metrics,
			cfg.NodeName,
			ReconcilerName,
		),
		bdCl:                 repository.NewBDClient(cl, metrics),
		metrics:              metrics,
		sdsCache:             sdsCache,
		cfg:                  cfg,
		commands:             commands,
		resolver:             utils.HostNsenterCanonicalResolver,
		aliasResolveFailures: make(map[string]int),
		noProgressRetries:    make(map[string]int),
	}
}

func (r *Reconciler) Name() string {
	return ReconcilerName
}

func (r *Reconciler) MaxConcurrentReconciles() int {
	return 1
}

// ShouldReconcileUpdate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileUpdate(objectOld *v1alpha1.LVMVolumeGroup, objectNew *v1alpha1.LVMVolumeGroup) bool {
	return r.shouldLVGWatcherReconcileUpdateEvent(objectOld, objectNew)
}

// ShouldReconcileCreate implements controller.Reconciler.
func (r *Reconciler) ShouldReconcileCreate(obj *v1alpha1.LVMVolumeGroup) bool {
	return checkIfLVGBelongsToNode(obj, r.cfg.NodeName)
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, request controller.ReconcileRequest[*v1alpha1.LVMVolumeGroup]) (controller.Result, error) {
	r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Reconciler starts to reconcile the request %s", request.Object.Name))

	lvg := request.Object

	belongs := checkIfLVGBelongsToNode(lvg, r.cfg.NodeName)
	if !belongs {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s does not belong to the node %s", lvg.Name, r.cfg.NodeName))
		return controller.Result{}, nil
	}
	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s belongs to the node %s. Starts to reconcile", lvg.Name, r.cfg.NodeName))

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to add the finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	added, err := r.addLVGFinalizerIfNotExist(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add the finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		return controller.Result{}, err
	}

	if added {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully added a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to add a finalizer %s to the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	// this case handles the situation when a user decides to remove LVMVolumeGroup resource without created VG
	deleted, waitForCache, err := r.deleteLVGIfNeeded(ctx, lvg)
	if err != nil {
		return controller.Result{}, err
	}

	if deleted {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s was deleted, stop the reconciliation", lvg.Name))
		return controller.Result{}, nil
	}

	// The node has the Volume Group and the cache does not, so every delete
	// decision below it would be made on data that is known to be wrong. Stop the
	// reconcile here and come back: the scanner refills the cache on its own, and
	// nothing else in this function would wait for it.
	if waitForCache {
		return controller.Result{RequeueAfter: r.requeueInterval()}, nil
	}

	if _, exist := lvg.Labels[internal.LVGUpdateTriggerLabel]; exist {
		delete(lvg.Labels, internal.LVGUpdateTriggerLabel)
		err = r.cl.Update(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to update the LVMVolumeGroup %s", lvg.Name))
			return controller.Result{}, err
		}
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully removed the label %s from the LVMVolumeGroup %s", internal.LVGUpdateTriggerLabel, lvg.Name))
	}

	// Teardown runs before the spec is validated, and must.
	//
	// Everything below — listing BlockDevices by the selector, validating that the
	// spec still describes them — exists to protect PROVISIONING. Gating deletion
	// behind it means a resource whose selector matches nothing can never be
	// deleted: the reconcile sets VGConfigurationApplied=ValidationFailed and
	// returns, the delete path is never reached, the finalizer stays, and the
	// resource sits in Terminating forever. With a cluster policy that forbids
	// removing Deckhouse finalizers by hand (deny-deckhouse-finalizers), there is
	// then no way out at all — which is the state a hundred and fifty-five
	// LVMVolumeGroups were found in, all of them referring to BlockDevices that no
	// longer exist, all of them asked to be deleted.
	//
	// A resource being deleted has nothing to validate: the question is not whether
	// its spec is right but what to tear down, and that is answered from the node
	// and the resource's own status (see vgRemovalAllowed).
	if lvg.DeletionTimestamp != nil {
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s is being deleted; running the delete path without validating the spec", lvg.Name))
		shouldRequeue, err := r.reconcileLVGDeleteFunc(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to delete the LVMVolumeGroup %s", lvg.Name))
		}
		// Returning the error here as well would throw the interval away:
		// controller-runtime ignores a non-zero Result whenever the error is
		// non-nil and falls back to the workqueue's exponential backoff, which
		// climbs to sixteen minutes after a run of failures. Teardown is the one
		// path that must not slow down under repeated failure, and the error is
		// already reported above, so the interval is what travels out.
		if shouldRequeue {
			return controller.Result{RequeueAfter: r.requeueInterval()}, nil
		}
		return controller.Result{}, err
	}

	// blockDeviceSelector is optional: a file-only LVMVolumeGroup (only
	// spec.fileDevices) carries no selector. Listing block devices with a
	// nil selector would match EVERY BlockDevice on the node (the repository
	// maps an empty selector to "select all"), and validateSpecBlockDevices
	// dereferences the nil selector. So skip block-device discovery and
	// validation entirely when there is no selector, and reconcile only the
	// file devices.
	blockDevices := make(map[string]v1alpha1.BlockDevice)
	if lvg.Spec.BlockDeviceSelector != nil {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to get block device resources for the LVMVolumeGroup %s by the selector %v", lvg.Name, lvg.Spec.BlockDeviceSelector))
		blockDevices, err = r.bdCl.GetAPIBlockDevices(ctx, ReconcilerName, lvg.Spec.BlockDeviceSelector)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to get BlockDevices. Retry in %s", r.cfg.BlockDeviceScanInterval.String()))
			err = r.lvgCl.UpdateLVGConditionIfNeeded(
				ctx,
				lvg,
				v1.ConditionFalse,
				internal.TypeVGConfigurationApplied,
				"NoBlockDevices",
				fmt.Sprintf("unable to get block devices resources, err: %s", err.Error()),
			)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, r.cfg.BlockDeviceScanInterval.String()))
			}

			return controller.Result{RequeueAfter: r.cfg.BlockDeviceScanInterval}, nil
		}
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully got block device resources for the LVMVolumeGroup %s by the selector %v", lvg.Name, lvg.Spec.BlockDeviceSelector))

		blockDevices = filterBlockDevicesByNodeName(blockDevices, lvg.Spec.Local.NodeName)

		valid, reason := validateSpecBlockDevices(lvg, blockDevices)
		if !valid {
			r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupController] validation failed for the LVMVolumeGroup %s, reason: %s", lvg.Name, reason))
			err = r.lvgCl.UpdateLVGConditionIfNeeded(
				ctx,
				lvg,
				v1.ConditionFalse,
				internal.TypeVGConfigurationApplied,
				internal.ReasonValidationFailed,
				reason,
			)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
				return controller.Result{}, err
			}

			return controller.Result{}, nil
		}
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully validated BlockDevices of the LVMVolumeGroup %s", lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s has no blockDeviceSelector (file-only); skipping block device discovery and validation", lvg.Name))
	}

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to add label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, r.cfg.NodeName))
	added, err = r.addLVGLabelIfNeeded(ctx, lvg, internal.LVGMetadataNameLabelKey, lvg.Name)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, lvg.Name))
		return controller.Result{}, err
	}

	if added {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully added label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to add label %s to the LVMVolumeGroup %s", internal.LVGMetadataNameLabelKey, lvg.Name))
	}

	// We do this after BlockDevices validation and node belonging check to prevent multiple updates by all agents pods
	bds, _ := r.sdsCache.GetDevices()
	if len(bds) == 0 {
		r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no block devices in the cache, add the LVMVolumeGroup %s to requeue", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			"CacheEmpty",
			"unable to apply configuration due to the cache's state",
		)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s. Retry in %s", internal.TypeVGConfigurationApplied, lvg.Name, r.cfg.VolumeGroupScanInterval.String()))
		}

		return controller.Result{
			RequeueAfter: r.cfg.VolumeGroupScanInterval,
		}, nil
	}

	r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] tries to sync status and spec thin-pool AllocationLimit fields for the LVMVolumeGroup %s", lvg.Name))
	err = r.syncThinPoolsAllocationLimit(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to sync status and spec thin-pool AllocationLimit fields for the LVMVolumeGroup %s", lvg.Name))
		return controller.Result{}, err
	}

	requeueAfter, err := r.runEventReconcile(ctx, lvg, blockDevices)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
	}

	if requeueAfter > 0 {
		r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] the LVMVolumeGroup %s event will be requeued in %s", lvg.Name, requeueAfter.String()))
		return controller.Result{
			RequeueAfter: requeueAfter,
		}, nil
	}
	r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] Reconciler successfully reconciled the LVMVolumeGroup %s", lvg.Name))

	return controller.Result{}, nil
}

// runEventReconcile dispatches to the create or the update path and reports how
// long to wait before coming back; zero means "do not requeue". Deletion is not
// one of its cases: Reconcile runs the delete path itself, ahead of the spec
// validation this function sits behind.
//
// A duration rather than a bool because not every retry deserves the same
// interval. VolumeGroupScanInterval (5s by default) is the right cadence for
// "something is in flight" and the wrong one for "an entry does not fit on this
// node", which is a state that can persist indefinitely — see
// noProgressRequeueAfter. The create path has no such distinction to make and
// still answers yes/no.
func (r *Reconciler) runEventReconcile(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (time.Duration, error) {
	recType, vgStateKnown := r.identifyLVGReconcileFunc(ctx, lvg)

	switch recType {
	case internal.CreateReconcile:
		r.log.Info(fmt.Sprintf("[runEventReconcile] CreateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		shouldRequeue, err := r.reconcileLVGCreateFunc(ctx, lvg, blockDevices)
		return r.requeueIntervalIf(shouldRequeue), err
	case internal.UpdateReconcile:
		r.log.Info(fmt.Sprintf("[runEventReconcile] UpdateReconcile starts the reconciliation for the LVMVolumeGroup %s", lvg.Name))
		return r.reconcileLVGUpdateFunc(ctx, lvg, blockDevices)
	// There is deliberately no delete case. Teardown is dispatched by Reconcile
	// before the spec is validated, so a resource with a deletionTimestamp never
	// reaches here — which is the invariant the default branch below rests on.
	default:
		// Reaching this is not "nothing to do", it is "the cache disagrees with
		// the node". The only way here is: the resource is not being deleted, its
		// VG is absent from the cache, and shouldReconcileLVGByCreateFunc
		// confirmed against live LVM that the VG nevertheless exists (or could
		// not tell and assumed it does) — so create is refused and update has no
		// cached VG to work with.
		//
		// Returning without a requeue would leave it there. The cache is filled
		// only by the scanner, the scanner runs only on udev events, and writing
		// LVM metadata to a loop device does not reliably raise one — which is
		// exactly the situation that guard exists for. Nothing else would wake
		// this LVMVolumeGroup until controller-runtime's resync, hours later.
		//
		// The two ways to get here need different words. "The cache has not
		// caught up" is a state that clears itself; "LVM could not be read" is a
		// broken node that will sit here forever, and a resource that only ever
		// says Pending sends the operator looking in the wrong place. Both write
		// a condition — silence here is what made this indistinguishable from an
		// ordinary in-flight update.
		// The interval differs for the same reason the words do. CacheStale clears
		// itself on the next scan, so it deserves the scan interval. VGCheckFailed
		// does not clear itself at all — lvm.static is missing, nsenter is broken,
		// /etc/lvm is unreadable — and polling it at 5s has every affected node
		// re-running `vgs` (and a `pvs` plus a `losetup` per loop PV, via
		// vgExistsOnNode) forever over a question whose answer cannot change until
		// somebody fixes the node. Same reasoning as noProgressRequeueAfter, whose
		// backoff this reuses.
		reason, msg := internal.ReasonCacheStale, fmt.Sprintf("VG %s is present on the node but missing from the agent's cache; waiting for the cache to catch up", lvg.Spec.ActualVGNameOnTheNode)
		requeueAfter := r.requeueInterval()
		if !vgStateKnown {
			reason, msg = internal.ReasonVGCheckFailed, fmt.Sprintf("unable to read the Volume Groups of the node to decide whether VG %s has to be created; nothing is created while this is unknown", lvg.Spec.ActualVGNameOnTheNode)
			requeueAfter = r.noProgressRequeueAfter(lvg.Name)
		} else {
			// The cache caught up enough to answer; do not carry a stale backoff
			// into the next round of a different problem.
			r.resetNoProgressRetries(lvg.Name)
		}
		r.log.Warning(fmt.Sprintf("[runEventReconcile] %s (LVMVolumeGroup %s)", msg, lvg.Name))
		if err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, reason, msg); err != nil {
			r.log.Error(err, fmt.Sprintf("[runEventReconcile] unable to add a condition %s reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, reason, lvg.Name))
		}
		return requeueAfter, nil
	}
}

// requeueInterval is the reconciler's ordinary retry cadence.
func (r *Reconciler) requeueInterval() time.Duration {
	if r.cfg.VolumeGroupScanInterval > 0 {
		return r.cfg.VolumeGroupScanInterval
	}
	return defaultRequeueInterval
}

// requeueIntervalIf translates the create/delete paths' yes/no answer into the
// interval the reconciler has always used for them. Those two have no reason to
// distinguish a transient retry from a permanent one — see noProgressRequeueAfter
// for the case that does.
func (r *Reconciler) requeueIntervalIf(requeue bool) time.Duration {
	if !requeue {
		return 0
	}
	return r.requeueInterval()
}

func (r *Reconciler) reconcileLVGDeleteFunc(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))
	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] tries to add the condition %s status false to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied && c.Reason != internal.ReasonTerminating {
			err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, "trying to delete VG")
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				return true, err
			}
			break
		}
	}

	_, exist := lvg.Annotations[internal.DeletionProtectionAnnotation]
	if exist {
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] the LVMVolumeGroup %s has a deletion timestamp but also has a deletion protection annotation %s. Remove it to proceed the delete operation", lvg.Name, internal.DeletionProtectionAnnotation))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(
			ctx,
			lvg,
			v1.ConditionFalse,
			internal.TypeVGConfigurationApplied,
			internal.ReasonTerminating,
			fmt.Sprintf("to delete the LVG remove the annotation %s", internal.DeletionProtectionAnnotation),
		)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return false, nil
	}

	// Whether the Volume Group on the node is this resource's to remove is decided
	// before anything is removed. When it is not, the resource still goes away — it
	// is a leftover and the operator asked for it to be deleted — but the storage
	// stays and the condition says so.
	if why, allowed := r.vgRemovalAllowed(lvg); !allowed {
		r.log.Warning(fmt.Sprintf("[reconcileLVGDeleteFunc] not removing VG %s while deleting the LVMVolumeGroup %s: %s",
			lvg.Spec.ActualVGNameOnTheNode, lvg.Name, why))
		if err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating,
			fmt.Sprintf("removing the resource without touching VG %s: %s", lvg.Spec.ActualVGNameOnTheNode, why)); err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}
		return r.finishLVGDeletion(ctx, lvg)
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] check if VG %s of the LVMVolumeGroup %s uses LVs", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	usedLVs := r.getLVForVG(lvg.Spec.ActualVGNameOnTheNode)
	if len(usedLVs) > 0 {
		err := fmt.Errorf("VG %s uses LVs: %v. Delete used LVs first", lvg.Spec.ActualVGNameOnTheNode, usedLVs)
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to reconcile LVG %s", lvg.Name))
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] tries to add the condition %s status False to the LVMVolumeGroup %s due to LV does exist", internal.TypeVGConfigurationApplied, lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}

		return true, nil
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] VG %s of the LVMVolumeGroup %s does not use any LV. Start to delete the VG", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	// The vgremove failure is what this function has to report, so it is kept in a
	// variable of its own: assigning the condition write's result over it hands the
	// caller a nil error whenever the write succeeds, which is every ordinary run
	// of this branch.
	if vgErr := r.deleteVGIfExist(lvg.Spec.ActualVGNameOnTheNode); vgErr != nil {
		r.log.Error(vgErr, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete VG %s", lvg.Spec.ActualVGNameOnTheNode))
		if condErr := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, vgErr.Error()); condErr != nil {
			r.log.Error(condErr, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, condErr
		}

		return true, vgErr
	}

	return r.finishLVGDeletion(ctx, lvg)
}

// finishLVGDeletion is everything the delete path does once the question of the
// Volume Group is settled: clean up the backing files this resource owns, drop the
// finalizer, remove the resource.
//
// Shared by the two ways of getting here — the Volume Group was removed, or it was
// somebody else's and deliberately left alone — because the resource has to go away
// either way, with its own file devices cleaned up and nothing of anyone else's
// touched. cleanupFileDevices is safe on the second path by construction: it only
// acts on paths whose basename carries this LVMVolumeGroup's name.
func (r *Reconciler) finishLVGDeletion(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	if err := r.cleanupFileDevices(ctx, lvg); err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to clean up file devices for the LVMVolumeGroup %s", lvg.Name))
		condErr := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if condErr != nil {
			r.log.Error(condErr, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}

	removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}

	if removed {
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully removed a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[reconcileLVGDeleteFunc] no need to remove a finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
	}

	err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGDeleteFunc] unable to delete the LVMVolumeGroup %s", lvg.Name))
		return true, err
	}

	r.resetAliasResolveFailure(lvg.Name)
	r.resetNoProgressRetries(lvg.Name)
	r.log.Info(fmt.Sprintf("[reconcileLVGDeleteFunc] successfully reconciled VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	return false, nil
}

func (r *Reconciler) reconcileLVGUpdateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (time.Duration, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	pvs, _ := r.sdsCache.GetPVs()
	valid, reason, fdIssues := r.validateLVGForUpdateFunc(ctx, lvg, blockDevices)
	if !valid {
		r.log.Warning(fmt.Sprintf("[reconcileLVGUpdateFunc] the LVMVolumeGroup %s is not valid", lvg.Name))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, lvg.Name))
		}

		return r.requeueInterval(), err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to get VG %s for the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))
	found, vg := r.tryGetVG(lvg.Spec.ActualVGNameOnTheNode)
	if !found {
		err := fmt.Errorf("VG %s not found", lvg.Spec.ActualVGNameOnTheNode)
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGNotFound", err.Error())
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return r.requeueInterval(), err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] VG %s found for the LVMVolumeGroup %s", vg.VGName, lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to check and update VG %s tag %s", lvg.Spec.ActualVGNameOnTheNode, internal.LVMTags[0]))
	updated, err := r.updateVGTagIfNeeded(ctx, lvg, vg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGUpdateFailed", fmt.Sprintf("unable to update VG tag, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return r.requeueInterval(), err
	}

	if updated {
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully updated VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] no need to update VG %s tag of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to resize PV of the LVMVolumeGroup %s", lvg.Name))
	err = r.resizePVIfNeeded(ctx, lvg)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to resize PV of the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "PVResizeFailed", fmt.Sprintf("unable to resize PV, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return r.requeueInterval(), err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the resize operation for PV of the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	err = r.extendVGIfNeeded(ctx, lvg, vg, pvs, blockDevices)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to extend VG of the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGExtendFailed", fmt.Sprintf("unable to extend VG, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return r.requeueInterval(), err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the extend operation for VG of the LVMVolumeGroup %s", lvg.Name))

	// Problems that concern individual spec.fileDevices entries are collected
	// here instead of aborting the reconcile. The Volume Group is intact in every
	// one of them, so the rest of the work — extending by the entries that ARE
	// usable, growing the thin-pools — has to go ahead, and the entries left
	// behind are named on the condition at the end under a reason that keeps the
	// LVMVolumeGroup in service. See fileDevicesUnappliedError.
	fdUnapplied := strings.Builder{}
	fdUnappliedReason := ""
	// First reason wins, not the last. Growth is attempted before the extend, and
	// the extend's own reason is the generic ReasonUpdating in the ordinary "retry
	// next round" case — letting it overwrite would replace the specific
	// ReasonFileDeviceGrowFailed with a label that says nothing. Every message is
	// appended regardless, so nothing is hidden either way; only the reason the
	// condition carries is decided here.
	noteUnapplied := func(msg, reason string) {
		fdUnapplied.WriteString(msg)
		if reason != "" && fdUnappliedReason == "" {
			fdUnappliedReason = reason
		}
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to grow file devices of the LVMVolumeGroup %s", lvg.Name))
	growMsg, growReason, err := splitUnappliedFileDevices(r.growFileDevicesIfNeeded(ctx, lvg, vg, fdIssues))
	if err != nil {
		// A growth that did not go through never reaches here: it comes back
		// wrapped as a per-entry problem carrying ReasonFileDeviceGrowFailed and is
		// reported at the end of the reconcile, because every step of the sequence
		// fails towards the smaller size — the Volume Group is still the size it
		// was and still serving every volume on it.
		//
		// What reaches here is the reconcile itself being unable to continue: the
		// context was cancelled, or the condition write that marks the
		// LVMVolumeGroup as updating failed. Neither says anything about the file
		// devices, and neither can be written to the resource — under a cancelled
		// context the write fails too — so requeue and let the next round diagnose
		// it instead of labelling it a grow failure.
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to grow file devices of the LVMVolumeGroup %s", lvg.Name))
		return r.requeueInterval(), err
	}
	noteUnapplied(growMsg, growReason)
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the file-device grow operation for the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to extend VG %s of the LVMVolumeGroup %s with file devices", vg.VGName, lvg.Name))
	extendMsg, extendReason, err := splitUnappliedFileDevices(r.extendFileDevicesIfNeeded(ctx, lvg, vg, pvs, fdIssues))
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to extend VG of the LVMVolumeGroup %s with file devices", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGExtendFailed", fmt.Sprintf("unable to extend VG with file devices, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return r.requeueInterval(), err
	}
	noteUnapplied(extendMsg, extendReason)
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully ended the file-device extend operation for VG of the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] starts to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
		lvs, _ := r.sdsCache.GetLVs()
		err = r.reconcileThinPoolsIfNeeded(ctx, lvg, vg, lvs)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to reconcile thin-pools of the LVMVolumeGroup %s", lvg.Name))
			err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolReconcileFailed", fmt.Sprintf("unable to reconcile thin-pools, err: %s", err.Error()))
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			}
			return r.requeueInterval(), err
		}
		r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled thin-pools operation of the LVMVolumeGroup %s", lvg.Name))
	}

	// Everything that could be applied has been. Report the parts that could
	// not — unusable spec.fileDevices entries, entries the node could not bring
	// up, and entries dropped from the spec while their PV is still in the VG —
	// without having skipped the rest of the reconcile over them, so the
	// LVMVolumeGroup stays manageable and in service while the admin fixes it.
	if pending := joinFileDeviceIssues(fdIssues.reason, fdUnapplied.String(), r.fileDeviceDriftReason(lvg)); pending != "" {
		r.log.Warning(fmt.Sprintf("[reconcileLVGUpdateFunc] the LVMVolumeGroup %s has unapplied file devices: %s", lvg.Name, pending))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, fileDeviceConditionReason(fdIssues.reason, fdUnapplied.String(), fdUnappliedReason), pending)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return r.requeueInterval(), err
		}
		// A malformed entry and drift both wait for a human, and an edit produces
		// its own event — requeueing would only spin. An entry the node could not
		// bring up is different: the filesystem may gain room, the resolver may
		// recover, and nothing else will wake the reconcile when it does — but the
		// interval has to back off, because the same state also covers an entry that
		// will never fit here. See noProgressRequeueAfter.
		if fdUnapplied.Len() > 0 {
			return r.noProgressRequeueAfter(lvg.Name), nil
		}
		r.resetNoProgressRetries(lvg.Name)
		return 0, nil
	}
	r.resetNoProgressRetries(lvg.Name)

	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] tries to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
	err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "configuration has been applied")
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGUpdateFunc] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		return r.requeueInterval(), err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully added a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
	r.log.Info(fmt.Sprintf("[reconcileLVGUpdateFunc] successfully reconciled the LVMVolumeGroup %s", lvg.Name))

	return 0, nil
}

// noProgressRequeueAfter returns how long to wait before retrying an
// LVMVolumeGroup whose reconcile made no progress, and records that this round
// did not either.
//
// Two callers, one shape. A spec.fileDevices entry the node could not bring up: a
// filesystem briefly short of room clears in seconds and deserves the scan
// interval, while an entry asking for more than the node will ever have — a
// fat-fingered `size`, or a template rolled out by an LVMVolumeGroupSet to a fleet
// — never clears. A node whose Volume Groups cannot be read at all
// (ReasonVGCheckFailed): a transient nsenter failure clears, a missing lvm.static
// does not.
//
// At a fixed 5s the permanent variants have every affected node running a
// `stat -f`, a `losetup -j` per entry and a live `lvm pvs`/`vgs` forever over a
// question whose answer cannot change until somebody intervenes. Nothing
// distinguishes them from the inside, so the interval doubles until it reaches a
// ceiling: the transient case still recovers in one or two rounds, and the
// permanent one settles into a poll that costs nothing to leave running.
//
// The ceiling matters more than the growth rate. It has to stay well under
// controller-runtime's resync so the LVMVolumeGroup is still retried on its own,
// and short enough that an operator who frees space or fixes the node does not
// conclude nothing is happening.
func (r *Reconciler) noProgressRequeueAfter(lvgName string) time.Duration {
	base := r.requeueInterval()

	r.noProgressRetriesMu.Lock()
	r.noProgressRetries[lvgName]++
	streak := r.noProgressRetries[lvgName]
	r.noProgressRetriesMu.Unlock()

	// Shift rather than multiply, and cap the shift before it is applied: a streak
	// on a long-lived LVMVolumeGroup grows without bound, and 1<<64 is not a large
	// interval, it is zero.
	shift := min(streak-1, noProgressRetryMaxShift)
	backoff := base << shift
	if backoff <= 0 || backoff > noProgressRetryMaxInterval {
		backoff = noProgressRetryMaxInterval
	}
	// A SCAN_INTERVAL longer than the ceiling is a deliberate choice to poll the
	// node rarely, and the clamp above must not quietly override it into polling
	// more often than the operator asked for.
	if backoff < base {
		backoff = base
	}
	r.log.Debug(fmt.Sprintf("[noProgressRequeueAfter] the LVMVolumeGroup %s has made no progress for %d consecutive round(s); retrying in %s", lvgName, streak, backoff))
	return backoff
}

// resetNoProgressRetries clears the backoff after a round that made progress, so a
// transient shortage or an unreadable node does not leave the LVMVolumeGroup on a
// long interval once it is over.
func (r *Reconciler) resetNoProgressRetries(lvgName string) {
	r.noProgressRetriesMu.Lock()
	defer r.noProgressRetriesMu.Unlock()
	delete(r.noProgressRetries, lvgName)
}

func (r *Reconciler) reconcileLVGCreateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, error) {
	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] starts to reconcile the LVMVolumeGroup %s", lvg.Name))

	// this check prevents the LVMVolumeGroup resource's infinity updating after a retry
	exist := false
	for _, c := range lvg.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			exist = true
			break
		}
	}

	if !exist {
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonCreating, "trying to apply the configuration")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to add the condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			return true, err
		}
	}

	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to validate the LVMVolumeGroup %s", lvg.Name))
	valid, reason := r.validateLVGForCreateFunc(ctx, lvg, blockDevices)
	if !valid {
		r.log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] validation fails for the LVMVolumeGroup %s", lvg.Name))
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonValidationFailed, reason)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}

		return true, err
	}
	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully validated the LVMVolumeGroup %s", lvg.Name))

	r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] tries to create VG for the LVMVolumeGroup %s", lvg.Name))
	err := r.createVGComplex(ctx, lvg, blockDevices)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create VG for the LVMVolumeGroup %s", lvg.Name))
		err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "VGCreationFailed", fmt.Sprintf("unable to create VG, err: %s", err.Error()))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		}
		return true, err
	}
	r.log.Info(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created VG for the LVMVolumeGroup %s", lvg.Name))

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] the LVMVolumeGroup %s has thin-pools. Tries to create them", lvg.Name))

		var vgAfterCreate *internal.VGData
		if freshVG, _, _, getErr := r.commands.GetVG(lvg.Spec.ActualVGNameOnTheNode); getErr == nil {
			v := freshVG
			vgAfterCreate = &v
		} else {
			r.log.Warning(fmt.Sprintf("[reconcileLVGCreateFunc] unable to get VG %s after creation, will use default extent size for thin-pool alignment: %v", lvg.Spec.ActualVGNameOnTheNode, getErr))
		}
		extentForThinPools := extentSizeForThinPoolAlign(lvg, vgAfterCreate)

		for _, tp := range lvg.Spec.ThinPools {
			// vgSize must account for file-backed PVs too: a file-only VG has
			// no block devices, so countVGSizeByBlockDevices alone returns 0,
			// which collapses every percentage thin-pool to 0 and forces every
			// absolute-sized thin-pool into the full-VG-space branch (taking the
			// whole VG instead of the requested size). Add the spec.fileDevices
			// capacity, mirroring how validateLVGForCreateFunc computes totalVGSize.
			vgSize := countVGSizeByBlockDevices(blockDevices)
			vgSize.Add(countVGSizeByFileDevices(lvg))
			tpRequestedSize, err := utils.GetRequestedSizeFromString(tp.Size, vgSize)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to get thin-pool %s requested size of the LVMVolumeGroup %s", tp.Name, lvg.Name))
				return false, err
			}

			var cmd string
			alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentForThinPools)
			if alignErr != nil {
				r.log.Error(alignErr, fmt.Sprintf("[reconcileLVGCreateFunc] unable to align thin-pool %s size for LVMVolumeGroup %s", tp.Name, lvg.Name))
				return false, alignErr
			}
			if alignedTpSize.Value() >= vgSize.Value() {
				r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] Thin-pool %s of the LVMVolumeGroup %s will be created with full VG space size", tp.Name, lvg.Name))
				cmd, err = r.commands.CreateThinPoolFullVGSpace(tp.Name, lvg.Spec.ActualVGNameOnTheNode)
			} else {
				r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] Thin-pool %s of the LVMVolumeGroup %s will be created with size %s", tp.Name, lvg.Name, alignedTpSize.String()))
				cmd, err = r.commands.CreateThinPool(tp.Name, lvg.Spec.ActualVGNameOnTheNode, alignedTpSize.Value())
			}
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[reconcileLVGCreateFunc] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", tp.Name, lvg.Name, cmd))
				err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, "ThinPoolCreationFailed", fmt.Sprintf("unable to create thin-pool, err: %s", err.Error()))
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
				}

				return true, err
			}
		}
		r.log.Debug(fmt.Sprintf("[reconcileLVGCreateFunc] successfully created thin-pools for the LVMVolumeGroup %s", lvg.Name))
	}

	err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionTrue, internal.TypeVGConfigurationApplied, internal.ReasonApplied, "all configuration has been applied")
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
		return true, err
	}

	return false, nil
}

func (r *Reconciler) shouldUpdateLVGLabels(lvg *v1alpha1.LVMVolumeGroup, labelKey, labelValue string) bool {
	if lvg.Labels == nil {
		r.log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has no labels.", lvg.Name))
		return true
	}

	val, exist := lvg.Labels[labelKey]
	if !exist {
		r.log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has no label %s.", lvg.Name, labelKey))
		return true
	}

	if val != labelValue {
		r.log.Debug(fmt.Sprintf("[shouldUpdateLVGLabels] the LVMVolumeGroup %s has label %s but the value is incorrect - %s (should be %s)", lvg.Name, labelKey, val, labelValue))
		return true
	}

	return false
}

func (r *Reconciler) shouldLVGWatcherReconcileUpdateEvent(oldLVG, newLVG *v1alpha1.LVMVolumeGroup) bool {
	if !checkIfLVGBelongsToNode(newLVG, r.cfg.NodeName) {
		return false
	}

	if newLVG.DeletionTimestamp != nil {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s has deletionTimestamp", newLVG.Name))
		return true
	}

	for _, c := range newLVG.Status.Conditions {
		if c.Type == internal.TypeVGConfigurationApplied {
			if c.Reason == internal.ReasonUpdating || c.Reason == internal.ReasonCreating {
				r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should not be reconciled as the LVMVolumeGroup %s reconciliation still in progress", newLVG.Name))
				return false
			}
		}
	}

	if _, exist := newLVG.Labels[internal.LVGUpdateTriggerLabel]; exist {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s has the label %s", newLVG.Name, internal.LVGUpdateTriggerLabel))
		return true
	}

	if r.shouldUpdateLVGLabels(newLVG, internal.LVGMetadataNameLabelKey, newLVG.Name) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup's %s labels have been changed", newLVG.Name))
		return true
	}

	if !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s configuration has been changed", newLVG.Name))
		return true
	}

	if hasStatusNodesDiff(r.log, oldLVG.Status.Nodes, newLVG.Status.Nodes) {
		r.log.Debug(fmt.Sprintf("[shouldLVGWatcherReconcileUpdateEvent] update event should be reconciled as the LVMVolumeGroup %s status nodes have changed", newLVG.Name))
		return true
	}

	return false
}

func (r *Reconciler) addLVGFinalizerIfNotExist(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	if slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	lvg.Finalizers = append(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer)
	err := r.cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *Reconciler) syncThinPoolsAllocationLimit(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) error {
	updated := false

	tpSpecLimits := make(map[string]string, len(lvg.Spec.ThinPools))
	for _, tp := range lvg.Spec.ThinPools {
		tpSpecLimits[tp.Name] = tp.AllocationLimit
	}

	var (
		space resource.Quantity
		err   error
	)
	for i := range lvg.Status.ThinPools {
		if specLimits, matched := tpSpecLimits[lvg.Status.ThinPools[i].Name]; matched {
			if lvg.Status.ThinPools[i].AllocationLimit != specLimits {
				r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] thin-pool %s status AllocationLimit: %s of the LVMVolumeGroup %s should be updated by spec one: %s", lvg.Status.ThinPools[i].Name, lvg.Status.ThinPools[i].AllocationLimit, lvg.Name, specLimits))
				updated = true
				lvg.Status.ThinPools[i].AllocationLimit = specLimits

				space, err = utils.GetThinPoolAvailableSpace(lvg.Status.ThinPools[i].ActualSize, lvg.Status.ThinPools[i].AllocatedSize, specLimits)
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[syncThinPoolsAllocationLimit] unable to get thin pool %s available space", lvg.Status.ThinPools[i].Name))
					return err
				}
				r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] successfully got a new available space %s of the thin-pool %s", space.String(), lvg.Status.ThinPools[i].Name))
				lvg.Status.ThinPools[i].AvailableSpace = space
			}
		} else {
			r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] status thin-pool %s of the LVMVolumeGroup %s was not found as used in spec", lvg.Status.ThinPools[i].Name, lvg.Name))
		}
	}

	if updated {
		r.log.Trace(fmt.Sprintf("[syncThinPoolsAllocationLimit] LVMVolumeGroup %s ThinPools: %+v", lvg.Name, lvg.Status.ThinPools))
		r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] tries to update the LVMVolumeGroup %s", lvg.Name))
		err = r.cl.Status().Update(ctx, lvg)
		if err != nil {
			return err
		}
		r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] successfully updated the LVMVolumeGroup %s", lvg.Name))
	} else {
		r.log.Debug(fmt.Sprintf("[syncThinPoolsAllocationLimit] every status thin-pool AllocationLimit value is synced with spec one for the LVMVolumeGroup %s", lvg.Name))
	}

	return nil
}

// deleteLVGIfNeeded handles the deletion of an LVMVolumeGroup whose Volume Group
// was never created.
//
// deleted says the resource is gone and the reconcile is over. waitForCache says
// the opposite of "nothing to do": the node has the Volume Group and the cache
// does not, so no delete decision can be made from the data at hand and the
// caller must requeue without running any of them.
func (r *Reconciler) deleteLVGIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (deleted, waitForCache bool, err error) {
	if lvg.DeletionTimestamp == nil {
		return false, false, nil
	}

	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(lvg.Spec.ActualVGNameOnTheNode, vgs) {
		// The cache alone is not enough to conclude the Volume Group was never
		// created, and here that conclusion is destructive rather than merely
		// wrong: this branch removes backing files and the finalizer, so a cache
		// that has simply not caught up would delete the storage of a live VG and
		// then delete the record of it. The cache is filled only on udev events,
		// and writing LVM metadata to a loop device does not reliably raise one —
		// which is exactly why shouldReconcileLVGByCreateFunc confirms against
		// LVM before it commits to the create path. The same confirmation belongs
		// here, for a stronger reason: create wedges a condition, this loses data.
		//
		// When the VG turns out to exist (or cannot be ruled out), the whole
		// reconcile stops here and is requeued. Falling through to the ordinary
		// delete path would not be a safer route to the same place: every step of
		// it reads the same stale cache. getLVForVG finds no logical volumes and
		// waves the resource past the "delete used LVs first" guard, deleteVGIfExist
		// finds no Volume Group and reports success without doing anything, and for
		// an LVMVolumeGroup with no file devices cleanupFileDevices has nothing to
		// walk — so the finalizer comes off and the resource is deleted while its
		// Volume Group, and whatever is on it, stays on the node with no owner. Only
		// a file-backed group was ever saved from that, and by cleanupFileDevices'
		// own live `pvs` check rather than by this branch.
		//
		// The two ways this can answer "do not delete" need different words, exactly
		// as they do in runEventReconcile. "The cache has not caught up" clears
		// itself and is an acceptable reason, so the LVMVolumeGroup stays Ready
		// while it waits. "LVM could not be read at all" does not clear itself, and
		// an LVMVolumeGroup whose storage the agent has lost sight of must not keep
		// reporting Ready — ReasonVGCheckFailed is deliberately absent from the
		// conditions watcher's acceptableReasons for that reason. Reporting the
		// second as the first also sends the operator to look at the scanner while
		// the actual complaint is nsenter or lvm.static.
		exists, vgStateKnown := r.vgExistsOnNode(ctx, lvg.Spec.ActualVGNameOnTheNode)
		if exists {
			reason, msg := internal.ReasonCacheStale, fmt.Sprintf("VG %s is present on the node but missing from the agent's cache; the LVMVolumeGroup is not deleted until the cache catches up", lvg.Spec.ActualVGNameOnTheNode)
			if !vgStateKnown {
				reason, msg = internal.ReasonVGCheckFailed, fmt.Sprintf("unable to read the Volume Groups of the node to decide whether VG %s still exists; the LVMVolumeGroup is not deleted while this is unknown", lvg.Spec.ActualVGNameOnTheNode)
			}
			r.log.Warning(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] %s (LVMVolumeGroup %s)", msg, lvg.Name))
			if condErr := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, reason, msg); condErr != nil {
				r.log.Error(condErr, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, reason, lvg.Name))
			}
			return false, true, nil
		}

		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] VG %s was not yet created for the LVMVolumeGroup %s and the resource is marked as deleting. Delete the resource", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))

		// "No Volume Group" does not mean "nothing on the node". File devices are
		// provisioned before the VG is assembled, and when pvcreate succeeded but
		// vgcreate did not, rollbackProvisionedFileDevices deliberately keeps the
		// loop and its backing file — tearing down something that is already a PV
		// is how a live VG gets corrupted. The resource is then the only record of
		// what was left behind, so removing its finalizer without cleaning up
		// strands a preallocated file, a loop minor and an orphan PV that nothing
		// will ever collect: the reconciler is gone with the resource and the
		// discoverer has no VG to import. Deleting a stuck VGCreationFailed
		// LVMVolumeGroup is exactly what an operator does, and repeating it fills
		// the node.
		//
		// The call is free for a block-device-only LVMVolumeGroup (neither spec nor
		// status names a file device, so it walks an empty set) and refuses to act
		// on any path outside the managed naming pattern.
		if err := r.cleanupFileDevices(ctx, lvg); err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to clean up file devices of the LVMVolumeGroup %s; keeping the finalizer so the resource stays the record of what is on the node", lvg.Name))
			if condErr := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonTerminating, err.Error()); condErr != nil {
				r.log.Error(condErr, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to add a condition %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, lvg.Name))
			}
			return false, false, err
		}

		removed, err := r.removeLVGFinalizerIfExist(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
			return false, false, err
		}

		if removed {
			r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully removed the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		} else {
			r.log.Debug(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] no need to remove the finalizer %s from the LVMVolumeGroup %s", internal.SdsNodeConfiguratorFinalizer, lvg.Name))
		}

		err = r.lvgCl.DeleteLVMVolumeGroup(ctx, lvg)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[RunLVMVolumeGroupWatcherController] unable to delete the LVMVolumeGroup %s", lvg.Name))
			return false, false, err
		}
		// Same bookkeeping as the other delete path: the resource is gone, so
		// neither its resolver-failure streak nor its file-device retry backoff may
		// outlive it and greet a future LVMVolumeGroup of the same name mid-escalation.
		r.resetAliasResolveFailure(lvg.Name)
		r.resetNoProgressRetries(lvg.Name)
		r.log.Info(fmt.Sprintf("[RunLVMVolumeGroupWatcherController] successfully deleted the LVMVolumeGroup %s", lvg.Name))
		return true, false, nil
	}
	return false, false, nil
}

func (r *Reconciler) validateLVGForCreateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, string) {
	reason := strings.Builder{}

	r.log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] check if every selected BlockDevice of the LVMVolumeGroup %s is consumable", lvg.Name))
	// totalVGSize needs to count if there is enough space for requested thin-pools
	totalVGSize := countVGSizeByBlockDevices(blockDevices)
	for _, bd := range blockDevices {
		if !bd.Status.Consumable {
			r.log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice %s is not consumable", bd.Name))
			r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] BlockDevice name: %s, status: %+v", bd.Name, bd.Status))
			reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
		}
	}

	if reason.Len() == 0 {
		r.log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] all BlockDevices of the LVMVolumeGroup %s are consumable", lvg.Name))
	}

	r.validateFileDevices(ctx, lvg, &reason, &totalVGSize)

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools. Validate if VG size has enough space for the thin-pools", lvg.Name))
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] the LVMVolumeGroup %s has thin-pools %v", lvg.Name, lvg.Spec.ThinPools))
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total LVMVolumeGroup %s size: %s", lvg.Name, totalVGSize.String()))

		var totalThinPoolSize int64
		for _, tp := range lvg.Spec.ThinPools {
			tpRequestedSize, err := utils.GetRequestedSizeFromString(tp.Size, totalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", tp.Name))
				continue
			}

			alignedTpSize, alignErr := alignThinPoolSizeForValidation(tp.Size, tpRequestedSize, extentSizeForThinPoolAlign(lvg, nil))
			if alignErr != nil {
				reason.WriteString(fmt.Sprintf("Unable to align thin-pool %s size: %s. ", tp.Name, alignErr.Error()))
				continue
			}
			if alignedTpSize.Value() >= totalVGSize.Value() {
				if len(lvg.Spec.ThinPools) > 1 {
					reason.WriteString(fmt.Sprintf("Thin-pool %s requested size of full VG space, but there is any other thin-pool. ", tp.Name))
				}
			}

			totalThinPoolSize += alignedTpSize.Value()
		}
		r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] LVMVolumeGroup %s thin-pools requested space: %d", lvg.Name, totalThinPoolSize))

		if totalThinPoolSize > totalVGSize.Value() {
			r.log.Trace(fmt.Sprintf("[validateLVGForCreateFunc] total thin pool size: %s, total vg size: %s", resource.NewQuantity(totalThinPoolSize, resource.BinarySI).String(), totalVGSize.String()))
			r.log.Warning(fmt.Sprintf("[validateLVGForCreateFunc] requested thin pool size is more than VG total size for the LVMVolumeGroup %s", lvg.Name))
			reason.WriteString(fmt.Sprintf("Required space for thin-pools %d is more than VG size %d.", totalThinPoolSize, totalVGSize.Value()))
		}
	}

	if reason.Len() != 0 {
		return false, reason.String()
	}

	return true, ""
}

func (r *Reconciler) validateLVGForUpdateFunc(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	blockDevices map[string]v1alpha1.BlockDevice,
) (bool, string, fileDeviceIssues) {
	reason := strings.Builder{}
	var issues fileDeviceIssues

	// Bail out before any name-keyed cache lookups when the underlying VG name
	// is ambiguous on the node. Without this, FindVG/FindLV would return data
	// from an arbitrary VG of the same name and produce misleading reasons
	// (e.g. "Added thin-pools requested sizes are more than allowed free space
	// in VG") that hide the real problem.
	allVGs, _ := r.sdsCache.GetVGs()
	if duplicateVGs := findDuplicateVGNames(allVGs); len(duplicateVGs) > 0 {
		if uuids, dup := duplicateVGs[lvg.Spec.ActualVGNameOnTheNode]; dup {
			return false, duplicateVGMessage(lvg.Spec.ActualVGNameOnTheNode, uuids), issues
		}
	}

	pvs, _ := r.sdsCache.GetPVs()
	r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] check if every new BlockDevice of the LVMVolumeGroup %s is comsumable", lvg.Name))
	actualPVPaths := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		actualPVPaths[pv.PVName] = struct{}{}
	}

	//TODO: add a check if BlockDevice size got less than PV size

	// Check if added BlockDevices are consumable
	// additionBlockDeviceSpace value is needed to count if VG will have enough space for thin-pools
	var additionBlockDeviceSpace int64
	for _, bd := range blockDevices {
		if _, found := actualPVPaths[bd.Status.Path]; !found {
			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] unable to find the PV %s for BlockDevice %s. Check if the BlockDevice is already used", bd.Status.Path, bd.Name))
			for _, n := range lvg.Status.Nodes {
				for _, d := range n.Devices {
					if d.BlockDevice == bd.Name {
						r.log.Warning(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s misses the PV %s. That might be because the corresponding device was removed from the node. Unable to validate BlockDevices", bd.Name, bd.Status.Path))
						reason.WriteString(fmt.Sprintf("BlockDevice %s misses the PV %s (that might be because the device was removed from the node). ", bd.Name, bd.Status.Path))
					}

					if reason.Len() == 0 {
						r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s does not miss a PV", d.BlockDevice))
					}
				}
			}

			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] PV %s for BlockDevice %s of the LVMVolumeGroup %s is not created yet, check if the BlockDevice is consumable", bd.Status.Path, bd.Name, lvg.Name))
			if reason.Len() > 0 {
				r.log.Debug("[validateLVGForUpdateFunc] some BlockDevices misses its PVs, unable to check if they are consumable")
				continue
			}

			if !bd.Status.Consumable {
				reason.WriteString(fmt.Sprintf("BlockDevice %s is not consumable. ", bd.Name))
				continue
			}

			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] BlockDevice %s is consumable", bd.Name))
			additionBlockDeviceSpace += bd.Status.Size.Value()
		}
	}

	// File-device problems are collected apart from `reason`: they must not make
	// the whole LVMVolumeGroup invalid on the update path. See validateFileDevices.
	fdReason := strings.Builder{}
	invalidFileDevices := r.validateFileDevices(ctx, lvg, &fdReason, nil)
	issues = fileDeviceIssues{reason: fdReason.String(), invalid: invalidFileDevices}

	// additionFileDeviceSpace mirrors additionBlockDeviceSpace for file
	// devices: it accounts for spec.fileDevices entries that are not yet PVs
	// in the VG (i.e. not yet reflected in status.nodes[].fileDevices). Without
	// it, a combined "append a fileDevices entry + grow a thin-pool" edit would
	// be validated against the current VG size, wrongly rejecting a valid
	// request or forcing an absolute-sized thin-pool into the full-VG-space
	// branch — the same defect the create path avoids via countVGSizeByFileDevices.
	var additionFileDeviceSpace int64
	if len(lvg.Spec.FileDevices) > 0 {
		// Match by basename, not full path: status.nodes[].fileDevices[].FilePath
		// is the loop's backing file as reported by `losetup --output BACK-FILE`,
		// which canonicalizes symlink components of the directory (e.g. a spec
		// directory /data symlinked to /mnt/disk1/data is reported as
		// /mnt/disk1/data/...), while BuildFileDevicePath keeps the literal spec
		// directory. The basename `sds-<lvgName>.<entryName>.img` is identical on
		// both sides (it is built from the names alone and losetup leaves the
		// basename untouched), so a full-path compare would
		// miss an already-provisioned device whenever the directory is a symlink
		// and count it as new on every reconcile, inflating the VG size used for
		// thin-pool validation.
		//
		// Only this node's devices. A Local Volume Group has exactly one entry in
		// status.nodes, but the field is a list and the type may one day be Shared,
		// and another node's capacity is not this Volume Group's on this node.
		existingPVSize := make(map[string]int64)
		for _, n := range lvg.Status.Nodes {
			if n.Name != r.cfg.NodeName {
				continue
			}
			for _, fd := range n.FileDevices {
				existingPVSize[filepath.Base(fd.FilePath)] = fd.Size.Value()
			}
		}
		extentQuantity := extentSizeForThinPoolAlign(lvg, nil)
		extentSize := extentQuantity.Value()
		for _, fd := range lvg.Spec.FileDevices {
			if _, bad := invalidFileDevices[fd.Name]; bad {
				continue
			}
			base := filepath.Base(utils.BuildFileDevicePath(fd.Directory, lvg.Name, fd.Name))
			pvSize, provisioned := existingPVSize[base]
			if !provisioned {
				additionFileDeviceSpace += fd.Size.Value()
				continue
			}
			// An entry whose size was raised will grow in place, so the capacity
			// it is about to add counts too — otherwise "grow the file device and
			// grow the thin-pool" in one edit is validated against the pre-growth
			// VG size and wrongly rejected, the same way appending an entry used
			// to be. One extent is left out of the estimate: that is roughly what
			// LVM keeps for PV metadata, so counting the full delta would promise
			// space the VG never actually gains.
			if gain := fd.Size.Value() - pvSize - extentSize; gain > 0 {
				additionFileDeviceSpace += gain
			}
		}
	}

	if lvg.Spec.ThinPools != nil {
		r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s has thin-pools. Validate them", lvg.Name))
		actualThinPools := make(map[string]internal.LVData, len(lvg.Spec.ThinPools))
		for _, tp := range lvg.Spec.ThinPools {
			lv := r.sdsCache.FindLV(lvg.Spec.ActualVGNameOnTheNode, tp.Name)
			if lv != nil {
				if !isThinPool(lv.Data) {
					reason.WriteString(fmt.Sprintf("LV %s is already created on the node and it is not a thin-pool", lv.Data.LVName))
					continue
				}

				actualThinPools[lv.Data.LVName] = lv.Data
			}
		}

		// check if added thin-pools has valid requested size
		var (
			addingThinPoolSize int64
			hasFullThinPool    = false
		)

		vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
		if vg == nil {
			reason.WriteString(fmt.Sprintf("Missed VG %s in the cache", lvg.Spec.ActualVGNameOnTheNode))
			return false, reason.String(), issues
		}

		newTotalVGSize := resource.NewQuantity(vg.VGSize.Value()+additionBlockDeviceSpace+additionFileDeviceSpace, resource.BinarySI)
		for _, specTp := range lvg.Spec.ThinPools {
			// might be a case when Thin-pool is already created, but is not shown in status
			tpRequestedSize, err := utils.GetRequestedSizeFromString(specTp.Size, *newTotalVGSize)
			if err != nil {
				reason.WriteString(err.Error())
				continue
			}

			if tpRequestedSize.Value() == 0 {
				reason.WriteString(fmt.Sprintf("Thin-pool %s has zero size. ", specTp.Name))
				continue
			}

			r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s thin-pool %s requested size %s, Status VG size %s", lvg.Name, specTp.Name, tpRequestedSize.String(), lvg.Status.VGSize.String()))
			alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, vg))
			if alignErr != nil {
				reason.WriteString(fmt.Sprintf("Unable to align thin-pool %s size: %s. ", specTp.Name, alignErr.Error()))
				continue
			}
			if alignedTpSize.Value() >= newTotalVGSize.Value() {
				hasFullThinPool = true
				if len(lvg.Spec.ThinPools) > 1 {
					reason.WriteString(fmt.Sprintf("Thin-pool %s requests size of full VG space, but there are any other thin-pools. ", specTp.Name))
				}
			} else {
				if actualThinPool, created := actualThinPools[specTp.Name]; !created {
					r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is not yet created, adds its requested size", specTp.Name, lvg.Name))
					addingThinPoolSize += alignedTpSize.Value()
				} else {
					r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] thin-pool %s of the LVMVolumeGroup %s is already created, check its requested size", specTp.Name, lvg.Name))
					// LVM rounds a freshly created thin-pool data LV up by up to one extent, so
					// flag a shrink only when the actual pool exceeds the aligned request by MORE
					// than one extent — otherwise the LVG loops "is not valid" forever.
					extentSize := extentSizeForThinPoolAlign(lvg, vg)
					if actualThinPool.LVSize.Value()-alignedTpSize.Value() > extentSize.Value() {
						r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s is less than Status one: %s", lvg.Name, specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						reason.WriteString(fmt.Sprintf("Requested Spec.ThinPool %s size %s is less than actual one %s. ", specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						continue
					}

					thinPoolSizeDiff := alignedTpSize.Value() - actualThinPool.LVSize.Value()
					if thinPoolSizeDiff > 0 {
						r.log.Debug(fmt.Sprintf("[validateLVGForUpdateFunc] the LVMVolumeGroup %s Spec.ThinPool %s size %s more than Status one: %s", lvg.Name, specTp.Name, tpRequestedSize.String(), actualThinPool.LVSize.String()))
						addingThinPoolSize += thinPoolSizeDiff
					}
				}
			}
		}

		if !hasFullThinPool {
			allocatedSize := getVGAllocatedSize(*vg)
			totalFreeSpace := newTotalVGSize.Value() - allocatedSize.Value()
			r.log.Trace(fmt.Sprintf("[validateLVGForUpdateFunc] new LVMVolumeGroup %s thin-pools requested %d size, additional BlockDevices space %d, total: %d", lvg.Name, addingThinPoolSize, additionBlockDeviceSpace, totalFreeSpace))
			if addingThinPoolSize != 0 && addingThinPoolSize > totalFreeSpace {
				reason.WriteString("Added thin-pools requested sizes are more than allowed free space in VG.")
			}
		}
	}

	if reason.Len() != 0 {
		return false, reason.String(), issues
	}

	return true, "", issues
}

// identifyLVGReconcileFunc picks the reconcile path. vgStateKnown is carried out
// alongside it so the "none" caller can report why it got nothing to do without
// paying for a second `vgs`.
func (r *Reconciler) identifyLVGReconcileFunc(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (recType internal.ReconcileType, vgStateKnown bool) {
	shouldCreate, vgStateKnown := r.shouldReconcileLVGByCreateFunc(ctx, lvg)
	if shouldCreate {
		return internal.CreateReconcile, vgStateKnown
	}

	if r.shouldReconcileLVGByUpdateFunc(lvg) {
		return internal.UpdateReconcile, vgStateKnown
	}

	// Deletion is not asked about here. Reconcile dispatches it directly, so a
	// resource carrying a deletionTimestamp never gets this far.
	return "none", vgStateKnown
}

// shouldReconcileLVGByCreateFunc reports whether the Volume Group has to be
// created. vgStateKnown says whether that answer rests on an actual reading of
// the node — it is false only when LVM could not be queried, and it travels out
// so the caller can tell "waiting for the cache" from "cannot see the node".
func (r *Reconciler) shouldReconcileLVGByCreateFunc(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (shouldCreate, vgStateKnown bool) {
	if lvg.DeletionTimestamp != nil {
		return false, true
	}

	if vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode); vg != nil {
		return false, true
	}

	// The cache is filled only by the scanner, and the scanner only runs on udev
	// events. Writing LVM metadata to a loop device does not reliably raise one, so
	// after the agent creates a file-backed VG itself the cache can keep a snapshot
	// taken mid-operation — before vgcreate — with no way to notice. Attaching a
	// real disk raises plenty of events, which is why this never showed up for
	// block-device VGs.
	//
	// Taking the create path on that stale view is destructive rather than merely
	// slow: CreateVGComplex re-runs pvcreate on a device that is already a PV of
	// the very VG it is about to create, LVM refuses with "Can't initialize
	// physical volume ... without -ff", and the LVMVolumeGroup stays Pending with
	// vgSize=0 for good, because nothing will refresh the cache later either.
	//
	// So confirm against LVM before committing to it. This costs one vgs call, and
	// only on the path that would otherwise create a VG.
	exists, known := r.vgExistsOnNode(ctx, lvg.Spec.ActualVGNameOnTheNode)
	return !exists, known
}

// vgExistsOnNode asks LVM directly whether vgName is present.
//
// known distinguishes the two ways this can answer "exists": because LVM said so,
// and because LVM could not be asked. Both must keep the create path away from
// storage that may well be there, but they are different problems with different
// fixes, and a caller that reports one as the other sends the operator looking
// for a Volume Group that does not exist. Callers that only need the safe
// default can ignore it.
//
// A Volume Group that lives entirely on loop devices the agent does not own does
// not count as present. It is not this module's storage, so it is neither
// something to avoid overwriting nor something to reconcile — and taking it for
// ours has no way out: create is refused because "the VG is there", update finds
// nothing in the cache, and the LVMVolumeGroup sits in CacheStale forever while
// the condition tells the operator to wait for a cache that is not the problem.
// Before spec.fileDevices removed `loop` from LVMGlobalFilter such a Volume Group
// was invisible here. See utils/loopvg.go.
func (r *Reconciler) vgExistsOnNode(ctx context.Context, vgName string) (exists, known bool) {
	type vgsResult struct {
		vgs []internal.VGData
		cmd string
	}
	res, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (vgsResult, error) {
		vgs, cmd, _, err := r.commands.GetAllVGs(ctx)
		return vgsResult{vgs: vgs, cmd: cmd}, err
	})
	r.log.Debug(res.cmd)
	if err != nil {
		r.log.Warning(fmt.Sprintf("[vgExistsOnNode] unable to confirm whether VG %s exists; assuming it does so nothing is created over storage that may be there: %v", vgName, err))
		return true, false
	}

	candidates := make([]internal.VGData, 0, 1)
	for _, vg := range res.vgs {
		if vg.VGName == vgName {
			candidates = append(candidates, vg)
		}
	}
	if len(candidates) == 0 {
		return false, true
	}

	// The PV listing is paid for only when the name actually matched, which is the
	// rare branch: this function runs only when the VG is missing from the cache.
	type pvsResult struct {
		pvs []internal.PVData
		cmd string
	}
	pvsRes, pvsErr := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (pvsResult, error) {
		pvs, cmd, _, err := r.commands.GetAllPVs(ctx)
		return pvsResult{pvs: pvs, cmd: cmd}, err
	})
	r.log.Debug(pvsRes.cmd)
	if pvsErr != nil {
		r.log.Warning(fmt.Sprintf("[vgExistsOnNode] VG %s is present on the node but its PVs could not be listed to tell whether it is the module's own; assuming it is so nothing is created over storage that may be there: %v", vgName, pvsErr))
		return true, false
	}

	verdicts := utils.ClassifyLoopVGs(ctx, r.log, r.commands, r.cfg.CmdDeadlineDuration, candidates, pvsRes.pvs)
	for _, vg := range candidates {
		if verdicts.IsUnowned(vg.VGUUID) {
			r.log.Warning(fmt.Sprintf("[vgExistsOnNode] a VG named %s (VG_UUID=%s) is present on the node but lives entirely on loop devices this agent did not create; it is not ours and does not count as existing", vgName, vg.VGUUID))
			continue
		}
		r.log.Warning(fmt.Sprintf("[vgExistsOnNode] VG %s is absent from the cache but present on the node; not re-creating it", vgName))
		return true, true
	}
	return false, true
}

func (r *Reconciler) shouldReconcileLVGByUpdateFunc(lvg *v1alpha1.LVMVolumeGroup) bool {
	if lvg.DeletionTimestamp != nil {
		return false
	}

	vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	return vg != nil
}

func (r *Reconciler) reconcileThinPoolsIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	lvs []internal.LVData,
) error {
	actualThinPools := make(map[string]internal.LVData, len(lvs))
	for _, lv := range lvs {
		if isThinPool(lv) {
			actualThinPools[lv.LVName] = lv
		}
	}

	errs := strings.Builder{}
	for _, specTp := range lvg.Spec.ThinPools {
		tpRequestedSize, err := utils.GetRequestedSizeFromString(specTp.Size, lvg.Status.VGSize)
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to get requested thin-pool %s size of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
			return err
		}

		if actualTp, exist := actualThinPools[specTp.Name]; !exist {
			r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s is not created yet. Create it", specTp.Name, lvg.Name))
			if isApplied(lvg) {
				err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
			}

			var cmd string
			start := time.Now()
			alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, &vg))
			if alignErr != nil {
				r.log.Error(alignErr, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to align thin-pool %s size for LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to align thin-pool %s size, err: %s. ", specTp.Name, alignErr.Error()))
				continue
			}
			if alignedTpSize.Value() >= lvg.Status.VGSize.Value() {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size 100FREE", specTp.Name, lvg.Name))
				cmd, err = r.commands.CreateThinPoolFullVGSpace(specTp.Name, vg.VGName)
			} else {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s will be created with size %s", specTp.Name, lvg.Name, alignedTpSize.String()))
				cmd, err = r.commands.CreateThinPool(specTp.Name, vg.VGName, alignedTpSize.Value())
			}
			r.metrics.UtilsCommandsDuration(ReconcilerName, "lvcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
			r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "lvcreate").Inc()
			if err != nil {
				r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "lvcreate").Inc()
				r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to create thin-pool %s of the LVMVolumeGroup %s, cmd: %s", specTp.Name, lvg.Name, cmd))
				errs.WriteString(fmt.Sprintf("unable to create thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}

			r.log.Info(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] thin-pool %s of the LVMVolumeGroup %s has been successfully created", specTp.Name, lvg.Name))
		} else {
			alignedTpSizeForResize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, &vg))
			if alignErr != nil {
				r.log.Error(alignErr, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to align thin-pool %s size for LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to align thin-pool %s size, err: %s. ", specTp.Name, alignErr.Error()))
				continue
			}
			if actualTp.LVSize.Value() >= alignedTpSizeForResize.Value() {
				r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is equal to actual one", lvg.Name, tpRequestedSize.String()))
				continue
			}

			r.log.Debug(fmt.Sprintf("[ReconcileThinPoolsIfNeeded] the LVMVolumeGroup %s requested thin pool %s size is more than actual one. Resize it", lvg.Name, tpRequestedSize.String()))
			if isApplied(lvg) {
				err = r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
				if err != nil {
					r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
					return err
				}
			}
			err = r.extendThinPool(lvg, specTp)
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[ReconcileThinPoolsIfNeeded] unable to resize thin-pool %s of the LVMVolumeGroup %s", specTp.Name, lvg.Name))
				errs.WriteString(fmt.Sprintf("unable to resize thin-pool %s, err: %s. ", specTp.Name, err.Error()))
				continue
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func (r *Reconciler) resizePVIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) error {
	if len(lvg.Status.Nodes) == 0 {
		r.log.Warning(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s nodes are empty. Wait for the next update", lvg.Name))
		return nil
	}

	vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	extentSize := extentSizeForThinPoolAlign(lvg, vg)

	errs := strings.Builder{}
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			if d.DevSize.Value()-d.PVSize.Value() > extentSize.Value() {
				if isApplied(lvg) {
					err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
					if err != nil {
						r.log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
						return err
					}
				}

				r.log.Debug(fmt.Sprintf("[ResizePVIfNeeded] the LVMVolumeGroup %s BlockDevice %s PVSize is less than actual device size. Resize PV", lvg.Name, d.BlockDevice))

				start := time.Now()
				cmd, err := utils.RunWithTimeout(ctx, r.cfg.CmdDeadlineDuration, func(ctx context.Context) (string, error) {
					return r.commands.ResizePV(ctx, d.Path)
				})
				r.metrics.UtilsCommandsDuration(ReconcilerName, "pvresize").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
				r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvresize").Inc()
				if err != nil {
					r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvresize").Inc()
					r.log.Error(err, fmt.Sprintf("[ResizePVIfNeeded] unable to resize PV %s of BlockDevice %s of LVMVolumeGroup %s, cmd: %s", d.Path, d.BlockDevice, lvg.Name, cmd))
					errs.WriteString(fmt.Sprintf("unable to resize PV %s, err: %s. ", d.Path, err.Error()))
					continue
				}

				r.log.Info(fmt.Sprintf("[ResizePVIfNeeded] successfully resized PV %s of BlockDevice %s of LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			} else {
				r.log.Debug(fmt.Sprintf("[ResizePVIfNeeded] no need to resize PV %s of BlockDevice %s of the LVMVolumeGroup %s", d.Path, d.BlockDevice, lvg.Name))
			}
		}
	}

	if errs.Len() != 0 {
		return errors.New(errs.String())
	}

	return nil
}

func (r *Reconciler) extendVGIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
	pvs []internal.PVData,
	blockDevices map[string]v1alpha1.BlockDevice,
) error {
	for _, n := range lvg.Status.Nodes {
		for _, d := range n.Devices {
			r.log.Trace(fmt.Sprintf("[ExtendVGIfNeeded] the LVMVolumeGroup %s status block device: %s", lvg.Name, d.BlockDevice))
		}
	}

	pvsMap := make(map[string]struct{}, len(pvs))
	for _, pv := range pvs {
		pvsMap[pv.PVName] = struct{}{}
	}

	devicesToExtend := make([]string, 0, len(blockDevices))
	for _, bd := range blockDevices {
		if _, exist := pvsMap[bd.Status.Path]; !exist {
			r.log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] the BlockDevice %s of LVMVolumeGroup %s Spec is not counted as used", bd.Name, lvg.Name))
			devicesToExtend = append(devicesToExtend, bd.Name)
		}
	}

	if len(devicesToExtend) == 0 {
		r.log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s should not be extended", vg.VGName, lvg.Name))
		return nil
	}

	if isApplied(lvg) {
		err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[ExtendVGIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[ExtendVGIfNeeded] VG %s should be extended as there are some BlockDevices were added to Spec field of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
	paths := extractPathsFromBlockDevices(devicesToExtend, blockDevices)
	err := r.extendVGComplex(ctx, nil, paths, vg.VGName)
	if err != nil {
		r.log.Error(err, fmt.Sprintf("[ExtendVGIfNeeded] unable to extend VG %s of the LVMVolumeGroup %s", vg.VGName, lvg.Name))
		return err
	}
	r.log.Info(fmt.Sprintf("[ExtendVGIfNeeded] VG %s of the LVMVolumeGroup %s was extended", vg.VGName, lvg.Name))

	return nil
}

func (r *Reconciler) tryGetVG(vgName string) (bool, internal.VGData) {
	vgs, _ := r.sdsCache.GetVGs()
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true, vg
		}
	}

	return false, internal.VGData{}
}

func (r *Reconciler) removeLVGFinalizerIfExist(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) (bool, error) {
	if !slices.Contains(lvg.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		return false, nil
	}

	for i := range lvg.Finalizers {
		if lvg.Finalizers[i] == internal.SdsNodeConfiguratorFinalizer {
			lvg.Finalizers = append(lvg.Finalizers[:i], lvg.Finalizers[i+1:]...)
			break
		}
	}

	err := r.cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *Reconciler) getLVForVG(vgName string) []string {
	lvs, _ := r.sdsCache.GetLVs()
	usedLVs := make([]string, 0, len(lvs))
	for _, lv := range lvs {
		if lv.VGName == vgName {
			usedLVs = append(usedLVs, lv.LVName)
		}
	}

	return usedLVs
}

func (r *Reconciler) deleteVGIfExist(vgName string) error {
	vgs, _ := r.sdsCache.GetVGs()
	if !checkIfVGExist(vgName, vgs) {
		r.log.Debug(fmt.Sprintf("[DeleteVGIfExist] no VG %s found, nothing to delete", vgName))
		return nil
	}

	pvs, _ := r.sdsCache.GetPVs()
	if len(pvs) == 0 {
		err := errors.New("no any PV found")
		r.log.Error(err, fmt.Sprintf("[DeleteVGIfExist] no any PV was found while deleting VG %s", vgName))
		return err
	}

	start := time.Now()
	command, err := r.commands.RemoveVG(vgName)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "vgremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgremove").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgremove").Inc()
		r.log.Error(err, "RemoveVG "+command)
		return err
	}
	r.log.Debug(fmt.Sprintf("[DeleteVGIfExist] VG %s was successfully deleted from the node", vgName))
	var pvsToRemove []string
	for _, pv := range pvs {
		if pv.VGName == vgName {
			pvsToRemove = append(pvsToRemove, pv.PVName)
		}
	}

	start = time.Now()
	command, err = r.commands.RemovePV(pvsToRemove)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "pvremove").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "pvremove").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "pvremove").Inc()
		r.log.Error(err, "RemovePV "+command)
		return err
	}
	r.log.Debug(fmt.Sprintf("[DeleteVGIfExist] successfully delete PVs of VG %s from the node", vgName))

	return nil
}

// extendVGComplex adds devices to an existing Volume Group.
//
// pvs may be nil, in which case the PV listing is taken here. Callers that
// already built one pass it in so the node is not asked twice; the important part
// is that the listing is live rather than the cache's — see pvView.
func (r *Reconciler) extendVGComplex(ctx context.Context, pvs *pvView, extendPVs []string, vgName string) error {
	if pvs == nil {
		pvs = r.newPVView(ctx, "ExtendVGComplex")
	}
	for _, pvPath := range extendPVs {
		if err := r.createPVIfNeeded(ctx, pvs, "ExtendVGComplex", pvPath); err != nil {
			return err
		}
	}

	start := time.Now()
	command, err := r.commands.ExtendVG(vgName, extendPVs)
	r.metrics.UtilsCommandsDuration(ReconcilerName, "vgextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgextend").Inc()
	r.log.Debug(command)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgextend").Inc()
		r.log.Error(err, "ExtendVG ")
		return err
	}

	r.triggerUdevForPaths(ctx, extendPVs)

	return nil
}

// triggerUdevForPaths sends a "change" uevent for the given device paths so that
// the host udev re-probes them. lvm.static is built without udev integration, so
// after pvcreate/vgcreate the udev DB stays stale and lsblk never reports
// LVM2_member as fstype — which blocks the BD discoverer from linking the device
// to its VG.
//
// The call is best-effort: a failure is logged as a warning and never propagates
// to the caller. Operators observe the call frequency and error rate through
// the UtilsCommands* metrics with cmd label "udevadm-trigger".
//
// The timeout is bounded by a child context derived from the caller's context,
// so a SIGTERM from kubelet (or any cancellation of the reconcile loop) is
// honoured immediately instead of being absorbed by a detached background ctx.
func (r *Reconciler) triggerUdevForPaths(parent context.Context, paths []string) {
	if len(paths) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(parent, udevadmTriggerTimeout)
	defer cancel()

	start := time.Now()
	cmd, err := r.commands.UdevadmTrigger(ctx, paths)
	r.metrics.UtilsCommandsDuration(ReconcilerName, udevadmTriggerCmdLabel).Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, udevadmTriggerCmdLabel).Inc()
	r.log.Debug(cmd)
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, udevadmTriggerCmdLabel).Inc()
		r.log.Warning(fmt.Sprintf("[triggerUdevForPaths] udevadm trigger failed for %v (non-fatal): %v, cmd: %s", paths, err, cmd))
	}
}

func (r *Reconciler) createVGComplex(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (retErr error) {
	paths := extractPathsFromBlockDevices(nil, blockDevices)

	// The create path validates strictly (validateLVGForCreateFunc), so an
	// LVMVolumeGroup that reaches here has no rejected entries to skip — and
	// nothing to be lenient about: without a Volume Group, a partially
	// provisioned set of file devices is not a state worth keeping.
	loopPaths, provisioned, err := r.provisionFileDevices(ctx, lvg, fileDeviceIssues{}, false)
	if err != nil {
		return fmt.Errorf("file device provisioning failed: %w", err)
	}
	paths = append(paths, loopPaths...)

	// If a later step (pvcreate/vgcreate) fails, tear down ONLY the file devices
	// this call provisioned — and never one that already became a PV. This must
	// NOT use the broad cleanupFileDevices (the delete-path cleanup): it walks
	// spec+status and could remove the backing file of a loop that a concurrent
	// reconcile, or a pvcreate/vgcreate that materially succeeded but returned a
	// non-zero status, had already turned into a live PV of the VG — which the
	// next reconcile then re-provisions with a second loop, doubling the VG.
	// See rollbackProvisionedFileDevices. Runs on a detached context because the
	// failure is frequently the reconcile ctx being cancelled.
	if len(provisioned) > 0 {
		defer func() {
			if retErr == nil {
				return
			}
			rollbackCtx, cancel := r.newRollbackContext()
			defer cancel()
			r.rollbackProvisionedFileDevices(rollbackCtx, provisioned)
		}()
	}

	r.log.Trace(fmt.Sprintf("[CreateVGComplex] LVMVolumeGroup %s devices paths %v", lvg.Name, paths))

	existingPVs := r.newPVView(ctx, "CreateVGComplex")
	for _, path := range paths {
		if err := r.createPVIfNeeded(ctx, existingPVs, "CreateVGComplex", path); err != nil {
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[CreateVGComplex] successfully created all PVs for the LVMVolumeGroup %s", lvg.Name))
	r.log.Debug(fmt.Sprintf("[CreateVGComplex] the LVMVolumeGroup %s type is %s", lvg.Name, lvg.Spec.Type))
	switch lvg.Spec.Type {
	case internal.Local:
		start := time.Now()
		cmd, err := r.commands.CreateVGLocal(lvg.Spec.ActualVGNameOnTheNode, lvg.Name, paths)
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgcreate").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgcreate").Inc()
		r.log.Debug(cmd)
		if err != nil {
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgcreate").Inc()
			r.log.Error(err, "error CreateVGLocal")
			return err
		}
	}

	r.log.Debug(fmt.Sprintf("[CreateVGComplex] successfully create VG %s of the LVMVolumeGroup %s", lvg.Spec.ActualVGNameOnTheNode, lvg.Name))

	r.triggerUdevForPaths(ctx, paths)

	return nil
}

func (r *Reconciler) updateVGTagIfNeeded(
	ctx context.Context,
	lvg *v1alpha1.LVMVolumeGroup,
	vg internal.VGData,
) (bool, error) {
	found, tagName := utils.ReadValueFromTags(vg.VGTags, internal.LVMVolumeGroupTag)
	if found && lvg.Name != tagName {
		// Retagging is how a VG follows its LVMVolumeGroup being renamed. For a
		// file-backed VG the rename is only half a rename: the backing files keep
		// the old name in their basenames, so after this the agent stops
		// recognising its own file devices (IsManagedFileDevicePath is gated on
		// the LVG name) and the next provision round creates a second set from
		// the spec, doubling the VG. The files are deliberately not renamed —
		// a live loop device is attached to them — so the honest thing is to warn
		// loudly and let the operator decide.
		if len(lvg.Spec.FileDevices) > 0 {
			r.log.Warning(fmt.Sprintf("[UpdateVGTagIfNeeded] VG %s is tagged for the LVMVolumeGroup %s but is being reconciled as %s, and it has file devices whose backing files still encode %s. "+
				"The file devices will stop being recognised as managed; move the backing files to the new name manually, or restore the original resource name",
				vg.VGName, tagName, lvg.Name, tagName))
		}

		if isApplied(lvg) {
			err := r.lvgCl.UpdateLVGConditionIfNeeded(ctx, lvg, v1.ConditionFalse, internal.TypeVGConfigurationApplied, internal.ReasonUpdating, "trying to apply the configuration")
			if err != nil {
				r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add the condition %s status False reason %s to the LVMVolumeGroup %s", internal.TypeVGConfigurationApplied, internal.ReasonUpdating, lvg.Name))
				return false, err
			}
		}

		start := time.Now()
		cmd, err := r.commands.VGChangeDelTag(ctx, vg.VGName, fmt.Sprintf("%s=%s", internal.LVMVolumeGroupTag, tagName))
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgchange").Inc()
		r.log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to delete LVMVolumeGroupTag: %s=%s, vg: %s", internal.LVMVolumeGroupTag, tagName, vg.VGName))
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgchange").Inc()
			return false, err
		}

		start = time.Now()
		cmd, err = r.commands.VGChangeAddTag(ctx, vg.VGName, fmt.Sprintf("%s=%s", internal.LVMVolumeGroupTag, lvg.Name))
		r.metrics.UtilsCommandsDuration(ReconcilerName, "vgchange").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
		r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "vgchange").Inc()
		r.log.Debug(fmt.Sprintf("[UpdateVGTagIfNeeded] exec cmd: %s", cmd))
		if err != nil {
			r.log.Error(err, fmt.Sprintf("[UpdateVGTagIfNeeded] unable to add LVMVolumeGroupTag: %s=%s, vg: %s", internal.LVMVolumeGroupTag, lvg.Name, vg.VGName))
			r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "vgchange").Inc()
			return false, err
		}

		return true, nil
	}

	return false, nil
}

func (r *Reconciler) extendThinPool(lvg *v1alpha1.LVMVolumeGroup, specThinPool v1alpha1.LVMVolumeGroupThinPoolSpec) error {
	volumeGroupFreeSpaceBytes := lvg.Status.VGSize.Value() - lvg.Status.AllocatedSize.Value()
	tpRequestedSize, err := utils.GetRequestedSizeFromString(specThinPool.Size, lvg.Status.VGSize)
	if err != nil {
		return err
	}

	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupSize = %s", lvg.Status.VGSize.String()))
	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupAllocatedSize = %s", lvg.Status.AllocatedSize.String()))
	r.log.Trace(fmt.Sprintf("[ExtendThinPool] volumeGroupFreeSpaceBytes = %d", volumeGroupFreeSpaceBytes))

	r.log.Info(fmt.Sprintf("[ExtendThinPool] start resizing thin pool: %s; with new size: %s", specThinPool.Name, tpRequestedSize.String()))

	var cmd string
	start := time.Now()
	vg := r.sdsCache.FindVG(lvg.Spec.ActualVGNameOnTheNode)
	alignedTpSize, alignErr := utils.AlignSizeToExtent(tpRequestedSize, extentSizeForThinPoolAlign(lvg, vg))
	if alignErr != nil {
		r.log.Error(alignErr, fmt.Sprintf("[ExtendThinPool] unable to align thin-pool %s size for LVMVolumeGroup %s", specThinPool.Name, lvg.Name))
		return alignErr
	}
	if alignedTpSize.Value() >= lvg.Status.VGSize.Value() {
		r.log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size 100VG", specThinPool.Name, lvg.Name))
		cmd, err = r.commands.ExtendLVFullVGSpace(lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	} else {
		r.log.Debug(fmt.Sprintf("[ExtendThinPool] thin-pool %s of the LVMVolumeGroup %s will be extend to size %s", specThinPool.Name, lvg.Name, tpRequestedSize.String()))
		cmd, err = r.commands.ExtendLV(tpRequestedSize.Value(), lvg.Spec.ActualVGNameOnTheNode, specThinPool.Name)
	}
	r.metrics.UtilsCommandsDuration(ReconcilerName, "lvextend").Observe(r.metrics.GetEstimatedTimeInSeconds(start))
	r.metrics.UtilsCommandsExecutionCount(ReconcilerName, "lvextend").Inc()
	if err != nil {
		r.metrics.UtilsCommandsErrorsCount(ReconcilerName, "lvextend").Inc()
		r.log.Error(err, fmt.Sprintf("[ExtendThinPool] unable to extend LV, name: %s, cmd: %s", specThinPool.Name, cmd))
		return err
	}

	return nil
}

func (r *Reconciler) addLVGLabelIfNeeded(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, labelKey, labelValue string) (bool, error) {
	if !r.shouldUpdateLVGLabels(lvg, labelKey, labelValue) {
		return false, nil
	}

	if lvg.Labels == nil {
		lvg.Labels = make(map[string]string)
	}

	lvg.Labels[labelKey] = labelValue
	err := r.cl.Update(ctx, lvg)
	if err != nil {
		return false, err
	}

	return true, nil
}

// vgRemovalAllowed answers whether the Volume Group named in lvg.Spec may be
// removed from the node as part of deleting this resource, and if not, why not.
//
// It exists because the removal addresses the Volume Group BY NAME, and a name is
// not a handle. Three ways that goes wrong, all of them seen on a live cluster:
//
//   - a leftover resource over somebody else's storage. Hundreds of stale
//     LVMVolumeGroups named after e2e runs pointed at one live VG; the first delete
//     would have vgremove'd it out from under the resource that owns it. The VG's
//     own storage.deckhouse.io/lvmVolumeGroupName tag says who that is.
//   - a Volume Group on shared storage. A LUN-backed VG is presented to several
//     hosts, so the same VG appears in one node's listing on one scan and another
//     node's on the next; a stale resource on the second host would remove it the
//     moment the LUN arrives there. status.vgUuid is what pins the identity.
//   - a duplicated name. Two Volume Groups answering to one name — a guest's LVM
//     inside a disk this node can see is enough — and `vgremove <name>` is then a
//     coin toss.
//
// The permissive answer is deliberate for the two honest cases: no Volume Group of
// that name on the node (nothing to remove, and the caller no-ops), and a Volume
// Group with no owner tag at all (a manually created one this module adopted, whose
// removal on delete is the documented behaviour).
func (r *Reconciler) vgRemovalAllowed(lvg *v1alpha1.LVMVolumeGroup) (string, bool) {
	vgs, _ := r.sdsCache.GetVGs()

	matched := make([]internal.VGData, 0, 1)
	for _, vg := range vgs {
		if vg.VGName == lvg.Spec.ActualVGNameOnTheNode {
			matched = append(matched, vg)
		}
	}

	switch len(matched) {
	case 0:
		return "", true
	case 1:
	default:
		uuids := make([]string, 0, len(matched))
		for _, vg := range matched {
			uuids = append(uuids, vg.VGUUID)
		}
		return fmt.Sprintf("%d VGs on the node answer to this name (UUIDs: %s), so removing it by name would be a guess",
			len(matched), strings.Join(uuids, ", ")), false
	}

	vg := matched[0]

	// status.vgUuid is written when the agent creates or adopts the Volume Group, so
	// a mismatch means the VG under this name is not the one this resource was
	// serving. Empty status is not a mismatch: the resource never got that far.
	if lvg.Status.VGUuid != "" && vg.VGUUID != lvg.Status.VGUuid {
		return fmt.Sprintf("the VG under this name has VG_UUID=%s while the resource served %s, so it is a different Volume Group",
			vg.VGUUID, lvg.Status.VGUuid), false
	}

	// The first return of ReadValueFromTags says whether the VG is managed at all,
	// not whether the key was found, so the value is what has to be looked at. An
	// empty owner covers both honest cases — an unmanaged Volume Group and a managed
	// one carrying no name tag — and both keep the old behaviour.
	if _, owner := utils.ReadValueFromTags(vg.VGTags, internal.LVMVolumeGroupTag); owner != "" && owner != lvg.Name {
		return fmt.Sprintf("the VG is tagged %s=%s, so it belongs to that LVMVolumeGroup and not to this one", internal.LVMVolumeGroupTag, owner), false
	}

	return "", true
}

func checkIfVGExist(vgName string, vgs []internal.VGData) bool {
	for _, vg := range vgs {
		if vg.VGName == vgName {
			return true
		}
	}

	return false
}

func validateSpecBlockDevices(lvg *v1alpha1.LVMVolumeGroup, blockDevices map[string]v1alpha1.BlockDevice) (bool, string) {
	if len(blockDevices) == 0 {
		return false, "none of specified BlockDevices were found"
	}

	if len(lvg.Status.Nodes) > 0 {
		lostBdNames := make([]string, 0, len(lvg.Status.Nodes[0].Devices))
		for _, n := range lvg.Status.Nodes {
			for _, d := range n.Devices {
				if _, found := blockDevices[d.BlockDevice]; !found {
					lostBdNames = append(lostBdNames, d.BlockDevice)
				}
			}
		}

		// that means some of the used BlockDevices no longer match the blockDeviceSelector
		if len(lostBdNames) > 0 {
			return false, fmt.Sprintf("these BlockDevices no longer match the blockDeviceSelector: %s", strings.Join(lostBdNames, ","))
		}
	}

	// A file-only LVMVolumeGroup has no blockDeviceSelector; there are no
	// match expressions to validate, and dereferencing the nil selector
	// would panic. (The production caller already skips this function for
	// file-only groups; this guard keeps it safe if called directly.)
	if lvg.Spec.BlockDeviceSelector == nil {
		return true, ""
	}

	for _, me := range lvg.Spec.BlockDeviceSelector.MatchExpressions {
		if me.Key == internal.MetadataNameLabelKey && me.Operator == v1.LabelSelectorOpIn {
			if len(me.Values) != len(blockDevices) {
				missedBds := make([]string, 0, len(me.Values))
				for _, bdName := range me.Values {
					if _, exist := blockDevices[bdName]; !exist {
						missedBds = append(missedBds, bdName)
					}
				}

				return false, fmt.Sprintf("unable to find specified BlockDevices: %s", strings.Join(missedBds, ","))
			}
		}
	}

	return true, ""
}

func filterBlockDevicesByNodeName(blockDevices map[string]v1alpha1.BlockDevice, nodeName string) map[string]v1alpha1.BlockDevice {
	bdsForUsage := make(map[string]v1alpha1.BlockDevice, len(blockDevices))
	for _, bd := range blockDevices {
		if bd.Status.NodeName == nodeName {
			bdsForUsage[bd.Name] = bd
		}
	}

	return bdsForUsage
}

func checkIfLVGBelongsToNode(lvg *v1alpha1.LVMVolumeGroup, nodeName string) bool {
	return lvg.Spec.Local.NodeName == nodeName
}

func extractPathsFromBlockDevices(targetDevices []string, blockDevices map[string]v1alpha1.BlockDevice) []string {
	var paths []string
	if len(targetDevices) > 0 {
		paths = make([]string, 0, len(targetDevices))
		for _, bdName := range targetDevices {
			bd := blockDevices[bdName]
			paths = append(paths, bd.Status.Path)
		}
	} else {
		paths = make([]string, 0, len(blockDevices))
		for _, bd := range blockDevices {
			paths = append(paths, bd.Status.Path)
		}
	}

	return paths
}

func countVGSizeByBlockDevices(blockDevices map[string]v1alpha1.BlockDevice) resource.Quantity {
	var totalVGSize int64
	for _, bd := range blockDevices {
		totalVGSize += bd.Status.Size.Value()
	}
	return *resource.NewQuantity(totalVGSize, resource.BinarySI)
}
