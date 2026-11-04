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

package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/config"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

const (
	MissingNodeReleaseCtrlName = "missing-node-finalizer-release"

	// missingNodeReleaseInterval is how often the resources are checked against
	// the nodes. The grace period is counted in minutes.
	missingNodeReleaseInterval = time.Minute

	eventReasonNodeMissing            = "NodeMissing"
	eventReasonAgentFinalizerReleased = "AgentFinalizerReleased"

	kindLVMVolumeGroup           = "LVMVolumeGroup"
	kindLVMLogicalVolume         = "LVMLogicalVolume"
	kindLVMLogicalVolumeSnapshot = "LVMLogicalVolumeSnapshot"

	// The reasons a resource of a missing node keeps the agent's finalizer past
	// the grace period, as the missing_node_resources_held metric reports them.
	heldReasonForeignFinalizers = "ForeignFinalizers"
	heldReasonVolumesLeft       = "VolumesLeft"
	heldReasonReleaseDisabled   = "ReleaseDisabled"
)

// RunMissingNodeRelease starts the release of the agent's finalizer from the
// resources of nodes deleted from the cluster.
//
// The agent of a node puts its finalizer on the LVMVolumeGroups,
// LVMLogicalVolumes and LVMLogicalVolumeSnapshots of that node and removes it
// once it has cleaned up on the node. A node deleted from the cluster has no
// agent left, and its resources stayed in Terminating forever. The controller
// removes the finalizer for it, once the node has been gone for the grace period
// and only the agent's finalizer is left; nothing on the node is touched.
//
// An LVMVolumeGroup that is not being deleted is left alone: the Volume Group on
// the disks outlives the node, and a node reinstalled under the same name finds
// it again.
func RunMissingNodeRelease(mgr manager.Manager, log logger.Logger, cfg config.Options, metrics monitoring.Metrics) error {
	return mgr.Add(&missingNodeRelease{
		cl:       mgr.GetClient(),
		reader:   mgr.GetAPIReader(),
		recorder: mgr.GetEventRecorderFor(MissingNodeReleaseCtrlName),
		log:      log.Named(MissingNodeReleaseCtrlName),
		metrics:  metrics,
		grace:    cfg.MissingNodeGracePeriod,
		interval: missingNodeReleaseInterval,
	})
}

type missingNodeRelease struct {
	cl       client.Client
	reader   client.Reader
	recorder record.EventRecorder
	log      logger.Logger
	metrics  monitoring.Metrics
	grace    time.Duration
	interval time.Duration
}

// NeedLeaderElection keeps the release to one controller replica.
func (r *missingNodeRelease) NeedLeaderElection() bool { return true }

func (r *missingNodeRelease) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.pass(ctx, time.Now()); err != nil {
			r.log.Error("unable to check the resources of nodes missing from the cluster", logger.Err(err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type nodePresence int

const (
	nodePresent nodePresence = iota
	nodeGone
	nodeUnknown
)

// missingLVG is an LVMVolumeGroup whose node is gone, with the time the node
// was first found gone.
type missingLVG struct {
	lvg   *v1alpha1.LVMVolumeGroup
	since time.Time
}

type heldKey struct {
	kind, node, reason string
}

// pass records on every LVMVolumeGroup whether its node is gone and releases
// what releaseVerdict allows. The LVMLogicalVolumes and the snapshots are read
// only when some node is gone, and past the cache: a volume the cache has not
// seen yet must not let its LVMVolumeGroup go.
//
// The snapshots are handled before the volumes and the volumes before the
// groups, and a group waits for every volume and snapshot on it: they find their
// node and their grace period through the group.
func (r *missingNodeRelease) pass(ctx context.Context, now time.Time) error {
	nodes := &v1.NodeList{}
	if err := r.cl.List(ctx, nodes); err != nil {
		return fmt.Errorf("listing the nodes: %w", err)
	}
	lvgList := &v1alpha1.LVMVolumeGroupList{}
	if err := r.cl.List(ctx, lvgList); err != nil {
		return fmt.Errorf("listing the LVMVolumeGroups: %w", err)
	}

	cached := make(map[string]struct{}, len(nodes.Items))
	for _, n := range nodes.Items {
		cached[n.Name] = struct{}{}
	}
	presence := make(map[string]nodePresence)

	missing := make(map[string]missingLVG)
	lvgs := make(map[string]*v1alpha1.LVMVolumeGroup, len(lvgList.Items))
	for i := range lvgList.Items {
		lvg := &lvgList.Items[i]
		lvgs[lvg.Name] = lvg
		nodeName := lvg.Spec.Local.NodeName
		if nodeName == "" {
			continue
		}
		state, seen := presence[nodeName]
		if !seen {
			state = r.nodePresence(ctx, nodeName, cached)
			presence[nodeName] = state
		}
		since, err := r.syncMissingSince(ctx, lvg, state, now)
		if err != nil {
			r.log.Error(fmt.Sprintf("unable to record on the LVMVolumeGroup %s whether its node %s is gone", lvg.Name, nodeName), logger.Err(err))
			continue
		}
		if !since.IsZero() {
			missing[lvg.Name] = missingLVG{lvg: lvg, since: since}
		}
	}

	r.metrics.ResetMissingNodeGauges()
	for name, m := range missing {
		r.metrics.LVGNodeMissingSince(name, m.lvg.Spec.Local.NodeName).Set(float64(m.since.Unix()))
	}
	if len(missing) == 0 {
		return nil
	}

	llvList := &v1alpha1.LVMLogicalVolumeList{}
	if err := r.reader.List(ctx, llvList); err != nil {
		return fmt.Errorf("listing the LVMLogicalVolumes: %w", err)
	}
	llvsList := &v1alpha1.LVMLogicalVolumeSnapshotList{}
	if err := r.reader.List(ctx, llvsList); err != nil && !meta.IsNoMatchError(err) {
		return fmt.Errorf("listing the LVMLogicalVolumeSnapshots: %w", err)
	}
	llvs := make(map[string]*v1alpha1.LVMLogicalVolume, len(llvList.Items))
	for i := range llvList.Items {
		llvs[llvList.Items[i].Name] = &llvList.Items[i]
	}

	held := make(map[heldKey]int)
	inUse := make(map[string]struct{})

	for i := range llvsList.Items {
		snapshot := &llvsList.Items[i]
		lvg := snapshotLVG(snapshot, llvs, lvgs)
		if lvg == nil {
			continue
		}
		inUse[lvg.Name] = struct{}{}
		if m, ok := missing[lvg.Name]; ok {
			r.consider(ctx, snapshot, kindLVMLogicalVolumeSnapshot, m, now, "", held)
		}
	}
	for _, llv := range llvs {
		lvg := volumeLVG(llv, lvgs)
		if lvg == nil {
			continue
		}
		inUse[lvg.Name] = struct{}{}
		if m, ok := missing[lvg.Name]; ok {
			r.consider(ctx, llv, kindLVMLogicalVolume, m, now, "", held)
		}
	}
	for name, m := range missing {
		blocker := ""
		if _, used := inUse[name]; used {
			blocker = heldReasonVolumesLeft
		}
		r.consider(ctx, m.lvg, kindLVMVolumeGroup, m, now, blocker, held)
	}

	for key, count := range held {
		r.metrics.MissingNodeResourcesHeld(key.kind, key.node, key.reason).Set(float64(count))
	}
	return nil
}

// nodePresence says whether nodeName is in the cluster. Only the API server,
// not the cache, can tell that a node is gone; a node the cache has is taken as
// present, which is the safe side of a cache that lags.
func (r *missingNodeRelease) nodePresence(ctx context.Context, nodeName string, cached map[string]struct{}) nodePresence {
	if _, ok := cached[nodeName]; ok {
		return nodePresent
	}
	err := r.reader.Get(ctx, client.ObjectKey{Name: nodeName}, &v1.Node{})
	switch {
	case err == nil:
		return nodePresent
	case apierrors.IsNotFound(err):
		return nodeGone
	default:
		r.log.Error(fmt.Sprintf("unable to check whether the node %s is in the cluster", nodeName), logger.Err(err))
		return nodeUnknown
	}
}

// missingSince returns the time the annotation of lvg records, or false when it
// has none or cannot be read.
func missingSince(lvg *v1alpha1.LVMVolumeGroup) (time.Time, bool) {
	raw, ok := lvg.Annotations[v1alpha1.NodeMissingSinceAnnotation]
	if !ok {
		return time.Time{}, false
	}
	since, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return since, true
}

// syncMissingSince brings the annotation of lvg in line with state and returns
// the time its node was first found gone, or zero when the node is not known to
// be gone. A node that could not be checked leaves the annotation as it is. An
// annotation that cannot be read is written again with now, which starts the
// grace period over rather than cutting it short.
func (r *missingNodeRelease) syncMissingSince(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup, state nodePresence, now time.Time) (time.Time, error) {
	since, recorded := missingSince(lvg)
	switch state {
	case nodeUnknown:
		return time.Time{}, nil
	case nodeGone:
		if recorded {
			return since, nil
		}
		since = now.UTC().Truncate(time.Second)
		patch := client.MergeFrom(lvg.DeepCopy())
		if lvg.Annotations == nil {
			lvg.Annotations = make(map[string]string, 1)
		}
		lvg.Annotations[v1alpha1.NodeMissingSinceAnnotation] = since.Format(time.RFC3339)
		if err := r.cl.Patch(ctx, lvg, patch); err != nil {
			return time.Time{}, client.IgnoreNotFound(err)
		}
		message := fmt.Sprintf("node %s is not in the cluster", lvg.Spec.Local.NodeName)
		if r.grace > 0 {
			message += fmt.Sprintf("; if this resource or its volumes are deleted, the finalizer %s is removed from them once the node has been gone for %s", internal.AgentFinalizer, r.grace)
		}
		r.recorder.Event(lvg, v1.EventTypeWarning, eventReasonNodeMissing, message)
		r.log.Warn(fmt.Sprintf("LVMVolumeGroup %s: %s", lvg.Name, message))
		return since, nil
	default:
		if _, has := lvg.Annotations[v1alpha1.NodeMissingSinceAnnotation]; !has {
			return time.Time{}, nil
		}
		patch := client.MergeFrom(lvg.DeepCopy())
		delete(lvg.Annotations, v1alpha1.NodeMissingSinceAnnotation)
		if err := r.cl.Patch(ctx, lvg, patch); err != nil {
			return time.Time{}, client.IgnoreNotFound(err)
		}
		r.log.Info(fmt.Sprintf("the node %s of the LVMVolumeGroup %s is in the cluster again", lvg.Spec.Local.NodeName, lvg.Name))
		return time.Time{}, nil
	}
}

// volumeLVG returns the LVMVolumeGroup llv is on: the one it names, or else the
// one on the node and Volume Group its status names.
func volumeLVG(llv *v1alpha1.LVMLogicalVolume, lvgs map[string]*v1alpha1.LVMVolumeGroup) *v1alpha1.LVMVolumeGroup {
	if lvg, ok := lvgs[llv.Spec.LVMVolumeGroupName]; ok {
		return lvg
	}
	if llv.Status == nil {
		return nil
	}
	return lvgByNodeAndVG(lvgs, llv.Status.NodeName, llv.Status.ActualVGNameOnTheNode)
}

// snapshotLVG returns the LVMVolumeGroup snapshot is on: the one of the volume
// it was taken from, or else, when that volume is gone, the one on the node and
// Volume Group its status names.
func snapshotLVG(snapshot *v1alpha1.LVMLogicalVolumeSnapshot, llvs map[string]*v1alpha1.LVMLogicalVolume, lvgs map[string]*v1alpha1.LVMVolumeGroup) *v1alpha1.LVMVolumeGroup {
	if llv, ok := llvs[snapshot.Spec.LVMLogicalVolumeName]; ok {
		if lvg := volumeLVG(llv, lvgs); lvg != nil {
			return lvg
		}
	}
	if snapshot.Status == nil {
		return nil
	}
	return lvgByNodeAndVG(lvgs, snapshot.Status.NodeName, snapshot.Status.ActualVGNameOnTheNode)
}

func lvgByNodeAndVG(lvgs map[string]*v1alpha1.LVMVolumeGroup, nodeName, vgName string) *v1alpha1.LVMVolumeGroup {
	if nodeName == "" || vgName == "" {
		return nil
	}
	for _, lvg := range lvgs {
		if lvg.Spec.Local.NodeName == nodeName && lvg.Spec.ActualVGNameOnTheNode == vgName {
			return lvg
		}
	}
	return nil
}

// releaseVerdict says whether the agent's finalizer can be removed from obj, a
// resource whose node has been gone since since, and if not, why it is kept past
// the grace period.
//
// Only a resource being deleted that still carries the agent's finalizer is
// considered. It is released once the node has been gone for grace and the
// agent's finalizer is the only one left: the agent itself waits for the other
// finalizers to go before it cleans up, since they mean a consumer still depends
// on the resource. blocker, when set, is another reason to keep it. grace <= 0
// disables the release.
func releaseVerdict(obj client.Object, since, now time.Time, grace time.Duration, blocker string) (release bool, heldReason string) {
	if obj.GetDeletionTimestamp() == nil || !controllerutil.ContainsFinalizer(obj, internal.AgentFinalizer) {
		return false, ""
	}
	if grace <= 0 {
		return false, heldReasonReleaseDisabled
	}
	if now.Sub(since) < grace {
		return false, ""
	}
	if len(obj.GetFinalizers()) > 1 {
		return false, heldReasonForeignFinalizers
	}
	if blocker != "" {
		return false, blocker
	}
	return true, ""
}

func (r *missingNodeRelease) consider(ctx context.Context, obj client.Object, kind string, m missingLVG, now time.Time, blocker string, held map[heldKey]int) {
	nodeName := m.lvg.Spec.Local.NodeName
	release, reason := releaseVerdict(obj, m.since, now, r.grace, blocker)
	if reason != "" {
		held[heldKey{kind: kind, node: nodeName, reason: reason}]++
		r.log.Debug(fmt.Sprintf("keeping the finalizer %s on the %s %s of the missing node %s: %s", internal.AgentFinalizer, kind, obj.GetName(), nodeName, reason))
	}
	if !release {
		return
	}
	if err := r.release(ctx, obj, kind, nodeName, m.since); err != nil {
		r.log.Error(fmt.Sprintf("unable to remove the finalizer %s from the %s %s", internal.AgentFinalizer, kind, obj.GetName()), logger.Err(err))
	}
}

// release removes the agent's finalizer from obj. The resource is read again
// past the cache and the finalizer is removed only if it is still the only one
// on a resource being deleted; the patch carries the resourceVersion read, so a
// finalizer added in between makes it fail rather than be lost.
func (r *missingNodeRelease) release(ctx context.Context, obj client.Object, kind, nodeName string, since time.Time) error {
	fresh, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("unable to copy the %s %s", kind, obj.GetName())
	}
	if err := r.reader.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
		return client.IgnoreNotFound(err)
	}
	if fresh.GetDeletionTimestamp() == nil || !slices.Equal(fresh.GetFinalizers(), []string{internal.AgentFinalizer}) {
		return nil
	}

	base, ok := fresh.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("unable to copy the %s %s", kind, obj.GetName())
	}
	controllerutil.RemoveFinalizer(fresh, internal.AgentFinalizer)
	if err := r.cl.Patch(ctx, fresh, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return client.IgnoreNotFound(err)
	}

	message := fmt.Sprintf("node %s is not in the cluster since %s, longer than %s: removed the finalizer %s; nothing was cleaned up on the node",
		nodeName, since.UTC().Format(time.RFC3339), r.grace, internal.AgentFinalizer)
	r.recorder.Event(fresh, v1.EventTypeWarning, eventReasonAgentFinalizerReleased, message)
	r.log.Warn(fmt.Sprintf("%s %s: %s", kind, obj.GetName(), message))
	return nil
}
