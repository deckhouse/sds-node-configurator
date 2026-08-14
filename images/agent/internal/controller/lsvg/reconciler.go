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

	if err := r.writeHostIDFile(hostID); err != nil {
		return controller.Result{}, err
	}

	if r.lockspaceStarted(node, lsvg.Name) {
		return controller.Result{}, nil
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
	return controller.Result{}, nil
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
