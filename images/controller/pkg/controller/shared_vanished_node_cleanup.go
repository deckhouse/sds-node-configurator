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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/internal"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/logger"
)

const SharedVanishedNodeCleanupCtrl = "shared-vanished-node-cleanup-controller"

// RunSharedVanishedNodeCleanup removes what a node that no longer exists left
// behind in a shared pool.
//
// Everything a member of a pool says about itself is written by that member,
// which is right while it exists and leaves nothing to correct it when it does
// not. A node fenced by the platform is exactly that case: for a cloud node the
// fencing controller deletes the Node object and the provider builds a new
// instance under a new name, so the agent that would have stopped the lockspace,
// retracted the node's entry and released its attachments is gone with the node
// it ran on. Measured on the stand: an attachment stayed Attached against a node
// that had not existed for ten minutes, and the volume group went on listing a
// member holding leases.
//
// None of this cleanup grants anybody access to anything. The lock over a volume
// of a shared pool is a sanlock lease, and a lease is taken away by its own
// clock, not by the state of the API: a node whose Node object was deleted while
// the machine kept running still renews its leases, and another node's exclusive
// activation still fails until they expire. What is removed here is only the
// bookkeeping — which is precisely what nothing else can remove.
func RunSharedVanishedNodeCleanup(mgr manager.Manager, log logger.Logger) error {
	cl := mgr.GetClient()

	c, err := controller.New(SharedVanishedNodeCleanupCtrl, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Debug(fmt.Sprintf("[%s] got a request %s", SharedVanishedNodeCleanupCtrl, request.String()))

			if err := cleanUpAfterVanishedNodes(ctx, cl, log); err != nil {
				log.Error(err, fmt.Sprintf("[%s] unable to clean up after nodes that no longer exist", SharedVanishedNodeCleanupCtrl))
				return reconcile.Result{}, err
			}

			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		return err
	}

	// Both watches matter and for different reasons. A node deletion is the
	// event this exists for, and it is the one event the objects being cleaned
	// up never produce themselves. The groups are watched too because a pool
	// commissioned after a node vanished has to be looked at as well.
	if err := c.Watch(source.Kind(mgr.GetCache(), &corev1.Node{},
		&handler.TypedEnqueueRequestForObject[*corev1.Node]{})); err != nil {
		return err
	}

	return c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMSharedVolumeGroup{},
		&handler.TypedEnqueueRequestForObject[*v1alpha1.LVMSharedVolumeGroup]{}))
}

func cleanUpAfterVanishedNodes(ctx context.Context, cl client.Client, log logger.Logger) error {
	groups := &v1alpha1.LVMSharedVolumeGroupList{}
	if err := cl.List(ctx, groups); err != nil {
		return fmt.Errorf("list LVMSharedVolumeGroups: %w", err)
	}
	attachments := &v1alpha1.LVMSharedLogicalVolumeAttachmentList{}
	if err := cl.List(ctx, attachments); err != nil {
		return fmt.Errorf("list LVMSharedLogicalVolumeAttachments: %w", err)
	}
	if len(groups.Items) == 0 && len(attachments.Items) == 0 {
		// No shared pools in this cluster, which is the ordinary case. Nothing
		// below would do anything, and listing the nodes for it would make every
		// node event of every cluster pay for a feature it does not use.
		return nil
	}

	nodes := &corev1.NodeList{}
	if err := cl.List(ctx, nodes); err != nil {
		return fmt.Errorf("list Nodes: %w", err)
	}
	// A List that came back short would read as "every node is gone", so the
	// answer is taken as a whole or not at all: an empty cluster does not exist,
	// and acting on one would delete every attachment there is.
	if len(nodes.Items) == 0 {
		return nil
	}

	exists := make(map[string]struct{}, len(nodes.Items))
	for i := range nodes.Items {
		exists[nodes.Items[i].Name] = struct{}{}
	}

	for i := range groups.Items {
		if err := pruneVanishedMembers(ctx, cl, log, &groups.Items[i], exists); err != nil {
			return err
		}
	}

	for i := range attachments.Items {
		if err := releaseAttachmentOfVanishedNode(ctx, cl, log, &attachments.Items[i], exists); err != nil {
			return err
		}
	}

	return nil
}

// pruneVanishedMembers drops the entries of nodes that are not in the cluster.
//
// The entries are applied by each member with a field manager of its own, and
// this is the one writer allowed to take one away: the manager that owns it will
// never run again. Nothing is written when there is nothing to drop, which is
// every pass on a healthy pool.
func pruneVanishedMembers(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	group *v1alpha1.LVMSharedVolumeGroup,
	exists map[string]struct{},
) error {
	if group.Status == nil || len(group.Status.Nodes) == 0 {
		return nil
	}

	kept := make([]v1alpha1.LVMSharedVolumeGroupNodeStatus, 0, len(group.Status.Nodes))
	vanished := make([]string, 0, len(group.Status.Nodes))
	for _, entry := range group.Status.Nodes {
		if _, ok := exists[entry.Name]; ok {
			kept = append(kept, entry)
			continue
		}
		vanished = append(vanished, entry.Name)
	}
	if len(vanished) == 0 {
		return nil
	}

	patch := client.MergeFrom(group.DeepCopy())
	group.Status.Nodes = kept
	if err := cl.Status().Patch(ctx, group, patch); err != nil {
		return fmt.Errorf("drop the entries of %v from %s: %w", vanished, group.Name, err)
	}

	log.Info(fmt.Sprintf("[%s] %s no longer lists %v: the node(s) are not in the cluster and nothing on them can correct their own entry",
		SharedVanishedNodeCleanupCtrl, group.Name, vanished))
	return nil
}

// releaseAttachmentOfVanishedNode lets go of a lock granted to a node that is
// not there any more.
//
// The finalizer is removed here rather than waited on, and that is the whole
// point: it is held for the node's agent to deactivate the volume before the
// object goes, and the agent is gone with the node. Deleting the object without
// removing it would replace a stale attachment with one stuck in deletion
// forever.
func releaseAttachmentOfVanishedNode(
	ctx context.Context,
	cl client.Client,
	log logger.Logger,
	attachment *v1alpha1.LVMSharedLogicalVolumeAttachment,
	exists map[string]struct{},
) error {
	if _, ok := exists[attachment.Spec.NodeName]; ok {
		return nil
	}

	if slices.Contains(attachment.Finalizers, internal.SdsNodeConfiguratorFinalizer) {
		patch := client.MergeFrom(attachment.DeepCopy())
		attachment.Finalizers = slices.DeleteFunc(attachment.Finalizers, func(f string) bool {
			return f == internal.SdsNodeConfiguratorFinalizer
		})
		if err := cl.Patch(ctx, attachment, patch); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("take the finalizer off attachment %s: %w", attachment.Name, err)
		}
	}

	if err := cl.Delete(ctx, attachment); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete attachment %s of the vanished node %s: %w", attachment.Name, attachment.Spec.NodeName, err)
	}

	log.Info(fmt.Sprintf("[%s] attachment %s is released: node %s is not in the cluster, so nothing there will ever deactivate the volume",
		SharedVanishedNodeCleanupCtrl, attachment.Name, attachment.Spec.NodeName))
	return nil
}
