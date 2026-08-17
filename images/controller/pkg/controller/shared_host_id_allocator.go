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

package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/logger"
)

const (
	SharedHostIDAllocatorCtrl = "shared-host-id-allocator-controller"

	// SanlockHostIDAnnotation is where a node's sanlock host_id lives. The
	// agent copies the value into the file lvmlockd reads through
	// --host-id-file; nothing else consumes it.
	SanlockHostIDAnnotation = "storage.deckhouse.io/sanlock-host-id"

	// defaultSanlockAlignSize is what a group gets when it says nothing, and it
	// matches the schema default rather than sanlock's own.
	defaultSanlockAlignSize = "4Mi"
)

// hostIDCeilings maps the sanlock lease alignment to the largest host_id a
// lockspace with that alignment can hold. The numbers are a property of the
// on-disk layout, not a policy: a lease area aligned at 1Mi has room for 250
// hosts and no more.
//
// LVM does not check this itself — a host_id above the ceiling is accepted by
// vgcreate and by lvmlockd, and the failure surfaces later as a node that
// cannot join — so the allocator is the only place it is enforced.
var hostIDCeilings = map[string]int{
	"1Mi": 250,
	"2Mi": 500,
	"4Mi": 1000,
	"8Mi": 2000,
}

// RunSharedHostIDAllocator hands every node of a shared pool the sanlock
// host_id it will use, and publishes it as an annotation on the node.
//
// One id per node, not one per pool: lvmlockd reads a single file for every
// lockspace it starts, so a node that belongs to three pools uses the same id
// in all three. The ceiling, on the other hand, is per pool, so a node's id has
// to fit under the smallest ceiling among the pools it belongs to.
//
// The id is kept for as long as the Node object exists, including while the
// node is not in any pool. That is deliberate. An id whose delta lease is still
// alive cannot be handed to a different node without the new owner waiting out
// host_dead_seconds, and a node that leaves a pool and comes back — a reboot, a
// pod restart, an OnDelete update — is the common case rather than the rare
// one. Reusing ids would trade a free resource (there are hundreds to
// thousands of them) for a class of stalls that is hard to recognise.
func RunSharedHostIDAllocator(mgr manager.Manager, log logger.Logger) error {
	cl := mgr.GetClient()

	c, err := controller.New(SharedHostIDAllocatorCtrl, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log.Debug(fmt.Sprintf("[%s] got a request %s", SharedHostIDAllocatorCtrl, request.String()))

			if err := reconcileHostIDs(ctx, cl, log); err != nil {
				log.Error(err, fmt.Sprintf("[%s] unable to allocate host ids", SharedHostIDAllocatorCtrl))
				return reconcile.Result{}, err
			}

			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		return err
	}

	// Every group is reconciled through the same pass, because an id is unique
	// across the cluster and a decision for one group depends on all of them.
	if err := c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMSharedVolumeGroup{},
		&handler.TypedEnqueueRequestForObject[*v1alpha1.LVMSharedVolumeGroup]{})); err != nil {
		return err
	}

	return nil
}

// reconcileHostIDs allocates ids for every node of every shared group in one
// pass. It never renumbers a node that already has an id: a node holding a
// lockspace under one id and told to use another would have to be evacuated
// first, so a conflict is reported rather than resolved.
func reconcileHostIDs(ctx context.Context, cl client.Client, log logger.Logger) error {
	groups := &v1alpha1.LVMSharedVolumeGroupList{}
	if err := cl.List(ctx, groups); err != nil {
		return fmt.Errorf("list LVMSharedVolumeGroups: %w", err)
	}

	ceilingByNode := ceilingsByNode(groups.Items)
	if len(ceilingByNode) == 0 {
		return nil
	}

	nodes := &corev1.NodeList{}
	if err := cl.List(ctx, nodes); err != nil {
		return fmt.Errorf("list Nodes: %w", err)
	}

	taken := map[int]string{}
	needID := make([]string, 0, len(ceilingByNode))
	for _, node := range nodes.Items {
		id, err := hostIDOf(&node)
		if err != nil {
			// An unreadable annotation is left alone rather than overwritten:
			// it may be a value someone is in the middle of fixing by hand, and
			// guessing at it could point two nodes at one lease.
			log.Error(err, fmt.Sprintf("[%s] node %s carries an unusable host id annotation", SharedHostIDAllocatorCtrl, node.Name))
			continue
		}
		if id != 0 {
			if other, clash := taken[id]; clash {
				return fmt.Errorf("host id %d is claimed by both %s and %s; resolve by hand, "+
					"renumbering a node that may hold a lockspace is not safe", id, other, node.Name)
			}
			taken[id] = node.Name

			if ceiling, inPool := ceilingByNode[node.Name]; inPool && id > ceiling {
				// Lowering a pool's alignment below an already allocated id is
				// the way to get here. Renumbering would evict the node, so say
				// so and leave it.
				log.Error(fmt.Errorf("host id %d exceeds the ceiling %d", id, ceiling),
					fmt.Sprintf("[%s] node %s cannot join its pool with the id it already has", SharedHostIDAllocatorCtrl, node.Name))
			}
			continue
		}
		if _, inPool := ceilingByNode[node.Name]; inPool {
			needID = append(needID, node.Name)
		}
	}

	// Deterministic order, so that a reconcile that is retried hands out the
	// same ids as the one that failed halfway.
	sort.Strings(needID)

	for _, name := range needID {
		id, err := lowestFreeHostID(taken, ceilingByNode[name])
		if err != nil {
			return fmt.Errorf("node %s: %w", name, err)
		}

		node := &corev1.Node{}
		if err := cl.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("get Node %s: %w", name, err)
		}

		patch := client.MergeFrom(node.DeepCopy())
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		node.Annotations[SanlockHostIDAnnotation] = strconv.Itoa(id)
		if err := cl.Patch(ctx, node, patch); err != nil {
			return fmt.Errorf("annotate Node %s with host id %d: %w", name, id, err)
		}

		taken[id] = name
		log.Info(fmt.Sprintf("[%s] node %s got sanlock host id %d", SharedHostIDAllocatorCtrl, name, id))
	}

	return nil
}

// ceilingsByNode gives every node the smallest ceiling among the pools it
// belongs to. A node has one id for all its lockspaces, so it has to fit under
// all of their ceilings, and the tightest one decides. A node in no pool does
// not appear here and is allocated nothing.
func ceilingsByNode(groups []v1alpha1.LVMSharedVolumeGroup) map[string]int {
	out := map[string]int{}
	for i := range groups {
		ceiling := ceilingForGroup(&groups[i])
		for _, node := range groups[i].Spec.Nodes {
			if current, ok := out[node]; !ok || ceiling < current {
				out[node] = ceiling
			}
		}
	}
	return out
}

// ceilingForGroup reads the ceiling implied by a group's lease alignment. An
// unknown value falls back to the default rather than to the largest ceiling:
// handing out ids a lockspace cannot hold produces nodes that fail to join
// long after the mistake was made.
func ceilingForGroup(group *v1alpha1.LVMSharedVolumeGroup) int {
	align := defaultSanlockAlignSize
	if group.Spec.LVM != nil && group.Spec.LVM.SanlockAlignSize != "" {
		align = group.Spec.LVM.SanlockAlignSize
	}
	if ceiling, ok := hostIDCeilings[align]; ok {
		return ceiling
	}
	return hostIDCeilings[defaultSanlockAlignSize]
}

// hostIDOf returns the id a node already carries, or 0 when it carries none.
func hostIDOf(node *corev1.Node) (int, error) {
	raw, ok := node.Annotations[SanlockHostIDAnnotation]
	if !ok || raw == "" {
		return 0, nil
	}
	id, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("annotation %s of node %s is %q, not a number", SanlockHostIDAnnotation, node.Name, raw)
	}
	if id < 1 {
		return 0, fmt.Errorf("annotation %s of node %s is %d; sanlock host ids start at 1", SanlockHostIDAnnotation, node.Name, id)
	}
	return id, nil
}

// lowestFreeHostID picks the smallest unused id that fits under the ceiling.
// Lowest rather than next-after-the-highest, so that ids of decommissioned
// nodes come back into use instead of the pool drifting towards its ceiling.
func lowestFreeHostID(taken map[int]string, ceiling int) (int, error) {
	for id := 1; id <= ceiling; id++ {
		if _, used := taken[id]; !used {
			return id, nil
		}
	}
	return 0, fmt.Errorf("all %d host ids are in use; the ceiling comes from the lease alignment "+
		"and cannot be raised on an existing volume group", ceiling)
}
