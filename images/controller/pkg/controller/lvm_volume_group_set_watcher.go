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
	"maps"
	"reflect"
	"slices"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/config"
	"github.com/deckhouse/sds-node-configurator/images/controller/pkg/monitoring"
)

const (
	LVMVolumeGroupSetCtrlName = "lvm-volume-group-set-watcher-controller"

	phaseCreated    = "Created"
	phaseNotCreated = "NotCreated"

	reasonWorkInProgress = "WorkInProgress"
	strategyPerNode      = "PerNode"
)

func RunLVMVolumeGroupSetWatcher(
	mgr manager.Manager,
	log logger.Logger,
	cfg config.Options,
	metrics monitoring.Metrics,
) error {
	log = log.Named(LVMVolumeGroupSetCtrlName)
	cl := mgr.GetClient()
	apiReader := mgr.GetAPIReader()

	c, err := controller.New(LVMVolumeGroupSetCtrlName, mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
			log := log.Named("reconcile")

			log.Info(fmt.Sprintf("tries to reconcile the request of the LVMVolumeGroupSet %s", request.Name))
			lvgSet := &v1alpha1.LVMVolumeGroupSet{}
			err := cl.Get(ctx, request.NamespacedName, lvgSet)
			if err != nil {
				if errors.IsNotFound(err) {
					log.Warn(fmt.Sprintf("seems like the LVMVolumeGroupSet %s has been deleted. Stop the reconcile", lvgSet.Name))
					return reconcile.Result{}, nil
				}

				log.Error(fmt.Sprintf("unable to get the LVMVolumeGroupSet %s", request.Name), logger.Err(err))
				return reconcile.Result{}, err
			}

			shouldRequeue, err := reconcileLVMVolumeGroupSet(ctx, cl, apiReader, log, metrics, lvgSet)
			if err != nil {
				log.Error(fmt.Sprintf("unable to reconcile the LVMVolumeGroupSet %s", lvgSet.Name), logger.Err(err))
				return reconcile.Result{}, err
			}

			if shouldRequeue {
				log.Warn(fmt.Sprintf("the LVMVolumeGroupSet %s request should be requeued in %s", lvgSet.Name, cfg.ScanInterval.String()))
				return reconcile.Result{RequeueAfter: cfg.ScanInterval}, nil
			}

			log.Info(fmt.Sprintf("successfully reconciled the request of the LVMVolumeGroupSet %s", request.Name))
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		log.Error("unable to create the controller", logger.Err(err))
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMVolumeGroupSet{}, handler.TypedFuncs[*v1alpha1.LVMVolumeGroupSet, reconcile.Request]{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[*v1alpha1.LVMVolumeGroupSet], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.Named("create")

			log.Info(fmt.Sprintf("CreateFunc got a create event for the LVMVolumeGroupSet, name: %s", e.Object.GetName()))
			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.Object.GetNamespace(), Name: e.Object.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("CreateFunc added a request for the LVMVolumeGroupSet %s to the Reconcilers queue", e.Object.GetName()))
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[*v1alpha1.LVMVolumeGroupSet], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			log := log.Named("update")

			log.Info(fmt.Sprintf("UpdateFunc got a update event for the LVMVolumeGroupSet %s", e.ObjectNew.GetName()))
			if !shouldLVGSetWatcherReconcileUpdateEvent(e.ObjectOld, e.ObjectNew) {
				log.Info(fmt.Sprintf("update event for the LVMVolumeGroupSet %s should not be reconciled as not target changed were made", e.ObjectNew.Name))
				return
			}

			request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: e.ObjectNew.GetNamespace(), Name: e.ObjectNew.GetName()}}
			q.Add(request)
			log.Info(fmt.Sprintf("UpdateFunc added a request for the LVMVolumeGroupSet %s to the Reconcilers queue", e.ObjectNew.Name))
		},
	}))
	if err != nil {
		log.Error("the controller is unable to watch the LVMVolumeGroupSet resources", logger.Err(err))
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1alpha1.LVMVolumeGroup{},
		handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, lvg *v1alpha1.LVMVolumeGroup) []reconcile.Request {
			return lvgSetRequestsForLVG(ctx, cl, log, lvg)
		}),
		predicate.TypedFuncs[*v1alpha1.LVMVolumeGroup]{
			UpdateFunc: func(e event.TypedUpdateEvent[*v1alpha1.LVMVolumeGroup]) bool {
				return shouldLVGSetWatcherReconcileLVGUpdateEvent(e.ObjectOld, e.ObjectNew)
			},
		},
	))
	if err != nil {
		log.Error("the controller is unable to watch the LVMVolumeGroup resources", logger.Err(err))
		return err
	}

	err = c.Watch(source.Kind(mgr.GetCache(), &v1.Node{}, handler.TypedFuncs[*v1.Node, reconcile.Request]{
		CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[*v1.Node], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			addRequests(q, lvgSetRequestsForNodeLabels(ctx, cl, log, e.Object.Labels))
		},
		UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[*v1.Node], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			if maps.Equal(e.ObjectOld.Labels, e.ObjectNew.Labels) {
				return
			}
			addRequests(q, lvgSetRequestsForNodeLabels(ctx, cl, log, e.ObjectOld.Labels, e.ObjectNew.Labels))
		},
		DeleteFunc: func(ctx context.Context, e event.TypedDeleteEvent[*v1.Node], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			addRequests(q, lvgSetRequestsForNodeLabels(ctx, cl, log, e.Object.Labels))
		},
	}))
	if err != nil {
		log.Error("the controller is unable to watch the Node resources", logger.Err(err))
		return err
	}

	return nil
}

func addRequests(q workqueue.TypedRateLimitingInterface[reconcile.Request], requests []reconcile.Request) {
	for _, request := range requests {
		q.Add(request)
	}
}

// lvgSetRequestsForNodeLabels returns the sets whose nodeSelector matches any of
// the given label sets of a node. A set otherwise learns about a node only when
// the set or one of its LVMVolumeGroups changes, so a node that joined the
// cluster, or got the label, after the set was created waited for that.
func lvgSetRequestsForNodeLabels(ctx context.Context, cl client.Client, log logger.Logger, nodeLabels ...map[string]string) []reconcile.Request {
	lvgSets := &v1alpha1.LVMVolumeGroupSetList{}
	if err := cl.List(ctx, lvgSets); err != nil {
		log.Error("unable to list the LVMVolumeGroupSets for a node event", logger.Err(err))
		return nil
	}

	var requests []reconcile.Request
	for _, lvgSet := range lvgSets.Items {
		selector, err := metav1.LabelSelectorAsSelector(lvgSet.Spec.NodeSelector)
		if err != nil {
			log.Error(fmt.Sprintf("unable to parse the nodeSelector of the LVMVolumeGroupSet %s", lvgSet.Name), logger.Err(err))
			continue
		}
		for _, set := range nodeLabels {
			// GetNodes reads an empty selector as every node; so does this.
			if selector == labels.Nothing() || selector.Matches(labels.Set(set)) {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: lvgSet.Name}})
				break
			}
		}
	}

	return requests
}

func shouldLVGSetWatcherReconcileUpdateEvent(oldLVG, newLVG *v1alpha1.LVMVolumeGroupSet) bool {
	return !reflect.DeepEqual(oldLVG.Spec, newLVG.Spec)
}

// shouldLVGSetWatcherReconcileLVGUpdateEvent lets through the changes of an
// LVMVolumeGroup a set acts on: an edit of its spec, which the set brings back
// to the template, and a change of its phase, which the set counts.
func shouldLVGSetWatcherReconcileLVGUpdateEvent(oldLVG, newLVG *v1alpha1.LVMVolumeGroup) bool {
	return oldLVG.Generation != newLVG.Generation || oldLVG.Status.Phase != newLVG.Status.Phase
}

// lvgSetRequestsForLVG returns the sets whose template names the Volume Group of
// lvg. The name is immutable on both, so it finds the set of a group the set
// created as well as of one that is gone.
func lvgSetRequestsForLVG(ctx context.Context, cl client.Client, log logger.Logger, lvg *v1alpha1.LVMVolumeGroup) []reconcile.Request {
	lvgSets := &v1alpha1.LVMVolumeGroupSetList{}
	if err := cl.List(ctx, lvgSets); err != nil {
		log.Error(fmt.Sprintf("unable to list the LVMVolumeGroupSets for the LVMVolumeGroup %s", lvg.Name), logger.Err(err))
		return nil
	}

	var requests []reconcile.Request
	for _, lvgSet := range lvgSets.Items {
		if lvgSet.Spec.LVGTemplate.ActualVGNameOnTheNode == lvg.Spec.ActualVGNameOnTheNode {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: lvgSet.Name}})
		}
	}

	return requests
}

func reconcileLVMVolumeGroupSet(ctx context.Context, cl client.Client, lvgReader client.Reader, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet) (bool, error) {
	log.Debug(fmt.Sprintf("starts the reconciliation of the LVMVolumeGroupSet %s", lvgSet.Name))
	// Pending stands for a spec the controller has not acted on yet. The set is
	// also reconciled on every change of its LVMVolumeGroups, and those passes
	// must not take it out of Created and back.
	if lvgSet.Status.Phase == "" || lvgSet.Status.ObservedGeneration != lvgSet.Generation {
		err := updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, v1alpha1.PhasePending, reasonWorkInProgress)
		if err != nil {
			return false, err
		}
	}

	log.Debug(fmt.Sprintf("tries to get nodes by the LVMVolumeGroupSet %s nodeSelector", lvgSet.Name))
	nodes, err := GetNodes(ctx, cl, metrics, lvgSet.Spec.NodeSelector)
	if err != nil {
		return false, err
	}
	log.Debug(fmt.Sprintf("successfully got nodes by the LVMVolumeGroupSet %s nodeSelector", lvgSet.Name))
	log.Trace(fmt.Sprintf("nodes: %+v", nodes))

	log.Debug(fmt.Sprintf("starts to validate the LVMVolumeGroupSet %s nodes", lvgSet.Name))
	valid, reason := validateLVMVolumeGroupSetNodes(nodes)
	if !valid {
		log.Warn(fmt.Sprintf("the LVMVolumeGroupSet %s nodes are invalid: %s", lvgSet.Name, reason))
		// The groups the set already has stay where they are and are still
		// counted; only the desired count drops to the nodes that match.
		lvgs, err := GetLVMVolumeGroups(ctx, lvgReader, metrics)
		if err != nil {
			return false, err
		}
		err = updateLVMVolumeGroupSetLVGsIfNeeded(ctx, cl, log, lvgSet, lvgs, nodes)
		if err != nil {
			return false, err
		}

		err = updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseNotCreated, reason)
		if err != nil {
			return false, err
		}

		return true, nil
	}
	log.Debug(fmt.Sprintf("the LVMVolumeGroupSet %s nodes are valid", lvgSet.Name))

	log.Debug(fmt.Sprintf("tries to provide LVMVolumeGroups by the LVMVolumeGroupSet %s", lvgSet.Name))
	err = provideLVMVolumeGroupsBySet(ctx, cl, lvgReader, log, metrics, lvgSet, nodes)
	if err != nil {
		log.Error(fmt.Sprintf("unable to provide LVMVolumeGroups by LVMVolumeGroupSet %s", lvgSet.Name), logger.Err(err))
		updErr := updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseNotCreated, err.Error())
		if updErr != nil {
			return false, updErr
		}
		return false, err
	}
	log.Debug(fmt.Sprintf("successfully provided LVMVolumeGroups by the LVMVolumeGroupSet %s", lvgSet.Name))

	err = updateLVMVolumeGroupSetPhaseIfNeeded(ctx, cl, log, lvgSet, phaseCreated, "")
	if err != nil {
		return false, err
	}

	return false, nil
}

func provideLVMVolumeGroupsBySet(ctx context.Context, cl client.Client, lvgReader client.Reader, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet, nodes map[string]v1.Node) error {
	//nolint:gocritic
	switch lvgSet.Spec.Strategy {
	case strategyPerNode:
		log.Debug(fmt.Sprintf("the LVMVolumeGroupSet %s has strategy %s, tries to provide the LVMVolumeGroups", lvgSet.Name, strategyPerNode))
		err := provideLVMVolumeGroupsPerNode(ctx, cl, lvgReader, log, metrics, lvgSet, nodes)
		if err != nil {
			log.Error(fmt.Sprintf("unable to provide LVMVolumeGroups by the LVMVolumeGroupSet %s with strategy %s", lvgSet.Name, strategyPerNode), logger.Err(err))
			return err
		}
		log.Debug(fmt.Sprintf("successfully provided LVMVolumeGroups by the LVMVolumeGroupSet %s with strategy %s", lvgSet.Name, strategyPerNode))
	default:
		return fmt.Errorf("LVMVolumeGroupSet %s strategy %s is not implemented", lvgSet.Name, lvgSet.Spec.Strategy)
	}

	return nil
}

func provideLVMVolumeGroupsPerNode(ctx context.Context, cl client.Client, lvgReader client.Reader, log logger.Logger, metrics monitoring.Metrics, lvgSet *v1alpha1.LVMVolumeGroupSet, nodes map[string]v1.Node) error {
	// The LVMVolumeGroups are read past the cache. The set is reconciled again on
	// the events of the groups it has just created, and a cache that has not seen
	// all of them yet would have it create a second group on the same node.
	log.Debug("tries to get LVMVolumeGroups")
	currentLVGs, err := GetLVMVolumeGroups(ctx, lvgReader, metrics)
	if err != nil {
		log.Error("unable to get LVMVolumeGroups", logger.Err(err))
		return err
	}
	log.Debug("successfully got LVMVolumeGroups")
	log.Trace(fmt.Sprintf("current LVMVolumeGroups: %+v", currentLVGs))

	for _, nodeName := range slices.Sorted(maps.Keys(nodes)) {
		configuredLVG := configureLVGBySet(lvgSet, nodes[nodeName])
		log.Trace(fmt.Sprintf("configurated LVMVolumeGroup: %+v", configuredLVG))

		currentLVG := matchConfiguredLVGWithExistingOne(configuredLVG, currentLVGs)
		if currentLVG != nil {
			log.Debug(fmt.Sprintf("tries to update the LVMVolumeGroup %s", currentLVG.Name))
			updated, err := updateLVMVolumeGroupByConfiguredFromSet(ctx, cl, currentLVG, configuredLVG)
			if err != nil {
				log.Error(fmt.Sprintf("unable to update the LVMVolumeGroup %s", currentLVG.Name), logger.Err(err))
				return err
			}
			if updated {
				log.Info(fmt.Sprintf("LVMVolumeGroup %s has been successfully updated", currentLVG.Name))
			}
			continue
		}

		configuredLVG.Name = configureLVGNameFromSet(lvgSet, currentLVGs)
		log.Debug(fmt.Sprintf("tries to create the LVMVolumeGroup %s", configuredLVG.Name))
		err = createLVMVolumeGroup(ctx, cl, configuredLVG)
		if err != nil {
			log.Error(fmt.Sprintf("unable to create the LVMVolumeGroup %s", configuredLVG.Name), logger.Err(err))
			return err
		}
		currentLVGs[configuredLVG.Name] = *configuredLVG
		log.Info(fmt.Sprintf("the LVMVolumeGroup %s has been created", configuredLVG.Name))
	}

	log.Debug(fmt.Sprintf("tries to update the LVMVolumeGroupSet %s status by its LVMVolumeGroups", lvgSet.Name))
	err = updateLVMVolumeGroupSetLVGsIfNeeded(ctx, cl, log, lvgSet, currentLVGs, nodes)
	if err != nil {
		log.Error(fmt.Sprintf("unable to update the LVMVolumeGroupSet %s status", lvgSet.Name), logger.Err(err))
		return err
	}

	return nil
}

func matchConfiguredLVGWithExistingOne(lvg *v1alpha1.LVMVolumeGroup, lvgs map[string]v1alpha1.LVMVolumeGroup) *v1alpha1.LVMVolumeGroup {
	for _, l := range lvgs {
		if l.Spec.Local.NodeName == lvg.Spec.Local.NodeName && l.Spec.ActualVGNameOnTheNode == lvg.Spec.ActualVGNameOnTheNode {
			return &l
		}
	}

	return nil
}

// updateLVMVolumeGroupByConfiguredFromSet brings the fields the set manages back
// to the template. They are compared by value: the API server drops an empty
// fileDevices list and writes sizes back in their canonical form.
func updateLVMVolumeGroupByConfiguredFromSet(ctx context.Context, cl client.Client, existing, configured *v1alpha1.LVMVolumeGroup) (bool, error) {
	if equality.Semantic.DeepEqual(existing.Spec.ThinPools, configured.Spec.ThinPools) &&
		equality.Semantic.DeepEqual(existing.Spec.BlockDeviceSelector, configured.Spec.BlockDeviceSelector) &&
		equality.Semantic.DeepEqual(existing.Spec.FileDevices, configured.Spec.FileDevices) {
		return false, nil
	}

	existing.Spec.ThinPools = configured.Spec.ThinPools
	existing.Spec.BlockDeviceSelector = configured.Spec.BlockDeviceSelector
	existing.Spec.FileDevices = configured.Spec.FileDevices

	return true, cl.Update(ctx, existing)
}

// updateLVMVolumeGroupSetLVGsIfNeeded records in the status the LVMVolumeGroups
// of the set as they are now. lvgs are all the LVMVolumeGroups of the cluster.
func updateLVMVolumeGroupSetLVGsIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvgSet *v1alpha1.LVMVolumeGroupSet, lvgs map[string]v1alpha1.LVMVolumeGroup, nodes map[string]v1.Node) error {
	clusterNodes := &v1.NodeList{}
	if err := cl.List(ctx, clusterNodes); err != nil {
		return fmt.Errorf("listing the nodes: %w", err)
	}
	existing := make(map[string]struct{}, len(clusterNodes.Items))
	for _, n := range clusterNodes.Items {
		existing[n.Name] = struct{}{}
	}

	// The state UpdateStatus read and wrote, kept so the server-side values can be
	// mirrored onto the caller's object below.
	var written *v1alpha1.LVMVolumeGroupSet

	err := conditions.UpdateStatus(ctx, cl, lvgSet, func(fresh *v1alpha1.LVMVolumeGroupSet) {
		written = fresh
		setLVMVolumeGroupSetLVGs(fresh, lvgs, nodes, existing)
	})
	if err != nil {
		return err
	}

	lvgSet.Status = written.Status
	lvgSet.ResourceVersion = written.ResourceVersion

	log.Debug(fmt.Sprintf("the LVMVolumeGroupSet %s has %d LVMVolumeGroups, %d of them Ready, of %d desired", lvgSet.Name,
		lvgSet.Status.CurrentLVMVolumeGroupsCount, lvgSet.Status.ReadyLVMVolumeGroupsCount, lvgSet.Status.DesiredLVMVolumeGroupsCount))
	return nil
}

// setLVMVolumeGroupSetLVGs lists in the status the groups the set created that
// still exist, together with the group it keeps on each of its nodes, and counts
// how many of them are Ready.
//
// A group the set finds on its node under another name, left by an earlier pass
// whose status write was lost or created before the set, is the one the set
// keeps there and is listed with the rest.
//
// A group whose node is no longer in the cluster, existingNodes being every node
// there is, is dropped from the list but not deleted: its Volume Group outlives
// the node, and the node may come back under the same name.
func setLVMVolumeGroupSetLVGs(lvgSet *v1alpha1.LVMVolumeGroupSet, lvgs map[string]v1alpha1.LVMVolumeGroup, nodes map[string]v1.Node, existingNodes map[string]struct{}) {
	listed := make([]v1alpha1.LVMVolumeGroupSetStatusLVG, 0, len(nodes))
	for _, entry := range lvgSet.Status.CreatedLVGs {
		// The node is taken from the group, not from the entry: a name the set
		// gave back to a new group may now stand for one on another node.
		if lvg, exists := lvgs[entry.LVMVolumeGroupName]; exists {
			if _, onNode := existingNodes[lvg.Spec.Local.NodeName]; !onNode {
				continue
			}
			listed = append(listed, v1alpha1.LVMVolumeGroupSetStatusLVG{
				LVMVolumeGroupName: lvg.Name,
				NodeName:           lvg.Spec.Local.NodeName,
			})
		}
	}

	for _, nodeName := range slices.Sorted(maps.Keys(nodes)) {
		lvg := matchConfiguredLVGWithExistingOne(configureLVGBySet(lvgSet, nodes[nodeName]), lvgs)
		if lvg == nil || slices.ContainsFunc(listed, func(entry v1alpha1.LVMVolumeGroupSetStatusLVG) bool {
			return entry.LVMVolumeGroupName == lvg.Name
		}) {
			continue
		}
		listed = append(listed, v1alpha1.LVMVolumeGroupSetStatusLVG{
			LVMVolumeGroupName: lvg.Name,
			NodeName:           lvg.Spec.Local.NodeName,
		})
	}

	ready := 0
	for _, entry := range listed {
		if lvgs[entry.LVMVolumeGroupName].Status.Phase == v1alpha1.PhaseReady {
			ready++
		}
	}

	lvgSet.Status.CreatedLVGs = listed
	lvgSet.Status.CurrentLVMVolumeGroupsCount = len(listed)
	lvgSet.Status.DesiredLVMVolumeGroupsCount = len(nodes)
	lvgSet.Status.ReadyLVMVolumeGroupsCount = ready
}

func createLVMVolumeGroup(ctx context.Context, cl client.Client, lvg *v1alpha1.LVMVolumeGroup) error {
	return cl.Create(ctx, lvg)
}

// configureLVGBySet returns the LVMVolumeGroup the set keeps on node, without a
// name: the name is chosen only when the group is created.
func configureLVGBySet(lvgSet *v1alpha1.LVMVolumeGroupSet, node v1.Node) *v1alpha1.LVMVolumeGroup {
	return &v1alpha1.LVMVolumeGroup{
		ObjectMeta: metav1.ObjectMeta{
			Labels: lvgSet.Spec.LVGTemplate.Metadata.Labels,
		},
		Spec: v1alpha1.LVMVolumeGroupSpec{
			ActualVGNameOnTheNode: lvgSet.Spec.LVGTemplate.ActualVGNameOnTheNode,
			BlockDeviceSelector:   lvgSet.Spec.LVGTemplate.BlockDeviceSelector,
			ThinPools:             lvgSet.Spec.LVGTemplate.ThinPools,
			Type:                  lvgSet.Spec.LVGTemplate.Type,
			FileDevices:           lvgSet.Spec.LVGTemplate.FileDevices,
			Local: v1alpha1.LVMVolumeGroupLocalSpec{
				NodeName: node.Name,
			},
		},
	}
}

// configureLVGNameFromSet returns the first name of the form <set>-<n> that no
// LVMVolumeGroup has. A group deleted from the set gets its name back.
func configureLVGNameFromSet(lvgSet *v1alpha1.LVMVolumeGroupSet, lvgs map[string]v1alpha1.LVMVolumeGroup) string {
	for i := 0; ; i++ {
		name := fmt.Sprintf("%s-%d", lvgSet.Name, i)
		if _, taken := lvgs[name]; !taken {
			return name
		}
	}
}

func GetNodes(ctx context.Context, cl client.Client, metrics monitoring.Metrics, selector *metav1.LabelSelector) (map[string]v1.Node, error) {
	list := &v1.NodeList{}
	s, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, err
	}
	if s == labels.Nothing() {
		s = nil
	}
	start := time.Now()
	err = cl.List(ctx, list, &client.ListOptions{LabelSelector: s})
	metrics.APIMethodsDuration(LVMVolumeGroupSetCtrlName, "list").Observe(metrics.GetEstimatedTimeInSeconds(start))
	metrics.APIMethodsExecutionCount(LVMVolumeGroupSetCtrlName, "list").Inc()
	if err != nil {
		metrics.APIMethodsErrors(LVMVolumeGroupSetCtrlName, "list").Inc()
		return nil, err
	}

	result := make(map[string]v1.Node, len(list.Items))
	for _, item := range list.Items {
		result[item.Name] = item
	}

	return result, nil
}

// readyConditionForSetPhase translates a phase this controller is about to write
// into the Ready condition that goes with it.
//
// The phase is derived from the condition in the resources that were built this
// way from the start. Here it is the other way round, because the phase came
// first and is the load-bearing contract: the strings are part of the published
// API and are what operators watch. Deriving the condition from the phase keeps
// the two in step by construction, with one place to look.
//
// reason carries free-form text, including raw error strings, so it becomes the
// condition's message rather than its reason.
func readyConditionForSetPhase(generation int64, phase, reason string) metav1.Condition {
	cond := metav1.Condition{
		Type:               conditions.TypeReady,
		ObservedGeneration: generation,
		Message:            reason,
	}

	switch phase {
	case phaseCreated:
		cond.Status = metav1.ConditionTrue
		cond.Reason = conditions.ReasonReconciled
	case v1alpha1.PhasePending:
		cond.Status = metav1.ConditionFalse
		cond.Reason = conditions.ReasonPending
	case phaseNotCreated:
		cond.Status = metav1.ConditionFalse
		cond.Reason = conditions.ReasonReconcileFailed
	default:
		// Nothing has observed the set yet. The phase enum admits the empty
		// string, so this is reachable rather than defensive.
		cond.Status = metav1.ConditionUnknown
		cond.Reason = conditions.ReasonPending
	}

	if cond.Message == "" {
		cond.Message = fmt.Sprintf("the LVMVolumeGroupSet is in the %s phase", phase)
	}

	// reason is free-form and carries raw error strings, while the schema caps
	// the condition message at 32768. Over the cap the API server rejects the
	// whole status write, so the set would keep reporting its previous verdict
	// and the reconcile would fail on the write rather than on what actually
	// went wrong. conditions.Set does not truncate: it is a thin wrapper over
	// meta.SetStatusCondition.
	cond.Message = conditions.TruncateMessage(cond.Message)

	return cond
}

func updateLVMVolumeGroupSetPhaseIfNeeded(ctx context.Context, cl client.Client, log logger.Logger, lvgSet *v1alpha1.LVMVolumeGroupSet, phase, reason string) error {
	log.Debug(fmt.Sprintf("tries to update the LVMVolumeGroupSet %s status phase to %s and reason to %s", lvgSet.Name, phase, reason))

	// The state UpdateStatus read and wrote, kept so the server-side values can be
	// mirrored onto the caller's object below.
	var written *v1alpha1.LVMVolumeGroupSet

	err := conditions.UpdateStatus(ctx, cl, lvgSet, func(fresh *v1alpha1.LVMVolumeGroupSet) {
		written = fresh
		fresh.Status.Phase = phase
		fresh.Status.Reason = reason
		fresh.Status.ObservedGeneration = fresh.Generation
		conditions.Set(&fresh.Status.Conditions, readyConditionForSetPhase(fresh.Generation, phase, reason))
	})
	if err != nil {
		log.Error(fmt.Sprintf("unable to update the LVMVolumeGroupSet %s", lvgSet.Name), logger.Err(err))
		return err
	}

	lvgSet.Status = written.Status
	lvgSet.ResourceVersion = written.ResourceVersion

	log.Debug(fmt.Sprintf("successfully updated the LVMVolumeGroupSet %s to phase %s and reason %s", lvgSet.Name, phase, reason))
	return nil
}

func validateLVMVolumeGroupSetNodes(nodes map[string]v1.Node) (bool, string) {
	if len(nodes) == 0 {
		return false, "no nodes found by specified nodeSelector"
	}

	return true, ""
}
