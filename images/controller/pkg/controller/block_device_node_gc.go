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
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/deckhouse/sds-common-lib/conditions"
	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
	"github.com/deckhouse/sds-node-configurator/images/controller/config"
)

const (
	BlockDeviceNodeGCCtrlName = "block-device-node-gc"

	// blockDeviceNodeGCInterval is how often the BlockDevices are checked
	// against the nodes. The retention is counted in hours, so a minute adds
	// nothing that matters to it.
	blockDeviceNodeGCInterval = time.Minute

	// nodeMissingGracePeriod is how long a node has to stay without an agent
	// before its BlockDevices go Missing=True. It is the agent's own
	// missingGracePeriod: an agent Pod replaced during a rollout leaves its
	// node without one for a few seconds, and that must not count.
	nodeMissingGracePeriod = 10 * time.Minute
)

// RunBlockDeviceNodeGC starts the collection of the BlockDevices of nodes no
// agent runs on.
//
// The agent of a node marks the BlockDevices whose devices left the node
// Missing and deletes them after the retention. A node that is deleted, or that
// loses the labels the agent DaemonSet is placed by, has no agent left to do it,
// and its BlockDevices stayed forever. The controller does the same for them:
// Missing=Unknown first, True after nodeMissingGracePeriod, and deletion once
// Missing has been True for the retention and no LVMVolumeGroup references the
// BlockDevice.
//
// Whether a node has an agent is read from the agent Pods rather than from the
// node labels, so the DaemonSet's placement stays its own business.
func RunBlockDeviceNodeGC(mgr manager.Manager, log logger.Logger, cfg config.Options) error {
	selector, err := labels.Parse(config.AgentPodLabelSelector)
	if err != nil {
		return fmt.Errorf("parsing the agent Pod selector: %w", err)
	}
	return mgr.Add(&blockDeviceNodeGC{
		cl:        mgr.GetClient(),
		reader:    mgr.GetAPIReader(),
		log:       log.Named(BlockDeviceNodeGCCtrlName),
		namespace: cfg.AgentNamespace,
		selector:  selector,
		retention: cfg.MissingBlockDeviceRetention,
		interval:  blockDeviceNodeGCInterval,
	})
}

type blockDeviceNodeGC struct {
	cl        client.Client
	reader    client.Reader
	log       logger.Logger
	namespace string
	selector  labels.Selector
	retention time.Duration
	interval  time.Duration
}

// NeedLeaderElection keeps the collection to one controller replica.
func (g *blockDeviceNodeGC) NeedLeaderElection() bool { return true }

func (g *blockDeviceNodeGC) Start(ctx context.Context) error {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		if err := g.pass(ctx, time.Now()); err != nil {
			g.log.Error("unable to check the BlockDevices of nodes without an agent", logger.Err(err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// pass marks and deletes what nodeGCDecide says for every BlockDevice. A failed
// list stops the pass before anything is written, and so does finding no agent
// Pod at all, which is a DaemonSet being recreated or a wrong namespace far more
// likely than every node of the cluster losing its agent: either way every node
// would look abandoned. Without the LVMVolumeGroups nothing proves a
// BlockDevice unreferenced.
func (g *blockDeviceNodeGC) pass(ctx context.Context, now time.Time) error {
	pods := &v1.PodList{}
	if err := g.reader.List(ctx, pods, client.InNamespace(g.namespace), client.MatchingLabelsSelector{Selector: g.selector}); err != nil {
		return fmt.Errorf("listing the agent Pods: %w", err)
	}
	nodes := &v1.NodeList{}
	if err := g.cl.List(ctx, nodes); err != nil {
		return fmt.Errorf("listing the nodes: %w", err)
	}
	blockDevices := &v1alpha1.BlockDeviceList{}
	if err := g.cl.List(ctx, blockDevices); err != nil {
		return fmt.Errorf("listing the BlockDevices: %w", err)
	}
	lvgs := &v1alpha1.LVMVolumeGroupList{}
	if err := g.cl.List(ctx, lvgs); err != nil {
		return fmt.Errorf("listing the LVMVolumeGroups: %w", err)
	}

	agentNodes := agentNodeNames(pods.Items)
	if len(agentNodes) == 0 {
		g.log.Warn(fmt.Sprintf("no agent Pods found in the namespace %s; not checking the BlockDevices in this pass", g.namespace))
		return nil
	}
	nodeNames := make(map[string]struct{}, len(nodes.Items))
	for _, n := range nodes.Items {
		nodeNames[n.Name] = struct{}{}
	}
	refs := newLVGReferences(lvgs.Items)

	for i := range blockDevices.Items {
		bd := &blockDevices.Items[i]
		reason := nodeLossReason(bd.Status.NodeName, nodeNames, agentNodes)
		if reason == "" {
			continue
		}
		switch nodeGCDecide(bd, reason, refs, now, g.retention) {
		case nodeGCMark:
			if err := g.cl.Update(ctx, bd); client.IgnoreNotFound(err) != nil {
				g.log.Error(fmt.Sprintf("unable to mark the BlockDevice %s Missing", bd.Name), logger.Err(err))
				continue
			}
			cond := conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)
			g.log.Warn(fmt.Sprintf("BlockDevice %s marked %s=%s: %s", bd.Name, cond.Type, cond.Status, cond.Message))
		case nodeGCDelete:
			cond := conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)
			if err := g.cl.Delete(ctx, bd); client.IgnoreNotFound(err) != nil {
				g.log.Error(fmt.Sprintf("unable to delete the BlockDevice %s", bd.Name), logger.Err(err))
				continue
			}
			g.log.Info(fmt.Sprintf(
				"deleted the BlockDevice %s of the node %s: %s=True (%s) since %s, longer than the retention of %s, and no LVMVolumeGroup references it",
				bd.Name, bd.Status.NodeName, cond.Type, cond.Reason, cond.LastTransitionTime.UTC().Format(time.RFC3339), g.retention,
			))
		}
	}
	return nil
}

// agentNodeNames returns the nodes an agent Pod is placed on. A Pod in any
// phase counts: one that crash-loops or is being replaced still means the node
// is the agent's, and its BlockDevices are its agent's to handle.
func agentNodeNames(pods []v1.Pod) map[string]struct{} {
	names := make(map[string]struct{}, len(pods))
	for _, p := range pods {
		if p.Spec.NodeName != "" {
			names[p.Spec.NodeName] = struct{}{}
		}
	}
	return names
}

// nodeLossReason returns why nodeName has no agent, or "" when it has one. A
// BlockDevice without a node name is left alone: it names no node to be lost.
func nodeLossReason(nodeName string, nodes, agentNodes map[string]struct{}) string {
	if nodeName == "" {
		return ""
	}
	if _, ok := agentNodes[nodeName]; ok {
		return ""
	}
	if _, ok := nodes[nodeName]; !ok {
		return v1alpha1.ReasonNodeNotFound
	}
	return v1alpha1.ReasonAgentNotRunning
}

type nodeGCAction int

const (
	nodeGCNone nodeGCAction = iota
	nodeGCMark
	nodeGCDelete
)

// nodeGCDecide moves the Missing condition of bd, a BlockDevice of a node
// without an agent, one step and says what to write.
//
// Missing=True already set, whatever its reason, is kept and counted from: a
// device that left the node before the node itself did has waited that long
// already. Otherwise the condition goes Unknown with reason and turns True once
// it has been Unknown for nodeMissingGracePeriod. A True condition older than
// retention deletes bd unless an LVMVolumeGroup references it; retention <= 0
// disables the deletion, as it does in the agent.
//
// Nothing here clears the condition when the agent comes back: the agent's own
// scan removes Missing from every device it finds and handles the rest.
func nodeGCDecide(bd *v1alpha1.BlockDevice, reason string, refs lvgReferences, now time.Time, retention time.Duration) nodeGCAction {
	published := conditions.Get(bd.Status.Conditions, v1alpha1.BlockDeviceConditionMissing)

	if published != nil && published.Status == metav1.ConditionTrue {
		if retention <= 0 || now.Sub(published.LastTransitionTime.Time) < retention || refs.holds(bd) {
			return nodeGCNone
		}
		return nodeGCDelete
	}

	sameReason := published != nil && published.Reason == reason
	if sameReason && now.Sub(published.LastTransitionTime.Time) < nodeMissingGracePeriod {
		return nodeGCNone
	}

	cond := metav1.Condition{
		Type:               v1alpha1.BlockDeviceConditionMissing,
		Status:             metav1.ConditionUnknown,
		Reason:             reason,
		Message:            nodeLossMessage(bd.Status.NodeName, reason),
		ObservedGeneration: bd.Generation,
		LastTransitionTime: metav1.NewTime(now),
	}
	if sameReason {
		cond.Status = metav1.ConditionTrue
		cond.Message = fmt.Sprintf("%s for over %s", cond.Message, nodeMissingGracePeriod)
	}
	if !conditions.Set(&bd.Status.Conditions, cond) {
		return nodeGCNone
	}
	return nodeGCMark
}

func nodeLossMessage(nodeName, reason string) string {
	if reason == v1alpha1.ReasonNodeNotFound {
		return fmt.Sprintf("node %q is not in the cluster", nodeName)
	}
	return fmt.Sprintf("no agent Pod runs on node %q", nodeName)
}

// lvgReferences is what the LVMVolumeGroups say about BlockDevices: which ones
// they list as their Physical Volumes, and which LVMVolumeGroup names exist. It
// mirrors the agent's: a BlockDevice is kept while an LVMVolumeGroup lists it in
// status.nodes or its status.lvmVolumeGroupName names an existing one, so a
// Volume Group left on a node that is gone stays visible until its
// LVMVolumeGroup is deleted.
type lvgReferences struct {
	blockDevices map[string]struct{}
	names        map[string]struct{}
}

func newLVGReferences(lvgs []v1alpha1.LVMVolumeGroup) lvgReferences {
	refs := lvgReferences{
		blockDevices: make(map[string]struct{}),
		names:        make(map[string]struct{}, len(lvgs)),
	}
	for _, lvg := range lvgs {
		refs.names[lvg.Name] = struct{}{}
		for _, node := range lvg.Status.Nodes {
			for _, dev := range node.Devices {
				refs.blockDevices[dev.BlockDevice] = struct{}{}
			}
		}
	}
	return refs
}

func (r lvgReferences) holds(bd *v1alpha1.BlockDevice) bool {
	if _, ok := r.blockDevices[bd.Name]; ok {
		return true
	}
	if name := bd.Status.LVMVolumeGroupName; name != "" {
		if _, ok := r.names[name]; ok {
			return true
		}
	}
	return false
}
