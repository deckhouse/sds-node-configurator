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

// Package heartbeat publishes the one fact about this agent that other modules
// need: it is running.
//
// A Lease and not an annotation, and the difference is the whole point. An
// annotation saying "alive" keeps saying it after the process is gone, so a
// reader has to invent a timestamp convention and a staleness rule of its own —
// and every reader invents a different one. A Lease has the lifetime built in:
// a renewal that stopped happening is visible without anyone agreeing on
// anything.
//
// It also keeps the heartbeat off the Node object. Sixteen to thirty-two agents
// renewing an annotation on their own Node would each be rewriting an object
// that every controller in the cluster watches; a Lease in this module's own
// namespace is watched by the handful of things that care.
package heartbeat

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

// LeasePrefix plus the node name is the Lease this publishes. The name is
// derived so that a reader who knows the node knows where to look, without a
// label selector and without listing.
const LeasePrefix = "sds-node-configurator-agent-"

// Publisher renews the agent's Lease until its context is cancelled.
type Publisher struct {
	cl        client.Client
	log       logger.Logger
	nodeName  string
	namespace string
	// interval must stay several times below whatever a reader treats as stale.
	// A missed renewal is a busy node, not a dead agent, and the cost of reading
	// it as death is a node evicted from a storage pool.
	interval time.Duration
}

func NewPublisher(cl client.Client, log logger.Logger, nodeName, namespace string, interval time.Duration) *Publisher {
	return &Publisher{cl: cl, log: log, nodeName: nodeName, namespace: namespace, interval: interval}
}

// Run publishes immediately and then on every tick.
//
// Immediately matters: the pool a node belongs to cannot admit it until this
// exists, so waiting one interval before the first write would add that interval
// to every node's startup.
func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		if err := p.renew(ctx); err != nil {
			// Not fatal. The agent's work does not depend on being seen, and a
			// reader that cannot see it will simply not use this node yet.
			p.log.Error(err, fmt.Sprintf("[heartbeat] unable to renew the lease of %s", p.nodeName))
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Publisher) renew(ctx context.Context) error {
	name := LeasePrefix + p.nodeName
	now := metav1.NewMicroTime(time.Now())
	seconds := int32(p.interval.Seconds())

	lease := &coordinationv1.Lease{}
	err := p.cl.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: name}, lease)
	if apierrors.IsNotFound(err) {
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: p.namespace,
				Name:      name,
				Labels: map[string]string{
					"app":                       "sds-node-configurator",
					"storage.deckhouse.io/node": p.nodeName,
				},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &p.nodeName,
				LeaseDurationSeconds: &seconds,
				RenewTime:            &now,
			},
		}
		if err := p.cl.Create(ctx, lease); err != nil {
			return fmt.Errorf("create lease %s: %w", name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("get lease %s: %w", name, err)
	}

	patch := client.MergeFrom(lease.DeepCopy())
	lease.Spec.HolderIdentity = &p.nodeName
	lease.Spec.LeaseDurationSeconds = &seconds
	lease.Spec.RenewTime = &now
	if err := p.cl.Patch(ctx, lease, patch); err != nil {
		return fmt.Errorf("renew lease %s: %w", name, err)
	}

	return nil
}
