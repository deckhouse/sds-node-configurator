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

package leader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/deckhouse/sds-common-lib/logger"
)

func TestAddressFromIdentity(t *testing.T) {
	for _, tc := range []struct {
		identity string
		want     string
		ok       bool
	}{
		{identity: Identity("extender-7d9f-abcde", "10.111.0.12"), want: "10.111.0.12:8099", ok: true},
		{identity: Identity("extender-7d9f-abcde", "fd00::12"), want: "[fd00::12]:8099", ok: true},
		{identity: "extender-7d9f-abcde"},
		{identity: "extender-7d9f-abcde_"},
		{identity: "_10.111.0.12"},
		{identity: "extender_not-an-ip"},
		// The identity controller-runtime writes: hostname_uuid.
		{identity: "extender-7d9f-abcde_0f6b3c1e-2b1f-4c55-9d5e-6f1d2a3b4c5d"},
	} {
		t.Run(tc.identity, func(t *testing.T) {
			got, ok := addressFromIdentity(tc.identity, "8099")
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestObserveLeader(t *testing.T) {
	s := NewState(logger.NewNop(), Identity("a", "10.0.0.1"), "8099")
	assert.Empty(t, s.LeaderAddress(), "no leader is known before the first observation")

	s.observeLeader(Identity("b", "10.0.0.2"))
	assert.Equal(t, "10.0.0.2:8099", s.LeaderAddress())

	s.observeLeader(s.Identity())
	assert.Empty(t, s.LeaderAddress(), "the leader does not forward to itself")

	s.observeLeader("foreign-holder")
	assert.Empty(t, s.LeaderAddress(), "a holder without an address must not keep the previous leader's address")
}

// TestStartedLeadingAfterRenewalStopped covers the order client-go does not
// guarantee: OnStartedLeading runs in its own goroutine and may start after the
// lease is already gone. The flag must not be left set.
func TestStartedLeadingAfterRenewalStopped(t *testing.T) {
	s := NewState(logger.NewNop(), Identity("a", "10.0.0.1"), "8099")
	cb := s.callbacks()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cb.OnStoppedLeading()
	cb.OnStartedLeading(ctx)

	assert.False(t, s.IsLeader())
}

func newLock(clientset *fake.Clientset, identity string) resourcelock.Interface {
	return &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: "sds-common-scheduler-extender", Namespace: "d8-sds-node-configurator"},
		Client:     clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}
}

// TestElectionHandover runs two replicas against one Lease: exactly one leads,
// the other learns where to forward, and a leader that shuts down hands the lease
// over within a retry period instead of a whole lease duration.
func TestElectionHandover(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for real leader election retry periods")
	}
	clientset := fake.NewClientset()

	a := NewState(logger.NewNop(), Identity("a", "10.0.0.1"), "8099")
	b := NewState(logger.NewNop(), Identity("b", "10.0.0.2"), "8099")

	ctxA, stopA := context.WithCancel(context.Background())
	defer stopA()
	doneA := make(chan error, 1)
	go func() { doneA <- a.Run(ctxA, newLock(clientset, a.Identity())) }()

	require.Eventually(t, a.IsLeader, 5*time.Second, 50*time.Millisecond, "the only candidate must become the leader")

	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	doneB := make(chan error, 1)
	go func() { doneB <- b.Run(ctxB, newLock(clientset, b.Identity())) }()

	require.Eventually(t, func() bool { return b.LeaderAddress() == "10.0.0.1:8099" }, 5*time.Second, 50*time.Millisecond,
		"the standby must learn the address of the leader")
	assert.False(t, b.IsLeader(), "two replicas must never lead at once")
	assert.True(t, a.IsLeader())

	stopA()
	require.NoError(t, <-doneA, "a cancelled leader returns without an error")
	assert.False(t, a.IsLeader())

	// Released on cancel: the standby does not wait for LeaseDuration.
	require.Eventually(t, b.IsLeader, RetryPeriod*2+time.Second, 50*time.Millisecond,
		"the standby must take over the released lease within a retry period")
	assert.Empty(t, b.LeaderAddress())

	stopB()
	require.NoError(t, <-doneB)
}
