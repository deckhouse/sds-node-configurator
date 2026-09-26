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

// Package leader elects the one replica of the extender that holds the space
// reservations. The reservations live in process memory, so two replicas that
// both answered reservation requests would each see only half of them and
// together promise the same free space twice.
package leader

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/deckhouse/sds-common-lib/logger"
)

// The timings are the client-go and kube-scheduler defaults. A graceful handover
// (the leader releases the lease on shutdown) takes one RetryPeriod; a leader that
// dies without releasing is replaced once its lease expires, at most
// LeaseDuration + RetryPeriod after its last renewal. Shorter values would make a
// slow API server strip the leader of the lease more often, and every loss restarts
// the process and drops the reservations it held.
const (
	LeaseDuration = 15 * time.Second
	RenewDeadline = 10 * time.Second
	RetryPeriod   = 2 * time.Second
)

// ErrLeadershipLost is returned by Run when the lease could not be renewed. The
// caller is expected to exit: the reservations of this process are no longer
// authoritative and another replica may already be serving.
var ErrLeadershipLost = errors.New("leader election lost")

// identitySeparator splits the pod name from the pod IP in a lease holder
// identity. Neither a pod name (a DNS subdomain) nor an IP address contains it.
const identitySeparator = "_"

// Identity is the lease holder identity of a replica. It carries the pod IP so
// that the other replicas know where to forward reservation requests without
// looking the pod up.
func Identity(podName, podIP string) string {
	return podName + identitySeparator + podIP
}

// addressFromIdentity returns host:port of the replica that holds the given
// identity, or false when the identity was not written by Identity.
func addressFromIdentity(identity, port string) (string, bool) {
	i := strings.LastIndex(identity, identitySeparator)
	if i <= 0 || i == len(identity)-1 {
		return "", false
	}
	ip := identity[i+1:]
	if net.ParseIP(ip) == nil {
		return "", false
	}
	return net.JoinHostPort(ip, port), true
}

// State tells whether this replica is the leader and, when it is not, where the
// leader listens. It is safe for concurrent use.
type State struct {
	identity string
	port     string
	log      logger.Logger

	leader     atomic.Bool
	leaderAddr atomic.Pointer[string]
}

// NewState returns the state of a replica with the given identity whose peers
// serve on the given port.
func NewState(log logger.Logger, identity, port string) *State {
	s := &State{identity: identity, port: port, log: log}
	empty := ""
	s.leaderAddr.Store(&empty)
	return s
}

// IsLeader reports whether this replica currently holds the lease.
func (s *State) IsLeader() bool {
	return s.leader.Load()
}

// LeaderAddress returns host:port of the current leader when it is another
// replica, and "" when this replica is the leader or no leader is known.
func (s *State) LeaderAddress() string {
	return *s.leaderAddr.Load()
}

// Identity returns the lease holder identity of this replica.
func (s *State) Identity() string {
	return s.identity
}

func (s *State) observeLeader(identity string) {
	addr := ""
	switch identity {
	case s.identity, "":
	default:
		var ok bool
		addr, ok = addressFromIdentity(identity, s.port)
		if !ok {
			s.log.Warn(fmt.Sprintf("the lease is held by %q, which carries no address; reservation requests are refused until it changes", identity))
		}
	}
	s.leaderAddr.Store(&addr)
	s.log.Info(fmt.Sprintf("the leader is %q", identity))
}

// callbacks ties the leader flag to the context client-go hands to
// OnStartedLeading: client-go starts that callback in a goroutine and cancels its
// context when renewal stops, so the flag cannot outlive the lease even when the
// goroutine is scheduled after the renewal already failed.
func (s *State) callbacks() leaderelection.LeaderCallbacks {
	return leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			if ctx.Err() != nil {
				return
			}
			s.leader.Store(true)
			s.log.Info("acquired the lease, serving reservation requests")
			<-ctx.Done()
			s.leader.Store(false)
		},
		OnStoppedLeading: func() {
			s.leader.Store(false)
		},
		OnNewLeader: s.observeLeader,
	}
}

// Run takes part in the election on the given lock until ctx is done or the
// lease is lost. On ctx cancellation a held lease is released, so a standby
// replica takes over within one RetryPeriod instead of waiting for it to expire.
func (s *State) Run(ctx context.Context, lock resourcelock.Interface) error {
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   LeaseDuration,
		RenewDeadline:   RenewDeadline,
		RetryPeriod:     RetryPeriod,
		Callbacks:       s.callbacks(),
		ReleaseOnCancel: true,
		Name:            lock.Describe(),
	})
	if err != nil {
		return fmt.Errorf("unable to create the leader elector: %w", err)
	}

	le.Run(ctx)
	s.leader.Store(false)

	if ctx.Err() == nil {
		return ErrLeadershipLost
	}
	return nil
}
