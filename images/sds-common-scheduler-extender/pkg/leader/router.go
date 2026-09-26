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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
)

// ForwardedHeader marks a request a standby replica forwarded to the leader. A
// replica that receives it and is not the leader (the lease moved while the
// request was in flight) refuses it instead of forwarding it again.
const ForwardedHeader = "X-Sds-Scheduler-Extender-Forwarded-By"

// LeaderState is the part of State the router needs.
type LeaderState interface {
	IsLeader() bool
	LeaderAddress() string
	Identity() string
}

// Router serves on the leader every request that reads or writes the
// reservations, and everything else on the replica it reached.
//
// Every replica stays Ready and in the Service: gating readiness on the lease
// would count the standby as unavailable, and with the maxSurge 0 /
// maxUnavailable 1 rollout and the minAvailable 1 PodDisruptionBudget of the
// Deployment a rolling update could then never replace the leader, nor could its
// node be drained. A standby forwards instead.
type Router struct {
	state           LeaderState
	local           http.Handler
	needsLeader     func(path string) bool
	leaderTransport http.RoundTripper
	log             logger.Logger
}

// NewRouter returns a router that sends the paths needsLeader selects to the
// leader over leaderTransport and serves the rest with local.
func NewRouter(log logger.Logger, state LeaderState, local http.Handler, needsLeader func(path string) bool, leaderTransport http.RoundTripper) *Router {
	return &Router{
		state:           state,
		local:           local,
		needsLeader:     needsLeader,
		leaderTransport: leaderTransport,
		log:             log,
	}
}

func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !rt.needsLeader(r.URL.Path) || rt.state.IsLeader() {
		rt.local.ServeHTTP(w, r)
		return
	}

	if from := r.Header.Get(ForwardedHeader); from != "" {
		rt.log.Warn(fmt.Sprintf("refusing %s forwarded by %q: this replica is not the leader", r.URL.Path, from))
		http.Error(w, "this replica is not the leader", http.StatusServiceUnavailable)
		return
	}

	addr := rt.state.LeaderAddress()
	if addr == "" {
		rt.log.Warn(fmt.Sprintf("refusing %s: no leader is known", r.URL.Path))
		http.Error(w, "no leader is elected", http.StatusServiceUnavailable)
		return
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "https"
			pr.Out.URL.Host = addr
			pr.Out.Header.Set(ForwardedHeader, rt.state.Identity())
		},
		Transport: rt.leaderTransport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			rt.log.Warn(fmt.Sprintf("unable to forward %s to the leader at %s", r.URL.Path, addr), logger.Err(err))
			http.Error(w, "the leader is unreachable", http.StatusServiceUnavailable)
		},
	}
	proxy.ServeHTTP(w, r)
}

// NewLeaderTransport returns the transport a standby forwards over. The leader
// is dialled by its pod IP, which the serving certificate does not name, so the
// certificate is checked against serverName, one of the names it does carry. The
// dial timeout is short: a leader that died without releasing the lease must turn
// into a quick 503 rather than eat the whole kube-scheduler extender timeout.
func NewLeaderTransport(caPEM []byte, serverName string) (*http.Transport, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("the CA bundle holds no PEM certificate")
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			ServerName: serverName,
			MinVersion: tls.VersionTLS12,
		},
		DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: time.Second,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
	}, nil
}
