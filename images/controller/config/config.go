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

package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/deckhouse/sds-common-lib/logger"
	"github.com/deckhouse/sds-common-lib/monitoring"
)

const (
	ScanInterval                         = "SCAN_INTERVAL"
	DefaultHealthProbeBindAddressEnvName = "HEALTH_PROBE_BIND_ADDRESS"
	DefaultHealthProbeBindAddress        = ":8081"

	LeaderElectionEnvName          = "LEADER_ELECTION"
	LeaderElectionNamespaceEnvName = "LEADER_ELECTION_NAMESPACE"
	LeaderElectionIDEnvName        = "LEADER_ELECTION_ID"
	DefaultLeaderElectionID        = "controller-leader-election"

	// AgentNamespaceEnvName names the namespace the agent DaemonSet runs in,
	// the module's own. The controller lists the agent Pods there to tell the
	// nodes the agent runs on.
	AgentNamespaceEnvName = "AGENT_NAMESPACE"
	DefaultAgentNamespace = "d8-sds-node-configurator"
	AgentPodLabelSelector = "app=sds-node-configurator"

	// MissingBlockDeviceRetentionHours is the setting the agent reads too: a
	// BlockDevice of a node without an agent is kept as long as one whose
	// device left a node with an agent.
	MissingBlockDeviceRetentionHours        = "MISSING_BLOCK_DEVICE_RETENTION_HOURS"
	DefaultMissingBlockDeviceRetentionHours = 24
	MaxMissingBlockDeviceRetentionHours     = 8760
)

type Options struct {
	Loglevel                logger.Verbosity
	MetricsBindAddress      string
	ScanInterval            time.Duration
	NodeName                string
	HealthProbeBindAddress  string
	LeaderElection          bool
	LeaderElectionNamespace string
	LeaderElectionID        string

	AgentNamespace              string
	MissingBlockDeviceRetention time.Duration
}

// DefaultMetricsBindAddress keeps the endpoint on loopback when the chart says
// nothing: the kube-rbac-proxy of the same pod is what publishes it. The fleet-wide
// fallback is the wildcard ":8080", which would both answer unauthenticated on the
// pod IP and collide with the proxy, since the two carry the same port number.
//
// Exported so that a test asserts against the value this package actually uses
// rather than against the library default it deliberately overrides.
const DefaultMetricsBindAddress = "127.0.0.1:8080"

func NewConfig() (*Options, error) {
	var opts Options

	opts.Loglevel = logger.VerbosityFromEnv(logger.InfoLevel)

	// METRICS_BIND_ADDRESS, the name every module in the fleet reads, instead of the
	// METRICS_PORT this one used to read: the value is an address and not a port, and
	// the chart now sets it from the same place as everywhere else.
	opts.MetricsBindAddress = monitoring.BindAddressFromEnvOr(DefaultMetricsBindAddress)

	opts.HealthProbeBindAddress = os.Getenv(DefaultHealthProbeBindAddressEnvName)
	if opts.HealthProbeBindAddress == "" {
		opts.HealthProbeBindAddress = DefaultHealthProbeBindAddress
	}

	scanInt := os.Getenv(ScanInterval)
	if scanInt == "" {
		opts.ScanInterval = 5 * time.Second
	} else {
		interval, err := strconv.Atoi(scanInt)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", ScanInterval, err)
		}
		opts.ScanInterval = time.Duration(interval) * time.Second
	}

	leaderElection := os.Getenv(LeaderElectionEnvName)
	if leaderElection == "" || leaderElection == "true" {
		opts.LeaderElection = true
	} else {
		opts.LeaderElection = false
	}

	opts.LeaderElectionNamespace = os.Getenv(LeaderElectionNamespaceEnvName)

	opts.LeaderElectionID = os.Getenv(LeaderElectionIDEnvName)
	if opts.LeaderElectionID == "" {
		opts.LeaderElectionID = DefaultLeaderElectionID
	}

	opts.AgentNamespace = os.Getenv(AgentNamespaceEnvName)
	if opts.AgentNamespace == "" {
		opts.AgentNamespace = DefaultAgentNamespace
	}

	opts.MissingBlockDeviceRetention = DefaultMissingBlockDeviceRetentionHours * time.Hour
	if raw := os.Getenv(MissingBlockDeviceRetentionHours); raw != "" {
		parsed, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			return nil, fmt.Errorf("[NewConfig] %s must be an integer, got %q: %w", MissingBlockDeviceRetentionHours, raw, err)
		case parsed < 0 || parsed > MaxMissingBlockDeviceRetentionHours:
			return nil, fmt.Errorf("[NewConfig] %s must be between 0 and %d, got %d", MissingBlockDeviceRetentionHours, MaxMissingBlockDeviceRetentionHours, parsed)
		default:
			opts.MissingBlockDeviceRetention = time.Duration(parsed) * time.Hour
		}
	}

	return &opts, nil
}
