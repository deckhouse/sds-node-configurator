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

	"github.com/deckhouse/sds-node-configurator/images/sds-health-watcher-controller/pkg/logger"
)

const (
	ScanInterval                         = "SCAN_INTERVAL"
	LogLevel                             = "LOG_LEVEL"
	MetricsPort                          = "METRICS_PORT"
	DefaultHealthProbeBindAddressEnvName = "HEALTH_PROBE_BIND_ADDRESS"
	DefaultHealthProbeBindAddress        = ":8081"

	LeaderElectionEnvName          = "LEADER_ELECTION"
	LeaderElectionNamespaceEnvName = "LEADER_ELECTION_NAMESPACE"
	LeaderElectionIDEnvName        = "LEADER_ELECTION_ID"
	DefaultLeaderElectionID        = "sds-health-watcher-controller-leader-election"
)

type Options struct {
	Loglevel                logger.Verbosity
	MetricsPort             string
	ScanIntervalSec         time.Duration
	NodeName                string
	HealthProbeBindAddress  string
	LeaderElection          bool
	LeaderElectionNamespace string
	LeaderElectionID        string
}

func NewConfig() (*Options, error) {
	var opts Options

	loglevel := os.Getenv(LogLevel)
	if loglevel == "" {
		opts.Loglevel = logger.DebugLevel
	} else {
		opts.Loglevel = logger.Verbosity(loglevel)
	}

	opts.MetricsPort = os.Getenv(MetricsPort)
	if opts.MetricsPort == "" {
		opts.MetricsPort = ":8080"
	}

	opts.HealthProbeBindAddress = os.Getenv(DefaultHealthProbeBindAddressEnvName)
	if opts.HealthProbeBindAddress == "" {
		opts.HealthProbeBindAddress = DefaultHealthProbeBindAddress
	}

	scanInt := os.Getenv(ScanInterval)
	if scanInt == "" {
		opts.ScanIntervalSec = 5 * time.Second
	} else {
		interval, err := strconv.Atoi(scanInt)
		if err != nil {
			return nil, fmt.Errorf("[NewConfig] unable to get %s, error: %w", ScanInterval, err)
		}
		opts.ScanIntervalSec = time.Duration(interval) * time.Second
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

	return &opts, nil
}
