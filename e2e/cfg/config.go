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

package cfg

import (
	"os"
	"strings"

	"github.com/caarlos0/env/v11"
)

// Config holds the e2e suite configuration. Field-level `required` tags were
// dropped in favour of conditional enforcement in validate(): the provider-based
// scheme (E2E_TEST_CLUSTER_PROVIDER set, e.g. dvp) brings the cluster up and
// connects through storage-e2e's clusterprovider, so it does not need the legacy
// KUBE_CONFIG_PATH / CREATE_MODE inputs — while the legacy alwaysCreateNew /
// alwaysUseExisting paths still require the full set.
type Config struct {
	TestCluster       TestCluster `envPrefix:"TEST_CLUSTER_"`
	SSH               SSH         `envPrefix:"SSH_"`
	KubeConfigPath    string      `env:"KUBE_CONFIG_PATH"`
	DKPLicenceKey     string      `env:"DKP_LICENSE_KEY"`
	LogLevel          string      `env:"LOG_LEVEL" envDefault:"info"`
	YamlClusterConfig string      `env:"YAML_CONFIG_FILENAME" envDefault:"cluster_config.yml"`
	ModulesImageTag   string      `env:"MODULES_MODULE_TAG" envDefault:"main"`
}

type TestCluster struct {
	CreateMode   CreateMode `env:"CREATE_MODE"`
	Namespace    string     `env:"NAMESPACE" envDefault:"e2e-test-cluster"`
	StorageClass string     `env:"STORAGE_CLASS"`
	Cleanup      bool       `env:"CLEANUP" envDefault:"false"`
}

type SSH struct {
	User       string `env:"USER"`
	Host       string `env:"HOST"`
	PrivateKey string `env:"PRIVATE_KEY"`
	Passphrase string `env:"PASSPHRASE"`
	Jump       Jump   `envPrefix:"JUMP_"`
	VmUser     string `env:"VM_USER"`
}

type Jump struct {
	Host           string `env:"HOST"`
	User           string `env:"USER"`
	PrivateKeyPath string `env:"KEY_PATH"`
}

var cfg Config

func New() (*Config, error) {
	if err := env.Parse(&cfg); err != nil {
		return nil, err
	}

	applyProviderEnvFallbacks()

	vErr := validate()
	if vErr != nil {
		return nil, vErr
	}

	return &cfg, nil
}

// applyProviderEnvFallbacks fills the values specs consume from cfg (e.g.
// TestCluster.StorageClass for the VirtualDisks they attach, SSH creds for
// node access) from the dvp-provider env scheme (E2E_DVP_BASE_CLUSTER_*) when
// the legacy TEST_CLUSTER_/SSH_ vars are unset. Under the provider scheme the
// cluster is brought up out-of-band and the suite only receives the
// E2E_DVP_BASE_CLUSTER_* inputs, but specs still read cfg directly, so the two
// schemes must be reconciled in one place.
func applyProviderEnvFallbacks() {
	if !providerDriven() {
		return
	}
	setIfEmpty(&cfg.TestCluster.StorageClass, "E2E_DVP_BASE_CLUSTER_STORAGE_CLASS")
	setIfEmpty(&cfg.SSH.User, "E2E_DVP_BASE_CLUSTER_SSH_USER")
	setIfEmpty(&cfg.SSH.Host, "E2E_DVP_BASE_CLUSTER_SSH_HOST")
	setIfEmpty(&cfg.SSH.PrivateKey, "E2E_DVP_BASE_CLUSTER_SSH_PRIVATE_KEY")
	setIfEmpty(&cfg.SSH.Passphrase, "E2E_DVP_BASE_CLUSTER_SSH_PASSPHRASE")
}

func setIfEmpty(dst *string, envName string) {
	if *dst == "" {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			*dst = v
		}
	}
}

func Load() *Config {
	return &cfg
}
