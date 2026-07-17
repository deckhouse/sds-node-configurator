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
	"github.com/caarlos0/env/v11"
)

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

	vErr := validate()
	if vErr != nil {
		return nil, vErr
	}

	return &cfg, nil
}

func Load() *Config {
	return &cfg
}
