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
	TestCluster    TestCluster `envPrefix:"TEST_CLUSTER_"`
	SSH            SSH         `envPrefix:"SSH_"`
	KubeConfigPath string      `env:"KUBE_CONFIG_PATH,required"`
	DKPLicenceKey  string      `env:"DKP_LICENSE_KEY"`
	LogLevel       string      `env:"LOG_LEVEL" envDefault:"info"`
}

type TestCluster struct {
	CreateMode   CreateMode `env:"CREATE_MODE,required"`
	Namespace    string     `env:"NAMESPACE" envDefault:"e2e-test-cluster"`
	StorageClass string     `env:"STORAGE_CLASS,required"`
	Cleanup      bool       `env:"CLEANUP" envDefault:"false"`
}

type SSH struct {
	User       string `env:"USER,required"`
	Host       string `env:"HOST,required"`
	JumpHost   string `env:"JUMP_HOST"`
	JumpUser   string `env:"JUMP_USER"`
	PrivateKey string `env:"PRIVATE_KEY,required"`
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
