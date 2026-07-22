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
	TestCluster     TestCluster `envPrefix:"TEST_CLUSTER_"`
	ModulesImageTag string      `env:"MODULES_MODULE_TAG" envDefault:"main"`
}

type TestCluster struct {
	Namespace    string `env:"NAMESPACE" envDefault:"e2e-test-cluster"`
	StorageClass string `env:"STORAGE_CLASS"`
}

// Load parses the process environment into a fresh Config and returns it.
// It is stateless: every call reads the environment anew and returns an
// independent *Config, so there is no package-level global and no hidden
// ordering requirement between callers (mirrors LoadStress).
func Load() (*Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
