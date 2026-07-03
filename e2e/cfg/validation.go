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
	"fmt"
	"os"
	"strings"
)

// providerDriven reports whether the run uses storage-e2e's clusterprovider
// scheme (e.g. dvp): the cluster is bootstrapped by cmd/bootstrap-cluster and
// the suite connects through the provider, so the legacy KUBE_CONFIG_PATH /
// CREATE_MODE inputs are not required.
func providerDriven() bool {
	return strings.TrimSpace(os.Getenv("E2E_TEST_CLUSTER_PROVIDER")) != ""
}

func validate() error {
	if providerDriven() {
		// The provider brings the cluster up and connects; only inputs the specs
		// themselves consume are needed (node SSH for lsblk/VG, the base-cluster
		// StorageClass for VirtualDisks, the module image tag). These are validated
		// lazily by the specs/helpers that use them, so nothing is hard-required here.
		return nil
	}

	// Legacy alwaysCreateNew / alwaysUseExisting paths: enforce the full input set
	// the storage-e2e VM/SSH flow relies on.
	var missing []string
	if cfg.TestCluster.CreateMode == "" {
		missing = append(missing, "TEST_CLUSTER_CREATE_MODE")
	}
	if cfg.TestCluster.StorageClass == "" {
		missing = append(missing, "TEST_CLUSTER_STORAGE_CLASS")
	}
	if cfg.SSH.User == "" {
		missing = append(missing, "SSH_USER")
	}
	if cfg.SSH.Host == "" {
		missing = append(missing, "SSH_HOST")
	}
	if cfg.SSH.PrivateKey == "" {
		missing = append(missing, "SSH_PRIVATE_KEY")
	}
	if cfg.KubeConfigPath == "" {
		missing = append(missing, "KUBE_CONFIG_PATH")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required env for the legacy (non-provider) e2e flow: %s", strings.Join(missing, ", "))
	}

	if cfg.TestCluster.CreateMode == ModeCreateNew && cfg.DKPLicenceKey == "" {
		return fmt.Errorf("if create mode is alwaysCreateNew, DKP_LICENSE_KEY is required")
	}
	return nil
}
