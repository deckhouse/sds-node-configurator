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

package tests

import (
	"context"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

var (
	cfg       *rest.Config
	k8sClient client.Client
	ctx       context.Context
	cancel    context.CancelFunc
)

func TestBlockDeviceDiscovery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "BlockDevice Discovery E2E Suite")
}

var _ = BeforeSuite(func() {
	ctx, cancel = context.WithCancel(context.Background())

	By("Bootstrapping test environment")

	var err error
	cfg, err = config.GetConfig()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = v1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Verify we can connect to the cluster
	_, err = k8sClient.RESTMapper().RESTMapping(v1alpha1.SchemeGroupVersion.WithKind("BlockDevice").GroupKind())
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	By("Tearing down the test environment")
	cancel()
})

// GetNodeName returns the node name for optional filtering.
// When set via E2E_NODE_NAME, only BlockDevices on that node are considered.
// When unset, any node is accepted (node name can vary per cluster).
func GetNodeName() string {
	return os.Getenv("E2E_NODE_NAME")
}

// GetExpectedDeviceSerial returns the expected device serial number.
// Must be set via the E2E_DEVICE_SERIAL environment variable.
func GetExpectedDeviceSerial() string {
	return os.Getenv("E2E_DEVICE_SERIAL")
}

// GetExpectedDevicePath returns the path to the device when set via E2E_DEVICE_PATH.
// If unset, the test will accept any block device path on the node (/dev/sdX, /dev/vdX, etc.).
func GetExpectedDevicePath() string {
	return os.Getenv("E2E_DEVICE_PATH")
}
