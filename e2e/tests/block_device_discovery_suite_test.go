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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

const e2eLVMVGPrefix = "e2e-lvg-"

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
	// core/v1 (Node, etc.) is already in client-go's scheme.Scheme

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Verify we can connect to the cluster
	_, err = k8sClient.RESTMapper().RESTMapping(v1alpha1.SchemeGroupVersion.WithKind("BlockDevice").GroupKind())
	Expect(err).NotTo(HaveOccurred())

	// Step 0: remove any leftover e2e LVMVolumeGroups from previous runs (so tests start from a clean state)
	By("Step 0: Cleaning up existing e2e LVMVolumeGroups (prefix " + e2eLVMVGPrefix + ")")
	cleanupE2ELVMVolumeGroups(ctx, k8sClient)
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

// cleanupE2ELVMVolumeGroups deletes all LVMVolumeGroups whose name has prefix e2e-lvg-.
// If they don't disappear (e.g. stuck on finalizers), removes finalizers and deletes again.
func cleanupE2ELVMVolumeGroups(ctx context.Context, cl client.Client) {
	var list v1alpha1.LVMVolumeGroupList
	err := cl.List(ctx, &list, &client.ListOptions{})
	if err != nil {
		GinkgoWriter.Printf("Step 0: list LVMVolumeGroups failed (skip cleanup): %v\n", err)
		return
	}
	var toDelete []string
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, e2eLVMVGPrefix) {
			toDelete = append(toDelete, list.Items[i].Name)
		}
	}
	if len(toDelete) == 0 {
		return
	}
	GinkgoWriter.Printf("Step 0: deleting %d LVMVolumeGroup(s): %v\n", len(toDelete), toDelete)
	for _, name := range toDelete {
		lvg := &v1alpha1.LVMVolumeGroup{}
		lvg.Name = name
		if err := cl.Delete(ctx, lvg); err != nil {
			GinkgoWriter.Printf("Step 0: delete %s: %v\n", name, err)
		}
	}
	// Wait for deletion (up to 2 minutes)
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		err := cl.List(ctx, &list, &client.ListOptions{})
		if err != nil {
			break
		}
		var remaining []string
		for i := range list.Items {
			if strings.HasPrefix(list.Items[i].Name, e2eLVMVGPrefix) {
				remaining = append(remaining, list.Items[i].Name)
			}
		}
		if len(remaining) == 0 {
			GinkgoWriter.Println("Step 0: all e2e LVMVolumeGroups removed")
			return
		}
		// Force-remove finalizers on stuck objects
		for _, name := range remaining {
			lvg := &v1alpha1.LVMVolumeGroup{}
			if err := cl.Get(ctx, client.ObjectKey{Name: name}, lvg); err != nil {
				continue
			}
			if len(lvg.Finalizers) > 0 {
				lvg.Finalizers = nil
				_ = cl.Update(ctx, lvg)
			}
			_ = cl.Delete(ctx, lvg)
		}
		time.Sleep(5 * time.Second)
	}
	GinkgoWriter.Printf("Step 0: warning: some e2e LVMVolumeGroups may still exist after cleanup\n")
}
