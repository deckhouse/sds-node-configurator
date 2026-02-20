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

package tests

import (
	"context"
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// WaitForBlockDevice waits for a BlockDevice with the given name to appear.
func WaitForBlockDevice(ctx context.Context, cl client.Client, name string, timeout time.Duration) *v1alpha1.BlockDevice {
	bd := &v1alpha1.BlockDevice{}
	Eventually(func(g Gomega) {
		err := cl.Get(ctx, types.NamespacedName{Name: name}, bd)
		g.Expect(err).NotTo(HaveOccurred())
	}, timeout, 5*time.Second).Should(Succeed())

	return bd
}

// WaitForBlockDeviceByPath waits for a BlockDevice with the given path on the node to appear.
func WaitForBlockDeviceByPath(ctx context.Context, cl client.Client, nodeName, path string, timeout time.Duration) *v1alpha1.BlockDevice {
	var foundBD *v1alpha1.BlockDevice
	blockDevicesList := &v1alpha1.BlockDeviceList{}

	Eventually(func(g Gomega) {
		err := cl.List(ctx, blockDevicesList, &client.ListOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		for i := range blockDevicesList.Items {
			bd := &blockDevicesList.Items[i]
			if bd.Status.NodeName == nodeName && bd.Status.Path == path {
				foundBD = bd
				return
			}
		}

		g.Expect(foundBD).NotTo(BeNil())
	}, timeout, 10*time.Second).Should(Succeed())

	return foundBD
}

// DeleteBlockDeviceIfExists deletes the BlockDevice if it exists.
func DeleteBlockDeviceIfExists(ctx context.Context, cl client.Client, name string) error {
	bd := &v1alpha1.BlockDevice{}
	err := cl.Get(ctx, types.NamespacedName{Name: name}, bd)
	if err != nil {
		// If the object is not found, that's expected
		return client.IgnoreNotFound(err)
	}

	return cl.Delete(ctx, bd)
}

// GetAllBlockDevicesOnNode returns all BlockDevices on the given node.
func GetAllBlockDevicesOnNode(ctx context.Context, cl client.Client, nodeName string) ([]v1alpha1.BlockDevice, error) {
	blockDevicesList := &v1alpha1.BlockDeviceList{}
	err := cl.List(ctx, blockDevicesList, &client.ListOptions{})
	if err != nil {
		return nil, err
	}

	var result []v1alpha1.BlockDevice
	for _, bd := range blockDevicesList.Items {
		if bd.Status.NodeName == nodeName {
			result = append(result, bd)
		}
	}

	return result, nil
}

// WaitForBlockDeviceDeletion waits for the BlockDevice to be deleted.
func WaitForBlockDeviceDeletion(ctx context.Context, cl client.Client, name string, timeout time.Duration) {
	bd := &v1alpha1.BlockDevice{}
	Eventually(func(g Gomega) {
		err := cl.Get(ctx, types.NamespacedName{Name: name}, bd)
		g.Expect(err).To(HaveOccurred())
	}, timeout, 5*time.Second).Should(Succeed())
}
