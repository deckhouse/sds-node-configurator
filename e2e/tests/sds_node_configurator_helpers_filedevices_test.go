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
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

const (
	// e2eFileDevicesBaseDir must match the fileDevicesDirectory module default
	// (openapi/config-values.yaml) and DefaultFileDevicesDirectory in the agent.
	// File-backed tests place backing files here so the agent's base-dir
	// allowlist accepts them.
	e2eFileDevicesBaseDir = "/opt/deckhouse/sds/file-devices"

	e2eSNCNamespace = "d8-sds-node-configurator"
	e2eSNCAgentApp  = "sds-node-configurator"
)

// e2ePickNodeRunningAgent returns the node name of a Ready, Running
// sds-node-configurator agent pod. File-backed LVMVolumeGroups need no
// dedicated disk, so any node with a healthy agent can host the test VG.
func e2ePickNodeRunningAgent(ctx context.Context, cl client.Client) string {
	var chosen string
	Eventually(func(g Gomega) {
		var pods corev1.PodList
		g.Expect(cl.List(ctx, &pods, client.InNamespace(e2eSNCNamespace), client.MatchingLabels{"app": e2eSNCAgentApp})).To(Succeed())
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.Spec.NodeName == "" || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning || !isPodReady(p) {
				continue
			}
			chosen = p.Spec.NodeName
			return
		}
		g.Expect(chosen).NotTo(BeEmpty(), "no Ready sds-node-configurator agent pod found in %s", e2eSNCNamespace)
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
	return chosen
}

// e2eFileDevicesForNode returns the file-device status entries the LVG reports
// for node (nil if the node is absent or has no file devices yet).
func e2eFileDevicesForNode(lvg *v1alpha1.LVMVolumeGroup, node string) []v1alpha1.LVMVolumeGroupFileDevice {
	for i := range lvg.Status.Nodes {
		if lvg.Status.Nodes[i].Name == node {
			return lvg.Status.Nodes[i].FileDevices
		}
	}
	return nil
}

// e2eNodePathExists reports whether path exists on the node (checked as root,
// because backing files live under root-owned /opt/deckhouse/sds).
func e2eNodePathExists(ctx context.Context, kubeconfig *rest.Config, node, sshUser, path string) (bool, error) {
	out, err := e2eExecOnTestClusterNodeSSH(ctx, kubeconfig, node, sshUser,
		fmt.Sprintf("sudo -n sh -c 'test -e %q && echo EXISTS || echo MISSING'", path))
	if err != nil {
		return false, err
	}
	return strings.Contains(out, "EXISTS"), nil
}

// e2eLoopBoundToFileOnNode reports whether any loop device is currently
// attached to path on the node (via `losetup -j`).
func e2eLoopBoundToFileOnNode(ctx context.Context, kubeconfig *rest.Config, node, sshUser, path string) (bool, string, error) {
	out, err := e2eExecOnTestClusterNodeSSH(ctx, kubeconfig, node, sshUser,
		fmt.Sprintf("sudo -n losetup -j %q 2>/dev/null || losetup -j %q 2>/dev/null", path, path))
	if err != nil {
		return false, out, err
	}
	return strings.TrimSpace(out) != "", out, nil
}

// e2eVGListedOnNode reports whether vgName is visible to vgs on the node.
func e2eVGListedOnNode(ctx context.Context, kubeconfig *rest.Config, node, sshUser, vgName string) (bool, string, error) {
	out, err := e2eExecOnTestClusterNodeSSH(ctx, kubeconfig, node, sshUser,
		"vgs -o vg_name --noheadings 2>/dev/null || sudo -n vgs -o vg_name --noheadings 2>/dev/null")
	if err != nil {
		return false, out, err
	}
	return e2eVgNameListedInVgsOutput(out, vgName), out, nil
}

// e2ePVNamesInVGOnNode returns the pv_name of every PV that belongs to vgName on
// the node. A mixed block+file VG must report at least one /dev/loop* PV (the
// backing file) and at least one non-loop PV (the block device).
func e2ePVNamesInVGOnNode(ctx context.Context, kubeconfig *rest.Config, node, sshUser, vgName string) ([]string, string, error) {
	quotedVG := strconv.Quote(vgName)
	cmd := fmt.Sprintf(`sudo -n pvs --noheadings -o pv_name,vg_name 2>/dev/null | awk -v vg=%s '$2==vg {print $1}'`, quotedVG)
	out, err := e2eExecOnTestClusterNodeSSH(ctx, kubeconfig, node, sshUser, cmd)
	if err != nil {
		return nil, out, err
	}
	var pvs []string
	for _, line := range strings.Split(out, "\n") {
		if name := strings.TrimSpace(line); name != "" {
			pvs = append(pvs, name)
		}
	}
	return pvs, out, nil
}

// e2eCreateFileBackedLVGAndWaitReady creates lvg and waits (up to
// e2eLVMVolumeGroupReadyTimeout) until it reports Phase Ready, dumping
// conditions while it waits. Returns the refreshed object.
func e2eCreateFileBackedLVGAndWaitReady(ctx context.Context, cl client.Client, lvg *v1alpha1.LVMVolumeGroup) *v1alpha1.LVMVolumeGroup {
	Expect(cl.Create(ctx, lvg)).To(Succeed())
	var created v1alpha1.LVMVolumeGroup
	Eventually(func(g Gomega) {
		g.Expect(cl.Get(ctx, client.ObjectKeyFromObject(lvg), &created)).To(Succeed())
		if created.Status.Phase != v1alpha1.PhaseReady {
			GinkgoWriter.Printf("LVMVolumeGroup %s phase=%s (waiting for Ready)\n", lvg.Name, created.Status.Phase)
			for _, c := range created.Status.Conditions {
				GinkgoWriter.Printf("  condition %s status=%s reason=%s msg=%s\n", c.Type, c.Status, c.Reason, c.Message)
			}
		}
		g.Expect(created.Status.Phase).To(Equal(v1alpha1.PhaseReady), "Phase should be Ready, got %s", created.Status.Phase)
	}, e2eLVMVolumeGroupReadyTimeout, 10*time.Second).Should(Succeed())
	return &created
}

// e2eExpectNoFalseConditions fails if any status condition is False.
func e2eExpectNoFalseConditions(lvg *v1alpha1.LVMVolumeGroup) {
	for _, c := range lvg.Status.Conditions {
		Expect(c.Status).NotTo(Equal(metav1.ConditionFalse),
			"condition %s has status False: reason=%s message=%s", c.Type, c.Reason, c.Message)
	}
}

// e2eDeleteLVGAndWaitGone deletes the LVMVolumeGroup and waits until its CR is
// gone. The finalizer is only cleared after the agent detaches the loop
// devices and removes the backing files, so a successful delete also confirms
// node-side cleanup ran.
func e2eDeleteLVGAndWaitGone(ctx context.Context, cl client.Client, name string, timeout time.Duration) {
	Expect(client.IgnoreNotFound(cl.Delete(ctx, &v1alpha1.LVMVolumeGroup{ObjectMeta: metav1.ObjectMeta{Name: name}}))).To(Succeed())
	Eventually(func(g Gomega) {
		var cur v1alpha1.LVMVolumeGroup
		err := cl.Get(ctx, client.ObjectKey{Name: name}, &cur)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "LVMVolumeGroup %s should be deleted", name)
	}, timeout, 8*time.Second).Should(Succeed())
}
