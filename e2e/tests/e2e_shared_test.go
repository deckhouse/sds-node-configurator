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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/deckhouse/storage-e2e/pkg/cluster"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	testClusterModeCreateNew = "alwaysCreateNew"

	e2eVirtualizationModuleWaitDefault = 25 * time.Minute
)

var deckhouseModuleGVR = schema.GroupVersionResource{
	Group:    "deckhouse.io",
	Version:  "v1alpha1",
	Resource: "modules",
}

const deckhouseModuleConditionIsReady = "IsReady"

func moduleVirtualizationIsReady(obj *unstructured.Unstructured) (phase string, isReady bool) {
	p, found, _ := unstructured.NestedString(obj.Object, "status", "phase")
	if found {
		phase = p
	}
	if phase == "Ready" {
		return phase, true
	}
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return phase, false
	}
	for _, c := range conditions {
		cm, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := cm["type"].(string)
		st, _ := cm["status"].(string)
		if t == deckhouseModuleConditionIsReady && st == "True" {
			return phase, true
		}
	}
	return phase, false
}

func e2eTestTempDirFromStack() (string, error) {
	for i := 1; i <= 20; i++ {
		_, file, _, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if !strings.Contains(filepath.ToSlash(file), "/tests/") {
			continue
		}
		dir := filepath.Dir(file)
		for filepath.Base(dir) != "tests" {
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
		if filepath.Base(dir) != "tests" {
			continue
		}
		repoRoot := filepath.Dir(dir)
		testFileName := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		return filepath.Join(repoRoot, "temp", testFileName), nil
	}
	return "", fmt.Errorf("could not determine e2e temp dir from call stack (expected caller under tests/)")
}

func waitForVirtualizationModuleReadyWithRestConfig(ctx context.Context, baseCfg *rest.Config) error {
	cfg := rest.CopyConfig(baseCfg)
	cfg.Timeout = 30 * time.Second
	cfg.Dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := net.Dialer{Timeout: 15 * time.Second}
		return d.DialContext(ctx, network, addr)
	}

	timeout := e2eVirtualizationModuleWaitDefault
	if v := os.Getenv("E2E_VIRTUALIZATION_MODULE_WAIT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			timeout = d
		}
	}
	deadline := time.Now().Add(timeout)
	poll := 3 * time.Second
	reqTimeout := 30 * time.Second

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client for virtualization wait: %w", err)
	}

	GinkgoWriter.Printf("    Waiting for Deckhouse module %q to become Ready (timeout %s, polling every %s)...\n",
		"virtualization", timeout, poll)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %v waiting for Module/virtualization phase Ready (see logs above)", timeout)
		}

		getCtx, cancel := context.WithTimeout(ctx, reqTimeout)
		obj, err := dyn.Resource(deckhouseModuleGVR).Get(getCtx, "virtualization", metav1.GetOptions{})
		cancel()

		if err != nil {
			GinkgoWriter.Printf("    Module/virtualization get: %v\n", err)
			time.Sleep(poll)
			continue
		}
		phase, ready := moduleVirtualizationIsReady(obj)
		if ready {
			GinkgoWriter.Printf("    Module/virtualization is ready (phase=%q)\n", phase)
			return nil
		}
		if phase == "" {
			GinkgoWriter.Printf("    Module/virtualization: no status.phase yet (waiting)\n")
		} else {
			GinkgoWriter.Printf("    Module/virtualization phase=%q (waiting for Ready or IsReady=True)\n", phase)
		}
		time.Sleep(poll)
	}
}

// waitForVirtualizationModuleReadyIfNeeded polls Module/virtualization before CreateTestCluster so
// storage-e2e step 3 (short timeout, phase-only) does not fail while the module is still Reconciling.
// No-op if TEST_CLUSTER_CREATE_MODE is not alwaysCreateNew or E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true.
func waitForVirtualizationModuleReadyIfNeeded(ctx context.Context) error {
	if os.Getenv("E2E_SKIP_VIRTUALIZATION_MODULE_WAIT") == "true" {
		GinkgoWriter.Printf("    Skipping virtualization Module pre-wait (E2E_SKIP_VIRTUALIZATION_MODULE_WAIT=true)\n")
		return nil
	}
	if e2eConfigTestClusterCreateMode() != testClusterModeCreateNew {
		return nil
	}

	sshHost := e2eConfigSSHHost()
	sshUser := e2eConfigSSHUser()
	if sshHost == "" || sshUser == "" {
		return nil
	}

	sshKeyPath, err := cluster.GetSSHPrivateKeyPath()
	if err != nil {
		return fmt.Errorf("ssh key for virtualization pre-wait: %w", err)
	}

	kubeconfigDir, err := e2eTestTempDirFromStack()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(kubeconfigDir, 0o755); err != nil {
		return fmt.Errorf("mkdir kubeconfig dir for virtualization pre-wait: %w", err)
	}

	useJump := e2eConfigSSHJumpHost() != ""
	jumpUser := e2eConfigSSHJumpUser()
	if jumpUser == "" {
		jumpUser = sshUser
	}
	jumpHost := e2eConfigSSHJumpHost()
	jumpKeyPath := e2eConfigSSHJumpKeyPath()
	if jumpKeyPath == "" {
		jumpKeyPath = sshKeyPath
	}

	opts := cluster.ConnectClusterOptions{
		SSHUser:             sshUser,
		SSHHost:             sshHost,
		SSHKeyPath:          sshKeyPath,
		UseJumpHost:         useJump,
		KubeconfigOutputDir: kubeconfigDir,
	}
	if useJump {
		opts = cluster.ConnectClusterOptions{
			SSHUser:             jumpUser,
			SSHHost:             jumpHost,
			SSHKeyPath:          jumpKeyPath,
			UseJumpHost:         true,
			TargetUser:          sshUser,
			TargetHost:          sshHost,
			TargetKeyPath:       sshKeyPath,
			KubeconfigOutputDir: kubeconfigDir,
		}
	}

	GinkgoWriter.Printf("    Connecting to base cluster (SSH) for virtualization Module pre-wait...\n")

	connectTimeout := 20 * time.Minute
	if v := os.Getenv("E2E_VIRTUALIZATION_MODULE_CONNECT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			connectTimeout = d
		}
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, connectTimeout)
	defer cancelConnect()

	base, err := cluster.ConnectToCluster(connectCtx, opts)
	if err != nil {
		return fmt.Errorf("connect to base cluster for virtualization pre-wait: %w", err)
	}
	defer func() {
		if base.TunnelInfo != nil && base.TunnelInfo.StopFunc != nil {
			_ = base.TunnelInfo.StopFunc()
		}
		if base.SSHClient != nil {
			_ = base.SSHClient.Close()
		}
	}()

	if err := waitForVirtualizationModuleReadyWithRestConfig(ctx, base.Kubeconfig); err != nil {
		return err
	}
	GinkgoWriter.Printf("    Closed pre-wait SSH tunnel; proceeding to CreateTestCluster\n")
	return nil
}
