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

package v1alpha1_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"

	"github.com/go-logr/logr"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/deckhouse/sds-node-configurator/api/v1alpha1"
)

// fixtures
var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

func TestMain(m *testing.M) {
	os.Exit(runEnvtest(m))
}

func runEnvtest(m *testing.M) int {
	logf.SetLogger(logr.Discard())

	assets, err := resolveEnvtestAssets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest assets: %v\n", err)
		return 1
	}

	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "envtest: cannot resolve suite path")
		return 1
	}
	crdPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "crds", "storage.deckhouse.io_lvmlogicalvolumesnapshots.yaml")
	crdPath, err = filepath.Abs(crdPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest: abs crd path: %v\n", err)
		return 1
	}
	if _, err := os.Stat(crdPath); err != nil {
		fmt.Fprintf(os.Stderr, "envtest: CRD file missing: %s (%v)\n", crdPath, err)
		return 1
	}

	scheme := kruntime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "envtest: client-go scheme: %v\n", err)
		return 1
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "envtest: api scheme: %v\n", err)
		return 1
	}

	testEnv = &envtest.Environment{
		BinaryAssetsDirectory: assets,
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths:  []string{crdPath},
			Scheme: scheme,
		},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err = testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest: start: %v\n", err)
		return 1
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest: client: %v\n", err)
		_ = testEnv.Stop()
		return 1
	}

	code := m.Run()

	if err := testEnv.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "envtest: stop: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	return code
}

func resolveEnvtestAssets() (string, error) {
	if p := os.Getenv("KUBEBUILDER_ASSETS"); p != "" {
		return p, nil
	}

	cmd := exec.Command(
		"go", "run", "sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.20",
		"use", "1.33.x", "-p", "path",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("setup-envtest: %w\n%s", err, stderr.String())
	}
	path := string(bytes.TrimSpace(stdout.Bytes()))
	if path == "" {
		return "", fmt.Errorf("setup-envtest returned empty path (stderr: %s)", stderr.String())
	}
	return path, nil
}
