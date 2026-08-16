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

package framework

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// testKeys stands in for what a run supplies through NodeSSHKeysEnv. Made-up
// material on purpose: the point of taking the keys from the environment is that
// the real ones are not in this repository, and a fixture is not an exception to
// that.
var testKeys = []any{
	`cert-authority,principals="tfadm" ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAtestca ca`,
	`ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAtestfallback fallback`,
}

// newFakeDebugAccess builds a dynamic fake that knows both kinds. The list kinds
// have to be declared explicitly because neither is in any scheme the e2e module
// registers — the same reason the production path is dynamic.
func newFakeDebugAccess(t *testing.T, objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			nodeUsersGVR:               "NodeUserList",
			nodeGroupConfigurationsGVR: "NodeGroupConfigurationList",
		},
		objects...,
	)
}

func getResource(
	t *testing.T,
	dyn *dynamicfake.FakeDynamicClient,
	gvr schema.GroupVersionResource,
	name string,
) *unstructured.Unstructured {
	t.Helper()

	got, err := dyn.Resource(gvr).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)

	return got
}

// assertNodeUser checks the fields an operator actually depends on: sudo group
// membership, every node group, and the keys the run supplied. A NodeUser that
// lands without isSudoer or without the '*' node group is useless for the case it
// exists for — getting onto master-1 after a failed run.
func assertNodeUser(t *testing.T, got *unstructured.Unstructured) {
	t.Helper()

	isSudoer, found, err := unstructured.NestedBool(got.Object, "spec", "isSudoer")
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, isSudoer, "the account has to be in the sudoer group")

	groups, found, err := unstructured.NestedStringSlice(got.Object, "spec", "nodeGroups")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []string{"*"}, groups, "masters included, which is where the disk specs run")

	keys, found, err := unstructured.NestedStringSlice(got.Object, "spec", "sshPublicKeys")
	require.NoError(t, err)
	require.True(t, found)
	// Exactly what the run supplied, in order and with nothing added: the keys are
	// the whole of what this grants, so the package must not be able to widen the
	// set behind the back of whoever configured it.
	want := make([]string, 0, len(testKeys))
	for _, key := range testKeys {
		want = append(want, key.(string))
	}
	assert.Equal(t, want, keys)

	uid, found, err := unstructured.NestedInt64(got.Object, "spec", "uid")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, debugNodeUserUID, uid)

	// No password anywhere: the account is key-only, and sudo is made passwordless
	// by the NodeGroupConfiguration instead. A hash here would also mean a
	// credential committed to a public repository.
	_, found, err = unstructured.NestedString(got.Object, "spec", "passwordHash")
	require.NoError(t, err)
	assert.False(t, found, "the NodeUser must carry no passwordHash")
}

// assertSudoersConfig checks the half that makes the account able to do anything:
// isSudoer alone yields "%nodeadmin ALL=(ALL) ALL", which asks for a password the
// key-only account does not have.
func assertSudoersConfig(t *testing.T, got *unstructured.Unstructured) {
	t.Helper()

	content, found, err := unstructured.NestedString(got.Object, "spec", "content")
	require.NoError(t, err)
	require.True(t, found)
	assert.Contains(t, content, DebugNodeUserName+" ALL=(ALL) NOPASSWD:ALL",
		"without NOPASSWD the key-only account cannot sudo at all")
	assert.Contains(t, content, "/etc/sudoers.d/"+DebugNodeUserName,
		"the rule has to outrank 30-deckhouse-nodeadmins, so it goes in its own file")
	assert.True(t, strings.HasPrefix(content, "bb-sync-file "),
		"bb-sync-file makes the step a no-op once the file matches")

	// Exactly one grant, for the one account this package creates. A rule naming
	// an account nothing here creates is passwordless root on every node with
	// nobody to take it away, so the property to hold is the count, not the
	// absence of one particular name somebody could rename.
	granted := make([]string, 0, 1)
	for _, line := range strings.Split(content, "\n") {
		if fields := strings.Fields(line); len(fields) > 1 && strings.HasPrefix(fields[1], "ALL=") {
			granted = append(granted, fields[0])
		}
	}
	assert.Equal(t, []string{DebugNodeUserName}, granted,
		"every sudoers grant must name an account this package also creates")

	for _, field := range []string{"bundles", "nodeGroups"} {
		values, found, err := unstructured.NestedStringSlice(got.Object, "spec", field)
		require.NoError(t, err)
		require.True(t, found, field)
		assert.Equal(t, []string{"*"}, values, field)
	}
}

// The first run on a fresh cluster has to create both objects outright.
func TestEnsureDebugAccess_CreatesWhenAbsent(t *testing.T) {
	dyn := newFakeDebugAccess(t)

	require.NoError(t, ensureDebugAccessWith(context.Background(), dyn, testKeys))

	assertNodeUser(t, getResource(t, dyn, nodeUsersGVR, DebugNodeUserName))
	assertSudoersConfig(t, getResource(t, dyn, nodeGroupConfigurationsGVR, debugSudoersConfigName))
}

// Running the suite again against a kept cluster (e2e/keep-cluster) must not fail
// on AlreadyExists — the whole point of these resources is that they outlive the
// run that created them.
func TestEnsureDebugAccess_IsIdempotent(t *testing.T) {
	dyn := newFakeDebugAccess(t)

	require.NoError(t, ensureDebugAccessWith(context.Background(), dyn, testKeys))
	require.NoError(t, ensureDebugAccessWith(context.Background(), dyn, testKeys))

	assertNodeUser(t, getResource(t, dyn, nodeUsersGVR, DebugNodeUserName))
	assertSudoersConfig(t, getResource(t, dyn, nodeGroupConfigurationsGVR, debugSudoersConfigName))
}

// A NodeUser left over from an older revision — a rotated key, or the password
// hash this used to carry — has to converge rather than be left as it was, or a
// cluster kept across a key rotation stays locked.
func TestEnsureDebugAccess_OverwritesADivergedNodeUser(t *testing.T) {
	stale := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1",
			"kind":       "NodeUser",
			"metadata": map[string]any{
				"name":            DebugNodeUserName,
				"resourceVersion": "17",
				// The suite's own, which is what makes converging over it right.
				"labels": map[string]any{managedByLabelKey: managedByLabelValue},
			},
			"spec": map[string]any{
				"isSudoer":      false,
				"nodeGroups":    []any{"worker"},
				"passwordHash":  "$6$stale$stale",
				"sshPublicKeys": []any{"ssh-ed25519 AAAAstale stale"},
				"uid":           int64(999),
			},
		},
	}

	dyn := newFakeDebugAccess(t, stale)

	require.NoError(t, ensureDebugAccessWith(context.Background(), dyn, testKeys))
	// Notably this also drops the passwordHash the old object carried.
	assertNodeUser(t, getResource(t, dyn, nodeUsersGVR, DebugNodeUserName))
}

// Same for the sudoers rule: a cluster carrying an older, password-requiring
// version of the file has to be brought up to date.
func TestEnsureDebugAccess_OverwritesADivergedSudoersConfig(t *testing.T) {
	stale := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1alpha1",
			"kind":       "NodeGroupConfiguration",
			"metadata": map[string]any{
				"name":            debugSudoersConfigName,
				"resourceVersion": "23",
				// The suite's own, which is what makes converging over it right.
				"labels": map[string]any{managedByLabelKey: managedByLabelValue},
			},
			"spec": map[string]any{
				"bundles":    []any{"ubuntu-lts"},
				"nodeGroups": []any{"worker"},
				"weight":     int64(1),
				"content":    "echo stale",
			},
		},
	}

	dyn := newFakeDebugAccess(t, stale)

	require.NoError(t, ensureDebugAccessWith(context.Background(), dyn, testKeys))
	assertSudoersConfig(t, getResource(t, dyn, nodeGroupConfigurationsGVR, debugSudoersConfigName))
}

// Everything this package writes carries the standard managed-by label, which is
// what lets the access it leaves on the cluster be found and removed by selector
// instead of by knowing the two names.
func TestEnsureDebugAccess_LabelsWhatItOwns(t *testing.T) {
	dyn := newFakeDebugAccess(t)

	require.NoError(t, ensureDebugAccessWith(context.Background(), dyn, testKeys))

	for _, tc := range []struct {
		gvr  schema.GroupVersionResource
		name string
	}{
		{nodeUsersGVR, DebugNodeUserName},
		{nodeGroupConfigurationsGVR, debugSudoersConfigName},
	} {
		got := getResource(t, dyn, tc.gvr, tc.name)
		assert.Equal(t, managedByLabelValue, got.GetLabels()[managedByLabelKey], tc.name)
	}
}

// A tfadm somebody else manages is not the suite's to rewrite. Both names are
// ordinary enough for a cluster to carry its own, and replacing its keys, uid and
// node groups with these would take that cluster's access away from whoever set
// it up.
func TestEnsureDebugAccess_RefusesAForeignNodeUser(t *testing.T) {
	foreign := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1",
			"kind":       "NodeUser",
			"metadata": map[string]any{
				"name":            DebugNodeUserName,
				"resourceVersion": "5",
				"labels":          map[string]any{managedByLabelKey: "Helm"},
			},
			"spec": map[string]any{
				"isSudoer":      true,
				"nodeGroups":    []any{"worker"},
				"sshPublicKeys": []any{"ssh-ed25519 AAAAsomebodyelse theirs"},
				"uid":           int64(4242),
			},
		},
	}

	dyn := newFakeDebugAccess(t, foreign)

	err := ensureDebugAccessWith(context.Background(), dyn, testKeys)
	require.ErrorIs(t, err, ErrDebugAccessForeign)

	// And it is left exactly as it was, down to the uid.
	got := getResource(t, dyn, nodeUsersGVR, DebugNodeUserName)
	uid, found, err := unstructured.NestedInt64(got.Object, "spec", "uid")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(4242), uid, "the foreign object must not be rewritten")
}

// A tfadm made by hand carries no managed-by label at all, so "unlabelled" is the
// shape the object the suite has least claim to actually takes. Adopting it would
// replace somebody else's uid, keys and node groups — and undo a revocation the
// cluster's owner believed they had made — which is exactly what refusing an
// object under another manager is meant to prevent.
func TestEnsureDebugAccess_RefusesAnUnlabelledObject(t *testing.T) {
	unlabelled := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1",
			"kind":       "NodeUser",
			"metadata": map[string]any{
				"name":            DebugNodeUserName,
				"resourceVersion": "9",
			},
			"spec": map[string]any{
				"isSudoer":      true,
				"nodeGroups":    []any{"*"},
				"sshPublicKeys": []any{"ssh-ed25519 AAAAold old"},
				"uid":           debugNodeUserUID,
			},
		},
	}

	dyn := newFakeDebugAccess(t, unlabelled)

	require.ErrorIs(t, ensureDebugAccessWith(context.Background(), dyn, testKeys), ErrDebugAccessForeign)

	// And it is left exactly as it was, keys included.
	kept := getResource(t, dyn, nodeUsersGVR, DebugNodeUserName)
	keys, found, err := unstructured.NestedStringSlice(kept.Object, "spec", "sshPublicKeys")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []string{"ssh-ed25519 AAAAold old"}, keys)
}

// The opt-out has to work before the client is built, so a run pointed at a
// cluster it must not touch does not reach the API at all.
func TestEnsureDebugAccess_SkippedByEnv(t *testing.T) {
	t.Setenv(SkipDebugAccessEnv, "1")

	require.ErrorIs(t, EnsureDebugAccess(context.Background(), &rest.Config{}), ErrDebugAccessDisabled)
}

// The property that makes this safe to point at any cluster: with no keys
// configured nothing is installed, and the decision is reached before the client
// is built so the run does not touch the API at all. This is the default — the
// keys live in a CI secret, so a developer's plain `go test` grants nothing.
func TestEnsureDebugAccess_InstallsNothingWithoutKeys(t *testing.T) {
	t.Setenv(NodeSSHKeysEnv, "")

	require.ErrorIs(t, EnsureDebugAccess(context.Background(), &rest.Config{}), ErrDebugAccessNoKeys)
}

func TestDebugSSHKeys(t *testing.T) {
	const (
		ca       = `cert-authority,principals="tfadm" ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAtestca ca`
		fallback = `ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAtestfallback fallback`
	)
	plain := ca + "\n" + fallback + "\n"

	for _, tc := range []struct {
		name string
		raw  string
		want []any
		err  error
	}{
		{
			// What a developer exports locally, straight out of an authorized_keys
			// file: several lines, a comment, and a trailing newline.
			name: "authorized_keys text",
			raw:  "# team keys\n" + plain + "\n",
			want: []any{ca, fallback},
		},
		{
			// What CI delivers. The E2E_MODULE_ENV secret is KEY=VALUE and cannot
			// carry a newline, so the same text arrives base64-encoded.
			name: "base64 of the same text",
			raw:  base64.StdEncoding.EncodeToString([]byte(plain)),
			want: []any{ca, fallback},
		},
		{
			// GitHub Actions secrets keep surrounding whitespace, and a value pasted
			// into the web form usually ends with a newline.
			name: "base64 with surrounding whitespace",
			raw:  "\n  " + base64.StdEncoding.EncodeToString([]byte(plain)) + "  \n",
			want: []any{ca, fallback},
		},
		{
			name: "base64 without padding",
			raw:  base64.RawStdEncoding.EncodeToString([]byte(plain)),
			want: []any{ca, fallback},
		},
		{
			// A single key needs no newline, so it is indistinguishable from base64
			// by length alone — the space between the type and the key is what
			// decides, and every authorized_keys entry has one.
			name: "a single key is not mistaken for base64",
			raw:  fallback,
			want: []any{fallback},
		},
		{
			name: "unset",
			raw:  "",
			err:  ErrDebugAccessNoKeys,
		},
		{
			name: "nothing but comments",
			raw:  "# nothing here\n\n",
			err:  ErrDebugAccessNoKeys,
		},
		{
			// The one shape worth refusing rather than passing to the CRD: it is not
			// a malformed public key, it is a credential about to be written into a
			// cluster object anyone with get on NodeUsers can read.
			name: "a private key is refused",
			raw: base64.StdEncoding.EncodeToString([]byte(
				"-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaAAA\n-----END OPENSSH PRIVATE KEY-----\n")),
			err: ErrDebugAccessPrivateKey,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := debugSSHKeys(tc.raw)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				assert.Nil(t, got)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A value that holds no whitespace is read as base64, so one that is not base64
// either has to say so rather than reach the cluster as a single nonsense key.
func TestDebugSSHKeys_ReportsAValueThatIsNeither(t *testing.T) {
	_, err := debugSSHKeys("not-base64-and-not-a-key!!")
	require.Error(t, err)
	assert.Contains(t, err.Error(), NodeSSHKeysEnv)
}
