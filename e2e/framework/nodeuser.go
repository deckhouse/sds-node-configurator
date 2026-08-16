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
	"errors"
	"fmt"
	"os"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// DebugNodeUserName is the NodeUser this package maintains on the test cluster.
const DebugNodeUserName = "tfadm"

// debugNodeUserUID is fixed so files the account leaves behind keep the same
// ownership across cluster rebuilds.
const debugNodeUserUID = int64(12000)

// debugSudoersConfigName is the NodeGroupConfiguration that drops the sudoers
// rule. The .sh suffix is part of node-manager's naming convention for these.
const debugSudoersConfigName = "tfadm-custom.sh"

// SkipDebugAccessEnv turns the whole thing off. These resources grant passwordless
// root on every node of every node group and nothing takes them away afterwards —
// which is the point on a cluster the run created, and is not something to do to a
// cluster somebody else owns. Pointing the suite at such a cluster is the case this
// exists for.
//
// It is the second of two switches and the weaker one: without NodeSSHKeysEnv
// there is nothing to install in the first place, so a run that configures
// neither installs nothing.
const SkipDebugAccessEnv = "E2E_SKIP_NODE_ACCESS"

// NodeSSHKeysEnv carries the authorized_keys entries the NodeUser is created
// with — the whole of what this package grants access to, so it is what decides
// whether the grant happens at all.
//
// Supplied by the run rather than committed. These are public keys, so a
// committed copy would not be a leaked credential, but it would publish the trust
// configuration of the team's node access — which CA signs the tfadm principal,
// and a standing fallback key with no expiry — in a public repository whose
// history cannot be rewritten afterwards, so a later rotation could never be made
// retroactive.
//
// Format: authorized_keys lines, one per key, blanks and # comments ignored.
// Because the CI path that delivers this is KEY=VALUE and cannot carry a newline
// (see the E2E_MODULE_ENV secret in deckhouse/storage-e2e), the value may also be
// the same text base64-encoded. The two are told apart by a space: every
// authorized_keys line has one, and the standard base64 alphabet has none.
//
//	# CI, in the E2E_MODULE_ENV repository secret:
//	E2E_NODE_SSH_AUTHORIZED_KEYS=<base64 of the authorized_keys block>
//
//	# locally:
//	export E2E_NODE_SSH_AUTHORIZED_KEYS="$(cat ~/.ssh/e2e_authorized_keys)"
const NodeSSHKeysEnv = "E2E_NODE_SSH_AUTHORIZED_KEYS"

// The standard label for "which tool manages this object"
// (https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/).
// It is written on everything this file creates, so the access the suite leaves
// behind can be found and removed with one selector instead of by knowing the two
// names, and so ensureResource can tell an object of its own from one that belongs
// to somebody else.
const (
	managedByLabelKey   = "app.kubernetes.io/managed-by"
	managedByLabelValue = "sds-node-configurator-e2e"
)

// Both resources are addressed dynamically rather than through a typed client
// because the e2e module registers only the sds-node-configurator API group (see
// sdsclient.New), and pulling the whole Deckhouse API surface in for two debug
// resources is not worth the dependency.
var (
	nodeUsersGVR = schema.GroupVersionResource{
		Group:    "deckhouse.io",
		Version:  "v1",
		Resource: "nodeusers",
	}

	nodeGroupConfigurationsGVR = schema.GroupVersionResource{
		Group:    "deckhouse.io",
		Version:  "v1alpha1",
		Resource: "nodegroupconfigurations",
	}
)

// ErrDebugAccessCRDAbsent reports that the cluster has neither kind registered,
// which is what a disabled node-manager looks like. Callers are meant to treat
// it as "nothing to do": shell access is a debugging convenience, not a
// precondition of any spec.
var ErrDebugAccessCRDAbsent = errors.New("NodeUser/NodeGroupConfiguration CRDs are not registered in the cluster (node-manager disabled?)")

// ErrDebugAccessDisabled reports that the run asked for this to be skipped. Like
// ErrDebugAccessCRDAbsent it is a "nothing to do", not a failure.
var ErrDebugAccessDisabled = errors.New(SkipDebugAccessEnv + " is set, not installing node access")

// ErrDebugAccessNoKeys reports that no key was supplied, which is also a "nothing
// to do" — and the reason this whole thing is opt-in rather than opt-out. A
// NodeUser with no key grants nothing and would only leave an account and a
// passwordless-sudo rule behind on somebody's cluster, so the absence of keys is
// taken as "do not install", never as "install an empty one".
var ErrDebugAccessNoKeys = errors.New(NodeSSHKeysEnv + " holds no SSH key, not installing node access")

// ErrDebugAccessPrivateKey reports a private key where authorized_keys entries
// were expected. It is a failure rather than a "nothing to do": the value is
// wrong in a way somebody has to hear about, and the material is about to be
// written into a cluster object readable by anyone with get on NodeUsers.
var ErrDebugAccessPrivateKey = errors.New("the value looks like a PRIVATE key; " + NodeSSHKeysEnv + " takes authorized_keys (public) entries")

// ErrDebugAccessForeign reports that one of the two names is taken by an object
// another tool manages. Writing over it would replace somebody else's access
// configuration — a different set of keys, a different uid, a different set of
// node groups — with this one, and the suite has no claim to it. Leave it alone
// and let the caller say so.
var ErrDebugAccessForeign = errors.New("the object is managed by something other than this suite")

// EnsureDebugAccess puts the two resources that give the storage team shell
// access to every node of the test cluster: the NodeUser itself, and the
// NodeGroupConfiguration that lets it sudo without a password.
//
// It exists because a failed dvp run is close to undiagnosable from the job
// artifact alone: the artifact holds the ginkgo output and nothing else — no
// agent log, no lsblk from the node — so answering "what did master-1 actually
// look like" means getting onto the node. With the e2e/keep-cluster label the
// cluster outlives the run, and these two are what make it usable.
//
// Idempotent by construction: the desired spec is written over the suite's own
// copy, so repeated runs against a kept cluster converge instead of failing on
// AlreadyExists, and a cluster that outlived a key rotation picks the new keys
// up. What it will not write over is an object another tool manages — see
// ensureResource.
//
// Nothing removes what this puts on the cluster, and that is deliberate: the
// access is worth having precisely after the run that failed. Because the grant
// outlives the suite, it is asked for rather than declined: a run that supplies no
// NodeSSHKeysEnv installs nothing at all, so pointing the suite at a cluster
// somebody else owns cannot grant anything by default. SkipDebugAccessEnv is the
// explicit off switch for a run that does have keys configured.
func EnsureDebugAccess(ctx context.Context, restCfg *rest.Config) error {
	if os.Getenv(SkipDebugAccessEnv) != "" {
		return ErrDebugAccessDisabled
	}

	// Before the client is built, so a run with nothing to install does not reach
	// the API at all.
	keys, err := debugSSHKeys(os.Getenv(NodeSSHKeysEnv))
	if err != nil {
		return err
	}

	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building dynamic client: %w", err)
	}

	return ensureDebugAccessWith(ctx, dyn, keys)
}

// debugSSHKeys parses the authorized_keys entries the NodeUser is created with.
//
// raw is either the authorized_keys text itself or that text base64-encoded, and
// the two are told apart by a space: every authorized_keys line carries one
// between the type and the key, and the standard base64 alphabet has none. The
// encoded form exists because the CI path delivering this is KEY=VALUE and cannot
// carry a newline — see NodeSSHKeysEnv.
//
// What it does NOT do is validate the key material properly. That would mean a
// direct dependency on x/crypto for a debugging convenience, and the NodeUser CRD
// rejects malformed entries anyway. The one shape worth catching here is a
// private key pasted in by mistake, because that one is not a malformed public
// key — it is a credential about to be written into a cluster object, and the CRD
// would take it.
func debugSSHKeys(raw string) ([]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, ErrDebugAccessNoKeys
	}

	if !strings.ContainsAny(raw, " \t\n") {
		// StdEncoding already pads; the unpadded form is the fallback below, which
		// is what a `base64 -w0` from a shell that trimmed the padding produces.
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			if decoded, err = base64.RawStdEncoding.DecodeString(raw); err != nil {
				return nil, fmt.Errorf("decoding %s: it holds no whitespace, so it was read as base64: %w", NodeSSHKeysEnv, err)
			}
		}
		raw = string(decoded)
	}

	if strings.Contains(raw, "PRIVATE KEY") {
		return nil, ErrDebugAccessPrivateKey
	}

	keys := make([]any, 0, 3)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		keys = append(keys, line)
	}

	if len(keys) == 0 {
		return nil, ErrDebugAccessNoKeys
	}

	return keys, nil
}

// ensureDebugAccessWith holds the logic, split from the constructor so the tests
// can drive it with a fake dynamic client.
func ensureDebugAccessWith(ctx context.Context, dyn dynamic.Interface, keys []any) error {
	if err := ensureResource(ctx, dyn, nodeUsersGVR, debugNodeUser(keys)); err != nil {
		return err
	}

	// The sudoers rule is the half that makes the account useful. isSudoer only
	// puts the user in the nodeadmin group, for which bashible writes
	// "%nodeadmin ALL=(ALL) ALL" into /etc/sudoers.d/30-deckhouse-nodeadmins —
	// ALL, not NOPASSWD: ALL. The NodeUser carries no password (deliberately, so
	// none has to live in this repository), so without this rule sudo would
	// prompt for a password that does not exist and the account could not sudo at
	// all. NodeUser has no field for it; a NodeGroupConfiguration is the way
	// node-manager expresses "put this file on every node".
	return ensureResource(ctx, dyn, nodeGroupConfigurationsGVR, debugSudoersConfig())
}

// ensureResource creates the object, or writes the desired state over the one
// already there. Cluster-scoped: both kinds are.
//
// "Already there" is not by itself a licence to write over it. Converging is
// right for an object of the suite's own — that is what picks up a rotated key on
// a cluster kept across runs, and what drops the passwordHash an older revision
// wrote. It is wrong for an object somebody else manages: the two names this file
// uses are ordinary enough that a cluster can carry a hand-made or
// Helm/Argo-managed tfadm of its own, and replacing that one's keys, uid and node
// groups with these is not a change the suite is entitled to make. An object
// under another manager is therefore refused, not overwritten.
//
// An unlabelled object is refused too, and that is the point rather than an
// oversight. A tfadm somebody made by hand — the case the paragraph above is
// about — carries no app.kubernetes.io/managed-by at all, so admitting unlabelled
// objects would have refused exactly the objects the suite has no claim to least,
// and adopted the ones it has no claim to at all: a different uid, a different set
// of keys, a revocation the cluster's owner believed they had made.
//
// The one thing this costs is a stand that carries an object an earlier revision
// of this file wrote before the label existed. Those are labelled by hand, once,
// rather than paid for by a rule that lives forever.
func ensureResource(
	ctx context.Context,
	dyn dynamic.Interface,
	gvr schema.GroupVersionResource,
	desired *unstructured.Unstructured,
) error {
	name := desired.GetName()
	client := dyn.Resource(gvr)

	existing, err := client.Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
		if owner := existing.GetLabels()[managedByLabelKey]; owner != managedByLabelValue {
			return fmt.Errorf("%s %s is labelled %s=%q, not %q: %w",
				gvr.Resource, name, managedByLabelKey, owner, managedByLabelValue, ErrDebugAccessForeign)
		}

		// Carry the resourceVersion over: an update without it is rejected.
		desired.SetResourceVersion(existing.GetResourceVersion())
		if _, updErr := client.Update(ctx, desired, metav1.UpdateOptions{}); updErr != nil {
			return fmt.Errorf("updating %s %s: %w", gvr.Resource, name, updErr)
		}

		return nil

	case meta.IsNoMatchError(err):
		// The kind itself is unknown, which is a different thing from "the object
		// does not exist yet" and is the caller's to ignore.
		return ErrDebugAccessCRDAbsent

	case apierrors.IsNotFound(err):
		if _, createErr := client.Create(ctx, desired, metav1.CreateOptions{}); createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				// Another process created it between the Get and the Create; its
				// copy is this same spec, so there is nothing left to do.
				return nil
			}

			return fmt.Errorf("creating %s %s: %w", gvr.Resource, name, createErr)
		}

		return nil

	default:
		return fmt.Errorf("reading %s %s: %w", gvr.Resource, name, err)
	}
}

// debugNodeUser is the desired NodeUser. isSudoer and the '*' nodeGroups make the
// account usable on every node the suite might need to look at, masters
// included — which is the point, since master-1 is where the disk-attaching
// specs run.
//
// keys comes from the run, never from this file — see NodeSSHKeysEnv. The caller
// has already refused to build one with an empty set, since a NodeUser nobody can
// log into is an account and a sudo rule left on a cluster for nothing.
func debugNodeUser(keys []any) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1",
			"kind":       "NodeUser",
			"metadata": map[string]any{
				"name":   DebugNodeUserName,
				"labels": map[string]any{managedByLabelKey: managedByLabelValue},
			},
			// No passwordHash on purpose: the account authenticates by key, and
			// sudo is made passwordless by the NodeGroupConfiguration below rather
			// than by knowing a password. Leaving the field out means useradd runs
			// without -p, so the shadow entry stays locked and no password hash
			// has to live in this repository. passwordHash is not required by the
			// CRD — uid and one of sshPublicKey/sshPublicKeys are.
			"spec": map[string]any{
				"isSudoer":      true,
				"nodeGroups":    []any{"*"},
				"sshPublicKeys": keys,
				"uid":           debugNodeUserUID,
			},
		},
	}
}

// debugSudoersConfig is the desired NodeGroupConfiguration. bb-sync-file writes
// the file only when its content differs, so this is a no-op on every bashible
// pass after the first. weight 100 puts it after the steps that create the users
// it grants rights to.
func debugSudoersConfig() *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "deckhouse.io/v1alpha1",
			"kind":       "NodeGroupConfiguration",
			"metadata": map[string]any{
				"name":   debugSudoersConfigName,
				"labels": map[string]any{managedByLabelKey: managedByLabelValue},
			},
			"spec": map[string]any{
				"bundles":    []any{"*"},
				"nodeGroups": []any{"*"},
				"weight":     int64(100),
				"content":    debugSudoersContent,
			},
		},
	}
}

// debugSudoersContent lands as /etc/sudoers.d/<DebugNodeUserName>. It overrides
// the password-requiring %nodeadmin rule bashible writes into
// 30-deckhouse-nodeadmins: sudo applies the last match, and a user-specific rule
// outranks a group one.
//
// One rule, for the one account this file creates. It used to carry a second,
// for an account named "e2e" that nothing in this repository creates, references
// or documents — a passwordless-root grant with no owner, on every node of every
// node group, that nothing takes away. Whatever needs such a grant has to bring
// its own NodeUser and its own rule, so that the account and the privilege stay
// together and can be found and removed as one thing.
//
// Built from the constants rather than written out, because the account and the
// rule have to name the same user and nothing else checks that they do. Spelled
// literally, a rename of DebugNodeUserName compiles, passes every test here, and
// puts a NOPASSWD rule for a user that does not exist onto every node — which
// takes passwordless sudo away from the account that does, and with it every
// caller that needs root on a node: framework.WipeE2ELVM refuses to run at all
// without `+"`sudo -n`"+`, by design.
var debugSudoersContent = fmt.Sprintf(`bb-sync-file /etc/sudoers.d/%[1]s - << "EOF"
# Created by NodeGroupConfiguration %[2]s
# User rules for %[1]s
%[1]s ALL=(ALL) NOPASSWD:ALL
EOF
`, DebugNodeUserName, debugSudoersConfigName)
