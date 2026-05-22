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
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	gossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"github.com/deckhouse/storage-e2e/pkg/kubernetes"
)

const e2eSSHJumpDialRetries = 5

type e2eNodeSSHEntry struct {
	client *e2eNodeSSHClient
	nodeIP string
}

var (
	e2eNodeSSHCacheMu sync.Mutex
	e2eNodeSSHCache   = make(map[string]*e2eNodeSSHEntry)
	e2eSSHHopLogged   sync.Map // hop route string → struct{}
)

// e2eNodeSSHClient runs commands on a test-cluster worker VM (direct or via jump host).
type e2eNodeSSHClient struct {
	target *gossh.Client
	jump   *gossh.Client // non-nil when connected through SSH_JUMP_HOST
}

func (c *e2eNodeSSHClient) Close() error {
	var firstErr error
	if c.target != nil {
		if err := c.target.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.jump != nil {
		if err := c.jump.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *e2eNodeSSHClient) Exec(ctx context.Context, cmd string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	session, err := c.target.NewSession()
	if err != nil {
		return "", fmt.Errorf("create SSH session: %w", err)
	}
	defer session.Close()

	type execResult struct {
		out []byte
		err error
	}
	done := make(chan execResult, 1)
	go func() {
		out, err := session.CombinedOutput(cmd)
		done <- execResult{out: out, err: err}
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-done:
		if res.err != nil {
			return string(res.out), fmt.Errorf("command failed: %w", res.err)
		}
		return string(res.out), nil
	}
}

func e2eSSHAddr(host string) string {
	if strings.Contains(host, ":") {
		return host
	}
	return net.JoinHostPort(host, "22")
}

func e2eExpandPath(path string) (string, error) {
	if !strings.HasPrefix(path, "~") {
		return path, nil
	}
	usr, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("get current user: %w", err)
	}
	if path == "~" {
		return usr.HomeDir, nil
	}
	return filepath.Join(usr.HomeDir, strings.TrimPrefix(path, "~/")), nil
}

// e2eGetSSHPrivateKeyPath resolves SSH_PRIVATE_KEY (file path or base64 PEM) to a readable key file.
func e2eGetSSHPrivateKeyPath() (string, error) {
	key := strings.TrimSpace(os.Getenv("SSH_PRIVATE_KEY"))
	if key == "" {
		key = "~/.ssh/id_rsa"
	}
	looksLikePath := strings.Contains(key, "/") || strings.HasPrefix(key, "~") || strings.Contains(key, "\\")
	if !looksLikePath {
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err == nil && len(decoded) > 0 {
			tmp, err := os.CreateTemp("", "e2e_ssh_private_key_*")
			if err != nil {
				return "", fmt.Errorf("create temp key file: %w", err)
			}
			name := tmp.Name()
			if _, err := tmp.Write(decoded); err != nil {
				_ = tmp.Close()
				_ = os.Remove(name)
				return "", fmt.Errorf("write temp key file: %w", err)
			}
			if err := tmp.Close(); err != nil {
				_ = os.Remove(name)
				return "", fmt.Errorf("close temp key file: %w", err)
			}
			if err := os.Chmod(name, 0o600); err != nil {
				_ = os.Remove(name)
				return "", fmt.Errorf("chmod temp key file: %w", err)
			}
			return name, nil
		}
	}
	return e2eExpandPath(key)
}

func e2eCreateSSHClientConfig(user, keyPath string) (*gossh.ClientConfig, error) {
	expanded, err := e2eExpandPath(keyPath)
	if err != nil {
		return nil, err
	}
	keyPEM, err := os.ReadFile(expanded)
	if err != nil {
		return nil, fmt.Errorf("read private key %s: %w", expanded, err)
	}

	var signers []gossh.Signer
	signer, err := gossh.ParsePrivateKey(keyPEM)
	if err != nil {
		if !strings.Contains(err.Error(), "ssh: this private key is passphrase protected") {
			return nil, fmt.Errorf("parse private key %s: %w", expanded, err)
		}
		pass := e2eConfigSSHPassphrase()
		if pass == "" {
			return nil, fmt.Errorf("SSH key %s is passphrase protected: set SSH_PASSPHRASE", expanded)
		}
		signer, err = gossh.ParsePrivateKeyWithPassphrase(keyPEM, []byte(pass))
		if err != nil {
			return nil, fmt.Errorf("parse private key %s with passphrase: %w", expanded, err)
		}
	}
	signers = append(signers, signer)

	if agentSock := os.Getenv("SSH_AUTH_SOCK"); agentSock != "" {
		if conn, err := net.Dial("unix", agentSock); err == nil {
			agentSigners, agentErr := agent.NewClient(conn).Signers()
			if agentErr == nil {
				signers = append(signers, agentSigners...)
			} else {
				_ = conn.Close()
			}
		}
	}

	return &gossh.ClientConfig{
		User:            user,
		Auth:            []gossh.AuthMethod{gossh.PublicKeys(signers...)},
		HostKeyCallback: gossh.InsecureIgnoreHostKey(),
	}, nil
}

func e2eDialSSH(user, host, keyPath string) (*gossh.Client, error) {
	cfg, err := e2eCreateSSHClientConfig(user, keyPath)
	if err != nil {
		return nil, err
	}
	return gossh.Dial("tcp", e2eSSHAddr(host), cfg)
}

// e2eResolveJumpHost picks the SSH bastion for node InternalIPs (10.10.10.x). The CI runner cannot dial them directly.
//
// SSH_JUMP_* is optional. By default we hop via SSH_HOST (same as storage-e2e bootstrap: SSH_USER@SSH_HOST → SSH_VM_USER@node).
//   - alwaysCreateNew: SSH_HOST is the base cluster master.
//   - alwaysUseExisting: SSH_HOST is the test cluster master (must route to worker InternalIPs).
func e2eResolveJumpHost(nodeIP string) (host, source string) {
	if h := strings.TrimSpace(e2eConfigSSHJumpHost()); h != "" {
		return h, "SSH_JUMP_HOST (optional override)"
	}
	ip := net.ParseIP(strings.TrimSpace(nodeIP))
	if ip != nil && ip.IsPrivate() {
		if h := strings.TrimSpace(e2eConfigSSHHost()); h != "" {
			return h, "SSH_HOST"
		}
	}
	return "", ""
}

func e2eNewNodeSSHClient(sshUser, nodeIP, keyPath string) (*e2eNodeSSHClient, error) {
	jumpHost, jumpSource := e2eResolveJumpHost(nodeIP)
	if jumpHost == "" {
		ip := net.ParseIP(strings.TrimSpace(nodeIP))
		if ip != nil && ip.IsPrivate() {
			return nil, fmt.Errorf(
				"node has private IP %s but SSH_HOST is empty: set SSH_HOST to a host that can reach the node (base cluster for alwaysCreateNew, test master for alwaysUseExisting)",
				nodeIP,
			)
		}
		target, err := e2eDialSSH(sshUser, nodeIP, keyPath)
		if err != nil {
			return nil, fmt.Errorf("SSH to node %s@%s: %w", sshUser, nodeIP, err)
		}
		return &e2eNodeSSHClient{target: target}, nil
	}

	jumpUser := strings.TrimSpace(e2eConfigSSHJumpUser())
	if jumpUser == "" {
		jumpUser = strings.TrimSpace(e2eConfigSSHUser())
	}
	jumpKeyPath := strings.TrimSpace(e2eConfigSSHJumpKeyPath())
	if jumpKeyPath == "" {
		jumpKeyPath = keyPath
	}

	hopKey := fmt.Sprintf("%s@%s|%s@%s|%s", jumpUser, jumpHost, sshUser, nodeIP, jumpSource)
	if _, loaded := e2eSSHHopLogged.LoadOrStore(hopKey, struct{}{}); !loaded {
		GinkgoWriter.Printf("      SSH hop to node %s@%s via %s@%s (%s)\n", sshUser, nodeIP, jumpUser, jumpHost, jumpSource)
	}

	jumpClient, err := e2eDialSSH(jumpUser, jumpHost, jumpKeyPath)
	if err != nil {
		return nil, fmt.Errorf("SSH to bastion %s@%s (%s): %w", jumpUser, jumpHost, jumpSource, err)
	}

	targetAddr := e2eSSHAddr(nodeIP)
	targetCfg, err := e2eCreateSSHClientConfig(sshUser, keyPath)
	if err != nil {
		_ = jumpClient.Close()
		return nil, err
	}

	var (
		targetClient *gossh.Client
		lastErr      error
	)
	for attempt := 0; attempt < e2eSSHJumpDialRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		targetConn, dialErr := jumpClient.Dial("tcp", targetAddr)
		if dialErr != nil {
			lastErr = fmt.Errorf("dial %s via %s@%s: %w", targetAddr, jumpUser, jumpHost, dialErr)
			continue
		}
		targetClientConn, targetChans, targetReqs, connErr := gossh.NewClientConn(targetConn, targetAddr, targetCfg)
		if connErr != nil {
			_ = targetConn.Close()
			lastErr = fmt.Errorf("SSH handshake to %s@%s via bastion: %w", sshUser, nodeIP, connErr)
			continue
		}
		targetClient = gossh.NewClient(targetClientConn, targetChans, targetReqs)
		lastErr = nil
		break
	}
	if targetClient == nil {
		_ = jumpClient.Close()
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("SSH to %s@%s via %s@%s failed after %d attempts", sshUser, nodeIP, jumpUser, jumpHost, e2eSSHJumpDialRetries)
	}

	return &e2eNodeSSHClient{
		target: targetClient,
		jump:   jumpClient,
	}, nil
}

func e2eGetNodeInternalIP(ctx context.Context, kubeconfig *rest.Config, nodeName string) (string, error) {
	clientset, err := kubernetes.NewClientsetWithRetry(ctx, kubeconfig)
	if err != nil {
		return "", fmt.Errorf("create clientset: %w", err)
	}
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get node %s: %w", nodeName, err)
	}
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address, nil
		}
	}
	return "", fmt.Errorf("node %s has no InternalIP", nodeName)
}

func e2eNodeSSHCacheKey(nodeName, sshUser string) string {
	return nodeName + "\x00" + sshUser
}

// e2eConnectToTestClusterNode returns a cached SSH client per (nodeName, sshUser) for the suite run.
// Reuses the jump tunnel instead of redialing on every Eventually poll; call e2eCloseNodeSSHCache in AfterSuite.
func e2eConnectToTestClusterNode(ctx context.Context, testKubeconfig *rest.Config, nodeName, sshUser string) (*e2eNodeSSHClient, string, error) {
	key := e2eNodeSSHCacheKey(nodeName, sshUser)

	e2eNodeSSHCacheMu.Lock()
	if e, ok := e2eNodeSSHCache[key]; ok && e != nil && e.client != nil {
		e2eNodeSSHCacheMu.Unlock()
		return e.client, e.nodeIP, nil
	}
	e2eNodeSSHCacheMu.Unlock()

	nodeIP, err := e2eGetNodeInternalIP(ctx, testKubeconfig, nodeName)
	if err != nil {
		return nil, "", err
	}
	keyPath, err := e2eGetSSHPrivateKeyPath()
	if err != nil {
		return nil, "", err
	}
	client, err := e2eNewNodeSSHClient(sshUser, nodeIP, keyPath)
	if err != nil {
		return nil, nodeIP, err
	}

	e2eNodeSSHCacheMu.Lock()
	defer e2eNodeSSHCacheMu.Unlock()
	if e, ok := e2eNodeSSHCache[key]; ok && e != nil && e.client != nil {
		_ = client.Close()
		return e.client, e.nodeIP, nil
	}
	e2eNodeSSHCache[key] = &e2eNodeSSHEntry{client: client, nodeIP: nodeIP}
	return client, nodeIP, nil
}

func e2eCloseNodeSSHCache() {
	e2eNodeSSHCacheMu.Lock()
	defer e2eNodeSSHCacheMu.Unlock()
	for k, e := range e2eNodeSSHCache {
		if e != nil && e.client != nil {
			_ = e.client.Close()
		}
		delete(e2eNodeSSHCache, k)
	}
}
