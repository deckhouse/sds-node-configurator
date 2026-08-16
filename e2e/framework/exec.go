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
	"fmt"
	"strings"

	"github.com/deckhouse/storage-e2e/pkg/e2e"
)

// ExitCodeError is a command that ran and failed, as opposed to one that could not
// be run at all. It carries the code so a caller whose script uses exit codes to
// say WHY it failed can tell those reasons apart without matching on the message.
type ExitCodeError struct {
	Node     string
	ExitCode int
	Stderr   string
}

func (e *ExitCodeError) Error() string {
	return fmt.Sprintf("command on node %s exited with code %d: %s", e.Node, e.ExitCode, e.Stderr)
}

// NodeExecChecked runs cmd on node via the SDK NodeExecutor and returns the
// command's stdout. A non-zero exit code is surfaced as an *ExitCodeError (with
// stderr included); an err from Exec itself signals a transport failure.
func NodeExecChecked(ctx context.Context, cl *e2e.Cluster, node, cmd string) (string, error) {
	res, err := cl.Nodes().Exec(ctx, node, cmd)
	if err != nil {
		return "", fmt.Errorf("exec on node %s: %w", node, err)
	}
	stdout := string(res.Stdout)
	if res.ExitCode != 0 {
		return stdout, &ExitCodeError{
			Node:     node,
			ExitCode: res.ExitCode,
			Stderr:   strings.TrimSpace(string(res.Stderr)),
		}
	}
	return stdout, nil
}
