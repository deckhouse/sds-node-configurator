/*
Copyright 2023 Flant JSC

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

package lvm

import (
	"agent/internal"
	"agent/internal/nsenter"
	"context"
	"os/exec"
)

func makeCommand(name string, arg ...string) (string, []string) {
	_arg := append([]string{name}, arg...)
	return internal.LVMCmd, _arg
}

func Command(name string, arg ...string) *exec.Cmd {
	_name, _arg := makeCommand(name, arg...)
	return nsenter.Command(_name, _arg...)
}

func CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd {
	_name, _arg := makeCommand(name, arg...)
	return nsenter.CommandContext(ctx, _name, _arg...)
}
