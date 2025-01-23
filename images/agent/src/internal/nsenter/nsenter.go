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

package nsenter

import (
	"agent/internal"
	"context"
	"os/exec"
)

type Cmd struct {
	exec.Cmd
}

func MakeCommand(name string, arg ...string) (string, []string) {
	_arg := append([]string{"-t", "1", "-m", "-u", "-i", "-n", "-p", "--", name}, arg...)
	return internal.NSENTERCmd, _arg
}

func Command(name string, arg ...string) *Cmd {
	_name, _arg := MakeCommand(name, arg...)
	return &Cmd{*exec.Command(_name, _arg...)}
}

func CommandContext(ctx context.Context, name string, arg ...string) *Cmd {
	_name, _arg := MakeCommand(name, arg...)
	return &Cmd{*exec.CommandContext(ctx, _name, _arg...)}
}
