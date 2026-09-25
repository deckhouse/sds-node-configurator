#!/bin/bash

# Copyright 2026 Flant JSC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Regenerates api/v1alpha1/zz_generated.deepcopy.go.
#
# controller-gen is pinned in hack/tools/go.mod and built into a temporary
# directory. It used to be a `tool` of api/go.mod, which drags controller-tools'
# own pins (k8s.io/apiextensions-apiserver, k8s.io/code-generator) into the module
# graph of everything that imports api/ -- the images here, and the other storage
# modules that consume this API. A code generator should not be choosing what
# production code is built against.

set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

tools_dir=$(mktemp -d)
trap 'rm -rf "${tools_dir}"' EXIT

echo "Building controller-gen (pinned in hack/tools/go.mod)"
GOWORK=off go -C "${root_dir}/hack/tools" build -o "${tools_dir}" \
    sigs.k8s.io/controller-tools/cmd/controller-gen

cd "${root_dir}/api/v1alpha1"

echo "Generating deepcopy code"
"${tools_dir}/controller-gen" object:headerFile="${root_dir}/hack/boilerplate.txt" paths=.
