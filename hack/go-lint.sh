#!/usr/bin/env bash

# Copyright 2025 Flant JSC
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

# Run the Go linter the same way CI does (deckhouse/modules-actions/go_linter@v12):
#   * install a pinned golangci-lint (v2.9.0 by default) into hack/.bin
#   * for every go.mod under images/ and every build tag in GO_BUILD_TAGS
#     ("ce ee se seplus csepro") run `golangci-lint run --allow-parallel-runners
#     --build-tags <edition>`
#   * keep going after individual failures and exit non-zero if any run failed
#
# Usage:
#   hack/go-lint.sh                  # report-only (same as CI)
#   hack/go-lint.sh --fix            # apply `golangci-lint fmt` first (fixes gci/gofmt/goimports)
#   hack/go-lint.sh --fix-only       # only apply formatters, do not run linters
#   hack/go-lint.sh --module controller        # limit to images/controller/go.mod
#   hack/go-lint.sh --edition ce               # limit to a single build tag
#   hack/go-lint.sh --module controller --edition ce --fix
#
# Env overrides:
#   GOLANGCI_LINT_VERSION   golangci-lint tag to install (default: v2.9.0, same as CI)
#   GO_BUILD_TAGS           space-separated editions to lint (default: ce ee se seplus csepro)

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
bin_dir="${repo_root}/hack/.bin"

: "${GOLANGCI_LINT_VERSION:=v2.9.0}"
: "${GO_BUILD_TAGS:=ce ee se seplus csepro}"

fix=false
fix_only=false
only_edition=""
only_module=""

usage() {
  sed -n '17,34p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
  case "$1" in
    --fix)        fix=true; shift ;;
    --fix-only)   fix=true; fix_only=true; shift ;;
    --edition)    only_edition="${2:-}"; shift 2 ;;
    --module)     only_module="${2:-}"; shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *)            echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if ! command -v go >/dev/null 2>&1; then
  echo "go toolchain is required but not found in PATH" >&2
  exit 1
fi

# Install a version-pinned golangci-lint into hack/.bin. A sentinel file lets us
# detect version drift without invoking the binary (faster and avoids edge cases
# where the binary cannot print its version under newer macOS/linux variants).
mkdir -p "${bin_dir}"
lint_bin="${bin_dir}/golangci-lint"
version_sentinel="${bin_dir}/.installed-${GOLANGCI_LINT_VERSION}"
if [ ! -x "${lint_bin}" ] || [ ! -f "${version_sentinel}" ]; then
  echo ">>> Installing golangci-lint ${GOLANGCI_LINT_VERSION} into ${bin_dir}"
  rm -f "${bin_dir}"/.installed-* || true
  GOBIN="${bin_dir}" go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}"
  touch "${version_sentinel}"
fi
export PATH="${bin_dir}:${PATH}"

# Collect images/*/go.mod, optionally narrowed with --module.
mapfile -t modules < <(cd "${repo_root}" && find images -type f -name go.mod | sort)
if [ -n "${only_module}" ]; then
  filtered=()
  for m in "${modules[@]}"; do
    if [[ "${m}" == *"${only_module}"* ]]; then
      filtered+=("${m}")
    fi
  done
  modules=("${filtered[@]:-}")
fi
if [ ${#modules[@]} -eq 0 ] || [ -z "${modules[0]:-}" ]; then
  echo "No Go modules matched under images/ (module filter: '${only_module}')" >&2
  exit 1
fi

# Optionally narrow editions via --edition.
editions=()
for edition in ${GO_BUILD_TAGS}; do
  if [ -n "${only_edition}" ] && [ "${edition}" != "${only_edition}" ]; then
    continue
  fi
  editions+=("${edition}")
done
if [ ${#editions[@]} -eq 0 ]; then
  echo "No editions matched (edition filter: '${only_edition}', GO_BUILD_TAGS: '${GO_BUILD_TAGS}')" >&2
  exit 1
fi

group_open()  { if [ "${GITHUB_ACTIONS:-}" = "true" ]; then echo "::group::$*"; else echo; echo "=== $* ==="; fi; }
group_close() { if [ "${GITHUB_ACTIONS:-}" = "true" ]; then echo "::endgroup::"; fi; }

failed=0

for mod in "${modules[@]}"; do
  dir="$(dirname "${mod}")"
  cd "${repo_root}/${dir}"

  # Formatters (gci, gofmt, goimports, ...) don't type-check; run once per module.
  if [ "${fix}" = true ]; then
    group_open "golangci-lint fmt ${dir}"
    if ! golangci-lint fmt; then
      echo "Formatter reported issues in ${dir}"
      failed=1
    fi
    group_close
  fi

  if [ "${fix_only}" = true ]; then
    continue
  fi

  for edition in "${editions[@]}"; do
    group_open "golangci-lint run ${dir} (edition: ${edition})"
    if ! golangci-lint run --allow-parallel-runners --build-tags "${edition}"; then
      echo "Linter failed in ${dir} (edition: ${edition})"
      failed=1
    fi
    group_close
  done
done

if [ ${failed} -ne 0 ]; then
  echo
  echo ">>> One or more lint runs failed"
  exit 1
fi

echo
echo ">>> All lint runs passed"
