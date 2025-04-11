#!/bin/bash

echo "Running go generate"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"

(cd "$REPO_ROOT" && make go-generate)