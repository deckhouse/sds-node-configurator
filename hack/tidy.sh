#!/bin/bash
# Runs `go generate ./...`` from all the folders with `go.mod` file

find -type f -name go.mod -execdir sh -c "go mod tidy" {} +
