# Development tools

## `pre-commit` tool

[pre-commit](https://pre-commit.com/) is a tool to run linters or any other scripts automatically on `git commit` or by hands. See [.pre-commit-config.yaml](/.pre-commit-config.yaml)

### Installing pre-commit hook

To install hooks

```sh
pre-commit install
```

Remove hooks

```sh
pre-commit unistall
```

To commit without hooks git has `--no-verify` argument

```sh
git commit --no-verify
```

To skip some checks `SKIP` environment variable is respected by `pre-commit` tool

```sh
SKIP=go-mod-tidy git commit <...>
```

### Running hooks manually

To run pre-commit all hooks on staged files

```sh
pre-commit run
```

To select tools to run

```sh
pre-commit run go-mod-tidy
```

To run on all the files regardles if they changed or not there is `--all-files` argument

```sh
pre-commit run [check_to_run] --all-files
```

## `for-each-mod` tool

We have multiple go packages in our repo. To run command in all of them at once we have a [tool](/hack/for-each-mod) to run the same command in all the folder with go.mod file.

For example:

```sh
./hack/for-each-mod go mod tidy
```

## Code generation

To generate code we are using `go generate`

### Specifying generation command

To add generation command in `.go` file add a comment

```go
//go:generate <command>
```

For example to generate mock file:

```go
//go:generate go run go.uber.org/mock/mockgen -copyright_file ../../../../hack/boilerplate.txt -write_source_comment -destination=../mock_utils/$GOFILE -source=$GOFILE
```

To make tool available

1. Add it to go.mod:

    ```sh
    go get go.uber.org/mock/mockgen
    ```

2. Make a fake usage adding `tools.go`. Without this usage `go mod tidy` will remove it from `go.mod`

    ```go
    //go:build tools
    // +build tools
    
    /* LICENSE HERE */
    
    package tools

    import _ "go.uber.org/mock/mockgen"
    ```

### Running generation command

To run in single module

```sh
go generate ./...
```

To run for all the modules

```sh
./hack/for-each-mod go generate ./...
```
