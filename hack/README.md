# Hack Scripts

This folder contains auxiliary scripts for development and project maintenance.

## git_commits_after_tag.py

Script for getting a list of commits that are not included in the latest tag.

### Description

The script performs the following actions:

1. Switches to the main branch
2. Executes git fetch to get updates
3. Finds the latest tag in the repository
4. Displays a list of commits after the latest tag

### Usage

```bash
# From project root folder
python3 hack/git_commits_after_tag.py
```

### Requirements

- Python 3.6+
- Git repository

### Functionality

- **Automatic switch to main**: The script automatically switches to the main branch
- **Getting updates**: Executes `git fetch --all` to get the latest changes
- **Finding latest tag**: Finds the newest tag in the repository
- **List of commits**: Shows all commits after the latest tag
- **Detailed information**: Displays change statistics for each commit
- **Error handling**: Informative error messages

### Example Output

```
🚀 Script for getting commits after latest tag
============================================================
📍 Current branch: main
🔄 Switching to main branch...
✅ Successfully switched to main branch
🔄 Getting updates (git fetch)...
✅ Updates received successfully
🔍 Searching for latest tag...
✅ Latest tag: v0.2.4
🔍 Searching for commits after tag v0.2.4...
✅ Found 2 commits after tag v0.2.4

📋 List of commits after latest tag (2 commits):
============================================================
 1. a1b2c3d - feat: add new feature
 2. e4f5g6h - fix: resolve bug in authentication
============================================================
```

### Features

- Works with any git repositories
- Automatically handles absence of tags
- Shows detailed information about the first 10 commits
- Excludes merge commits from the list
- Supports colored output with emojis for better readability

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

Install a tool

```sh
go get -tool go.uber.org/mock/mockgen
```

Add annotation

```go
//go:generate go tool mockgen -copyright_file ../../../../hack/boilerplate.txt -write_source_comment -destination=../mock_utils/$GOFILE -source=$GOFILE
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

## CSpell

We added very simple [CSpell](https://cspell.org/) configuration [file](/cspell.config.yaml). So now we can keep dictionaries in the repository

To make use of then there are VSCode plugins:

- [Code Spell Checker](https://marketplace.visualstudio.com/items?itemName=streetsidesoftware.code-spell-checker)
- [Russian - Code Spell Checker](https://marketplace.visualstudio.com/items?itemName=streetsidesoftware.code-spell-checker-russian)

To run from console the [cspell-cli](https://github.com/streetsidesoftware/cspell-cli) tool available
