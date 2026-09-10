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

// Package llvs holds the LVMLogicalVolumeSnapshot reconciler. Snapshots are an
// EE feature, so reconciler_ee.go is built under `!ce` and nothing outside
// cmd/llvs_ee.go imports this package.
//
// This file deliberately carries no build tag, so the package exists in every
// edition even though it is empty in CE. The coverage job enumerates packages
// once with `go list` and no tags, then runs `go test $pkgs -cover -tags <edition>`
// for each edition in turn; a package whose every file is excluded by the tag
// does not report "no test files" but fails the run outright with
//
//	build constraints exclude all Go files in .../internal/controller/llvs
//	FAIL .../internal/controller/llvs [setup failed]
package llvs
