module github.com/deckhouse/sds-node-configurator/lib/go/common

go 1.23.4

// Do not combine multiple replacements into a single block,
// as this will break the CI workflow "Check Go module version."
replace github.com/deckhouse/sds-node-configurator/api => ../../../api
