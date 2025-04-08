package main

import (
	"github.com/deckhouse/module-sdk/pkg/app"
	_ "github.com/deckhouse/sds-node-configurator/hooks/go/020-webhook-certs"
)

func main() {
	app.Run()
}
