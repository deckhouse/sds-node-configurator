//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package utils

import (
	"agent/internal/logger"
	"context"
	"fmt"

	commonfeature "github.com/deckhouse/sds-node-configurator/lib/go/common/pkg/feature"
)

func VolumeCleanup(ctx context.Context, log logger.Logger, vgName, lvName, volumeCleanupMethod string) error {
	if !commonfeature.VolumeCleanupEnabled {
		return fmt.Errorf("Volume cleanup is not supported in your edition.")
	}

	return nil
}
