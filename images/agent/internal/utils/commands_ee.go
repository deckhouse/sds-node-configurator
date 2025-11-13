//go:build !ce

/*
Copyright 2025 Flant JSC
Licensed under the Deckhouse Platform Enterprise Edition (EE) license. See https://github.com/deckhouse/deckhouse/blob/main/ee/LICENSE
*/

package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/deckhouse/sds-node-configurator/images/agent/internal"
	"github.com/deckhouse/sds-node-configurator/images/agent/internal/logger"
)

func ThinDumpRaw(ctx context.Context, log logger.Logger, tpool, tmeta, devID string) (out []byte, err error) {
	log = log.WithName("ThinDumpRaw").WithValues("tpool", tpool, "tmeta", tmeta, "devID", devID)
	log.Trace("calling for tpool tmeta devID")
	cmd := exec.CommandContext(
		ctx,
		internal.NSENTERCmd,
		nsentrerExpendedArgs(internal.DMSetupCmd, "message", tpool, "0", "reserve_metadata_snap")...)
	log.Debug("running command", "command", cmd.String())
	if err = cmd.Run(); err != nil {
		log.Error(err, "can't reserve metadata snapshot")
		err = fmt.Errorf("reserving metadata snapshot: %w", err)
		return
	}
	defer func() {
		cmd := exec.CommandContext(
			ctx,
			internal.NSENTERCmd,
			nsentrerExpendedArgs(internal.DMSetupCmd, "message", tpool, "0", "release_metadata_snap")...)

		log.Debug("running command", "command", cmd)
		if errRelease := cmd.Run(); errRelease != nil {
			log.Error(errRelease, "can't release metadata snapshot")
			err = errors.Join(err, errRelease)
		}
	}()

	args := []string{tmeta, "-m", "-f", "xml"}
	if devID != "" {
		args = append(args, "--dev-id", devID)
	}
	cmd = exec.CommandContext(ctx, internal.ThinDumpCmd, args...)

	var output bytes.Buffer
	cmd.Stdout = &output

	log.Debug("running command", "command", cmd)
	if err = cmd.Run(); err != nil {
		log.Error(err, "can't get metadata")
		err = fmt.Errorf("dumping metadata: %w", err)
		return
	}
	log.Trace("device map", "output", output)
	return output.Bytes(), nil
}
