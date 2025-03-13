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
	log.Trace(fmt.Sprintf("[ThinDumpRaw] calling for tpool %s tmeta %s devID %s", tpool, tmeta, devID))
	cmd := exec.CommandContext(
		ctx,
		internal.NSENTERCmd,
		nsentrerExpendedArgs(internal.DMSetupCmd, "message", tpool, "0", "reserve_metadata_snap")...)
	log.Debug(fmt.Sprintf("[ThinDumpRaw] running %v", cmd))
	if err = cmd.Run(); err != nil {
		log.Error(err, fmt.Sprintf("[ThinDumpRaw] can't reserve metadata snapshot for %s", tpool))
		err = fmt.Errorf("reserving metadata snapshot: %w", err)
		return
	}
	defer func() {
		cmd := exec.CommandContext(
			ctx,
			internal.NSENTERCmd,
			nsentrerExpendedArgs(internal.DMSetupCmd, "message", tpool, "0", "release_metadata_snap")...)

		log.Debug(fmt.Sprintf("[ThinDumpRaw] running %v", cmd))
		if errRelease := cmd.Run(); errRelease != nil {
			log.Error(errRelease, fmt.Sprintf("[ThinDumpRaw] can't release metadata snapshot for %s", tpool))
			err = errors.Join(err, errRelease)
		}
	}()

	args := []string{tmeta, "-m", "-f", "xml"}
	if devID != "" {
		args = append(args, "--dev-id", devID)
	}
	cmd = exec.CommandContext(ctx,
		internal.NSENTERCmd,
		nsentrerExpendedArgs(internal.ThinDumpCmd, args...)...)

	var output bytes.Buffer
	cmd.Stdout = &output

	log.Debug(fmt.Sprintf("[ThinDumpRaw] running %v", cmd))
	if err = cmd.Run(); err != nil {
		log.Error(err, fmt.Sprintf("[ThinDumpRaw] can't get metadata %s", tmeta))
		err = fmt.Errorf("dumping metadata: %w", err)
		return
	}
	log.Trace(fmt.Sprintf("[ThinDumpRaw] device map is: %s", output.Bytes()))
	return output.Bytes(), nil
}
