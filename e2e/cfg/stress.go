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

package cfg

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"k8s.io/apimachinery/pkg/api/resource"
)

// maxVMBlockDevicesCeiling is the physical cap for VMBDA per VM in Deckhouse
// virtualization (≤16), leaving one slot reserved for the boot device.
const maxVMBlockDevicesCeiling = 16

// Stress holds the domain-specific knobs for the stress spec. It is loaded
// lazily in the stress spec's BeforeAll (NOT in cfg.Load), so a broken
// E2E_STRESS_* env never breaks non-stress runs. In CI these vars are usually
// not passed and the stress spec runs on defaults — see spec CT7.
type Stress struct {
	Target            int    `env:"E2E_STRESS_MAX_VG_TARGET" envDefault:"15"`
	DiskSize          string `env:"E2E_STRESS_MAX_VG_DISK_SIZE" envDefault:"1Gi"`
	BatchSize         int    `env:"E2E_STRESS_MAX_VG_BATCH_SIZE" envDefault:"5"`
	Strict            bool   `env:"E2E_STRESS_MAX_VG_STRICT" envDefault:"false"`
	MinReady          int    `env:"E2E_STRESS_MAX_VG_MIN_READY" envDefault:"0"`
	MaxVMBlockDevices int    `env:"E2E_STRESS_MAX_VM_BLOCK_DEVICES" envDefault:"15"`
}

// LoadStress parses the E2E_STRESS_* env into a fresh *Stress each call
// (stateless — no package global), validates fail-fast, and normalizes.
func LoadStress() (*Stress, error) {
	var s Stress
	if err := env.Parse(&s); err != nil {
		return nil, err
	}

	if s.Target <= 0 || s.BatchSize <= 0 || s.MaxVMBlockDevices <= 0 {
		return nil, fmt.Errorf("stress config: Target, BatchSize and MaxVMBlockDevices must be >0 (≤0 invalid): target=%d batch=%d maxVM=%d", s.Target, s.BatchSize, s.MaxVMBlockDevices)
	}

	if _, err := resource.ParseQuantity(s.DiskSize); err != nil {
		return nil, fmt.Errorf("stress config: bad DiskSize %q: %w", s.DiskSize, err)
	}

	if s.BatchSize > s.Target {
		return nil, fmt.Errorf("stress config: BatchSize (%d) exceeds Target (%d)", s.BatchSize, s.Target)
	}

	if s.MaxVMBlockDevices > maxVMBlockDevicesCeiling {
		s.MaxVMBlockDevices = maxVMBlockDevicesCeiling
	}
	if s.Target > s.MaxVMBlockDevices {
		s.Target = s.MaxVMBlockDevices
	}
	// Re-clamp BatchSize to the (possibly reduced) Target without erroring; the
	// pre-clamp check already validated BatchSize <= original Target.
	if s.BatchSize > s.Target {
		s.BatchSize = s.Target
	}

	if s.MinReady <= 0 {
		if s.Strict {
			s.MinReady = s.Target
		} else {
			s.MinReady = 1
		}
	}

	return &s, nil
}
