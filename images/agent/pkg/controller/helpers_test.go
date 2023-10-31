package controller

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHelpers(t *testing.T) {
	t.Run("CheckResourceDeprecated_returns_true", func(t *testing.T) {
		resource := "deprecated"
		actual := map[string]struct{}{
			"actual": {},
		}

		deprecated := CheckResourceDeprecated(resource, actual)

		assert.True(t, deprecated)
	})

	t.Run("CheckResourceDeprecated_returns_false", func(t *testing.T) {
		resource := "actual"
		actual := map[string]struct{}{
			"actual": {},
		}

		deprecated := CheckResourceDeprecated(resource, actual)

		assert.False(t, deprecated)
	})
}
