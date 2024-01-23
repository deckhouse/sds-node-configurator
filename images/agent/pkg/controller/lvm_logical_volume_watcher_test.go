package controller

import (
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestLVMLogicaVolumeWatcher(t *testing.T) {
	t.Run("subtractQuantity_returns_correct_value", func(t *testing.T) {
		min := resource.NewQuantity(1000, resource.BinarySI)
		sub := resource.NewQuantity(300, resource.BinarySI)
		expected := resource.NewQuantity(700, resource.BinarySI)

		actual := subtractQuantity(*min, *sub)
		assert.Equal(t, expected, &actual)
	})
}
