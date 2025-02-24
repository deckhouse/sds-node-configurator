package utils_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestVolumeCleanup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "utils Suite")
}
