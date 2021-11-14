package kms_tls_test

import (
	"log"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logger *log.Logger

// External KMS integration - TLS Vault - SA authentication test entry point
func TestTlsKMS(t *testing.T) {
	// this variable is defined in .github/workflows/run_hac_test.yml
	// indication of running in integration test environment
	_, ok := os.LookupEnv("OPERATOR_IMAGE")
	if !ok {
		t.Skip() // Not an integration test, skip
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "External KMS (TLS Vault) Suite")
}

var _ = BeforeSuite(func() {
	logger = log.New(GinkgoWriter, "INFO: ", log.Lshortfile)
}, 60)
