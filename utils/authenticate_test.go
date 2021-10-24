package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthenticate_CredentialChainFromToken(t *testing.T) {
	t.Run("credential chain", func(t *testing.T) {
		token := []byte("anA6YUdGNVlXSjFjMkV0WTJ4dmRXUTZaRWRXZW1SRE1" +
			"XaGpTRUUyVmxWa1YwMXJNVmxSYld4cVpXdHdUVll3WkdGaWF6bHpWbT" +
			"FHVTAxcVZsVlhiR2gzVmxkSmVGWnRkRTVUUjNONFZHNXdWMVJGTlhSY" +
			"WVrcFFVbFphZFZaVVJrZGhiRVpVWkVWT2JGWlVSblZXYWtaRFpXeGFW" +
			"VmRzVms1U2F6UjVXa1pXZDA1WFVuUlNiRXBTVlZRd09RPT0=")
		chain := CredentialChainFromToken(token)
		keys := chain.StringAccessKeys()
		assert.Len(t, keys, 4)
		assert.Equal(t, "jp", keys[0])
		assert.Equal(t, "hayabusa-cloud", keys[1])
		assert.Equal(t, "test-app", keys[2])
	})
}
