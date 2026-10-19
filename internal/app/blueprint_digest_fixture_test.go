package app

import (
	"encoding/hex"
	"testing"
)

func decodeTestDigest(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("invalid fixture digest: %v", err)
	}
	return decoded
}
