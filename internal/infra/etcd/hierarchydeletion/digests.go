package hierarchydeletion

import (
	"crypto/sha256"
	"encoding/hex"
)

func HierarchyDeletionBytesDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func HierarchyDeletionFoldDigest(domain string, values ...string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(domain))
	for _, value := range values {
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write([]byte(value))
	}
	return hex.EncodeToString(digest.Sum(nil))
}
