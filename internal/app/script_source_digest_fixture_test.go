package app

import (
	sha256 "crypto/sha256"
	hex "encoding/hex"
)

func scriptSourceReferenceDigest(value string) string {
	return scriptSourceReferenceBytesDigest([]byte(value))
}
func scriptSourceReferenceBytesDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
