// Package backup owns pure Backup capability constants and identities.
package backup

import (
	"crypto/sha256"
	"encoding/hex"
)

const KeyRotationTimeoutSeconds int64 = 120

// KeyRotationPlanHash is stable and contains no key material.
func KeyRotationPlanHash() string {
	digest := sha256.Sum256([]byte("groundplane/backup-key-rotation/v1"))
	return hex.EncodeToString(digest[:])
}
