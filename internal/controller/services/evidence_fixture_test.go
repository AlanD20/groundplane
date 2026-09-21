package services

import (
	"crypto/sha256"
	"encoding/hex"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func serviceTestProtectedIntent() idempotencyrecord.ProtectedIntentRecord {
	ciphertext := []byte("protected-service-mutation-intent")
	digest := sha256.Sum256(ciphertext)
	return idempotencyrecord.ProtectedIntentRecord{
		EnvelopeVersion:  1,
		Cipher:           "age-x25519",
		DigestAlgorithm:  "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]),
		Ciphertext:       ciphertext,
	}
}
