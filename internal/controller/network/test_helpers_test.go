package network

import (
	"crypto/sha256"
	"encoding/hex"

	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func networkTestProtectedIntent() testidempotency.ProtectedIntentRecord {
	ciphertext := []byte("protected-network-intent")
	digest := sha256.Sum256(ciphertext)
	return testidempotency.ProtectedIntentRecord{
		EnvelopeVersion:  1,
		Cipher:           "age-x25519",
		DigestAlgorithm:  "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]),
		Ciphertext:       ciphertext,
	}
}
