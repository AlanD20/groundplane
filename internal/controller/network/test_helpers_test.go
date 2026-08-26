package network

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func networkTestProtectedIntent() etcd.ProtectedIntentRecord {
	ciphertext := []byte("protected-network-intent")
	digest := sha256.Sum256(ciphertext)
	return etcd.ProtectedIntentRecord{
		EnvelopeVersion:  1,
		Cipher:           "age-x25519",
		DigestAlgorithm:  "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]),
		Ciphertext:       ciphertext,
	}
}
