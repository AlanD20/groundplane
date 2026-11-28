package buildcredentials

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const passwordPrefix = "gpbr1_"

func mintPassword() (string, [sha256.Size]byte, error) {
	var seed [32]byte
	defer clear(seed[:])
	if _, err := rand.Read(seed[:]); err != nil {
		return "", [sha256.Size]byte{}, errs.Wrap(errs.KindInternal, err)
	}
	return passwordPrefix + base64.RawURLEncoding.EncodeToString(seed[:]), passwordDigest(seed), nil
}

func parsePassword(password string) ([sha256.Size]byte, bool) {
	encoded, ok := strings.CutPrefix(password, passwordPrefix)
	if !ok || len(encoded) != base64.RawURLEncoding.EncodedLen(32) {
		return [sha256.Size]byte{}, false
	}
	var seed [32]byte
	defer clear(seed[:])
	size, err := base64.RawURLEncoding.Strict().Decode(seed[:], []byte(encoded))
	if err != nil || size != len(seed) {
		return [sha256.Size]byte{}, false
	}
	return passwordDigest(seed), true
}

// The registry digest has a different domain from the Runner API bearer.
func passwordDigest(seed [32]byte) [sha256.Size]byte {
	const purpose = "groundplane-registry-job-v1:"
	var input [len(purpose) + len(seed)]byte
	defer clear(input[:])
	copy(input[:], purpose)
	copy(input[len(purpose):], seed[:])
	return sha256.Sum256(input[:])
}
