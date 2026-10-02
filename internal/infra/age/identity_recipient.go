package age

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	age "filippo.io/age"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// IdentityRecipient binds a task-scoped identity to the selected encryption
// recipient without exposing the private identity through errors.
func IdentityRecipient(identity []byte, expectedSHA256 []byte) (string, error) {
	if len(identity) == 0 || len(identity) > maxIdentitySize || len(expectedSHA256) != sha256.Size {
		return "", errs.New(errs.KindValidationFailed, "age: encryption identity authority is invalid")
	}
	parsed, err := age.ParseX25519Identity(strings.TrimSpace(string(identity)))
	if err != nil {
		return "", errs.New(errs.KindValidationFailed, "age: encryption identity is invalid")
	}
	recipient := parsed.Recipient().String()
	digest := sha256.Sum256([]byte(recipient))
	if subtle.ConstantTimeCompare(digest[:], expectedSHA256) != 1 {
		return "", errs.New(errs.KindStateConflict, "age: encryption identity differs from sealed recipient")
	}
	return recipient, nil
}
