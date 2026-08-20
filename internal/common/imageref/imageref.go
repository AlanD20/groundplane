// Package imageref validates immutable OCI image identities shared by the
// Controller lifecycle and its infrastructure adapters.
package imageref

import (
	"strings"

	"github.com/distribution/reference"
	digest "github.com/opencontainers/go-digest"
)

// IsDigestPinned reports whether value is exactly a named OCI reference pinned
// by a canonical SHA-256 digest. Tags, including tag-plus-digest references, are
// not accepted as runtime identity.
func IsDigestPinned(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return false
	}
	if _, tagged := named.(reference.NamedTagged); tagged {
		return false
	}
	digested, ok := named.(reference.Digested)
	if !ok {
		return false
	}
	parsed, err := digest.Parse(digested.Digest().String())
	return err == nil && parsed.Algorithm() == digest.SHA256 && parsed.Validate() == nil
}
