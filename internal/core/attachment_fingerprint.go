package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// AttachmentSpecFingerprint identifies authored intent consistently for direct
// Attach publication and Blueprint reconciliation. It contains no generated
// credentials, runtime observations or execution identity.
func AttachmentSpecFingerprint(spec AttachmentSpec) (string, error) {
	encoded, err := json.Marshal(struct {
		Schema uint8
		Domain string
		Input  AttachmentSpec
	}{1, "attach-spec", spec})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return hex.EncodeToString(digest[:]), nil
}
