package attachments

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BindingDigest identifies the complete acknowledged connection without
// retaining Task history or copying any secret values into its resource receipt.
func BindingDigest(record Record) (string, error) {
	if record.Status != core.AttachReady || record.Operation != AttachOperationProvision {
		return "", errs.New(errs.KindStateConflict, "Attach binding digest requires a ready connection")
	}
	raw, err := EncodeAttachRecord(record)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
