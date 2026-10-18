package serviceruntimerecord

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Fingerprint identifies the whole acknowledged record, including its source
// and configuration. Restoring byte-identical authority can change its storage
// revision without selecting a new runtime. A matching image alone is not enough.
func Fingerprint(record Record) (string, error) {
	if err := Validate(record); err != nil {
		return "", err
	}
	value, err := json.Marshal(record)
	if err != nil {
		return "", errs.New(errs.KindInternal, "acknowledged Service runtime cannot be encoded")
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}
