package recordcodec

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"time"
	"unicode/utf8"
)

const MaximumValueBytes = 256 << 10

func ValidateID(kind ids.Kind, value string) error {
	if err := ids.Validate(kind, value); err != nil {
		return errs.New(errs.KindValidationFailed, err.Error())
	}
	return nil
}

func ValidSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func ValidateTimestamp(field string, value time.Time) error {
	if value.IsZero() || value.Location() != time.UTC {
		return errs.Newf(errs.KindValidationFailed, "%s must be a non-zero UTC timestamp", field)
	}
	return nil
}

func ValidateLabel(field string, value string) error {
	if value == "" || !utf8.ValidString(value) {
		return errs.Newf(errs.KindValidationFailed, "%s is required and must be valid UTF-8", field)
	}
	return nil
}
