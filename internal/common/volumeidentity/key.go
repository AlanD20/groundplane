package volumeidentity

import (
	"github.com/AlanD20/groundplane/internal/common/composekey"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateKey(key string) error {
	if len(key) > 255 {
		return errs.New(errs.KindValidationFailed, "volume key must not exceed 255 bytes")
	}
	if err := composekey.Validate(key); err != nil {
		return err
	}
	if key == "." || key == ".." {
		return errs.New(errs.KindValidationFailed, "volume key must name a direct environment child")
	}
	return nil
}
