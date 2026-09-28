package imagefetch

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func RemovalHash(imageID string) (string, error) {
	if !workloadimage.LocalIDValid(imageID) {
		return "", errs.New(errs.KindValidationFailed, "image removal requires the full sha256 local image ID")
	}
	digest := sha256.Sum256([]byte("remove-host-image\n" + imageID))
	return hex.EncodeToString(digest[:]), nil
}
