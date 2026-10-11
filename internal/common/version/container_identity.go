package version

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

// ContainerIdentity binds safe image labels to the immutable image configuration
// of an accepted container. Empty version/source fields mean not reported.
type ContainerIdentity struct {
	Image        string          `json:"image"`
	ConfigDigest string          `json:"config_digest"`
	SelectedTag  string          `json:"selected_tag,omitempty"`
	Version      string          `json:"version,omitempty"`
	Source       BuildProvenance `json:"source,omitempty,omitzero"`
}

func (identity ContainerIdentity) Validate() error {
	if !imageref.IsDigestPinned(identity.Image) || !validConfigDigest(identity.ConfigDigest) {
		return errs.New(errs.KindValidationFailed, "installed container image identity is invalid")
	}
	if identity.SelectedTag != "" {
		name, err := reference.ParseNormalizedNamed(identity.Image)
		if err != nil {
			return errs.New(errs.KindValidationFailed, "installed container image repository is invalid")
		}
		if _, err := reference.WithTag(reference.TrimNamed(name), identity.SelectedTag); err != nil {
			return errs.New(errs.KindValidationFailed, "installed container selected tag is invalid")
		}
	}
	if identity.Version == "" {
		if identity.Source != (BuildProvenance{}) {
			return errs.New(errs.KindValidationFailed, "installed container source requires its build version")
		}
		return nil
	}
	return (BuildIdentity{Version: identity.Version, Source: identity.Source}).Validate()
}

func validConfigDigest(digest string) bool {
	value, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'f' || character >= '0' && character <= '9') {
			return false
		}
	}
	return true
}
