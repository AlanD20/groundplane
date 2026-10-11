package version

import (
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BuildIdentity is the version and source reported by one accepted binary. It
// never describes a desired image or a prepared replacement.
type BuildIdentity struct {
	Version string          `json:"version"`
	Source  BuildProvenance `json:"source"`
}

func (build BuildIdentity) Validate() error {
	if build.Version == "" || len(build.Version) > 128 || !utf8.ValidString(build.Version) {
		return errs.New(errs.KindValidationFailed, "build version is invalid")
	}
	for _, character := range build.Version {
		if unicode.IsControl(character) {
			return errs.New(errs.KindValidationFailed, "build version contains control characters")
		}
	}
	return build.Source.Validate()
}
