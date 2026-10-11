package blueprintrelease

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

func releaseImage(value string) (string, string, string, error) {
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", "", "", errs.New(errs.KindValidationFailed, "Blueprint candidate image reference is invalid")
	}
	tag := "blueprint"
	if tagged, ok := named.(reference.NamedTagged); ok {
		tag = tagged.Tag()
	}
	digest := ""
	if digested, ok := named.(reference.Digested); ok {
		digest = digested.Digest().Encoded()
	}
	return named.String(), tag, digest, nil
}
