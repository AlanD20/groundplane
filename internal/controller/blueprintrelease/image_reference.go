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

func bytesToHex(value []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for index, item := range value {
		result[index*2], result[index*2+1] = digits[item>>4], digits[item&15]
	}
	return string(result)
}
