package imagedelivery

import (
	"strings"

	"github.com/distribution/reference"
)

// Docker may report nginx@sha256:... where the sealed input names
// docker.io/library/nginx@sha256:.... Local image IDs are not repository names.
func canonicalImageReference(value string) string {
	if strings.Contains(value, "@") {
		if named, err := reference.ParseNormalizedNamed(value); err == nil {
			return named.String()
		}
	}
	return value
}

func (retention imageRetention) retain(value, reason string) {
	if value != "" {
		retention.references[canonicalImageReference(value)] = reason
	}
}
