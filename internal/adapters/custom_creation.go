package adapters

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CustomBackingCreationSpec resolves the two operator-owned workload fields
// for a network-only custom backing service. It deliberately contributes no
// volume, environment, command, exposure, or health defaults.
func CustomBackingCreationSpec(serviceName string, image string) (CreationSpec, error) {
	if !core.ValidEnvironmentComposeName(serviceName) {
		return CreationSpec{}, errs.New(
			errs.KindValidationFailed,
			"custom backing-service slug cannot be used as its Service name",
		)
	}
	if strings.TrimSpace(image) == "" || image != strings.TrimSpace(image) {
		return CreationSpec{}, errs.New(
			errs.KindValidationFailed,
			"custom backing-service image is required and must not contain surrounding whitespace",
		)
	}
	return CreationSpec{ServiceName: serviceName, Image: image}, nil
}
