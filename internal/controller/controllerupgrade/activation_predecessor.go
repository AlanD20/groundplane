package controllerupgrade

import (
	"context"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ActivationPredecessor captures the installed executable before staging.
// Native execution verifies this same predecessor again before cutover.
func (service *Service) ActivationPredecessor(ctx context.Context) (upgrade.Digest, error) {
	if service.catalog == nil {
		return "", bootstrapUnavailable()
	}
	if err := service.unit.VerifyBootstrap(ctx); err != nil {
		return "", err
	}
	installed, err := service.catalog.Installed(ctx)
	if err != nil {
		return "", err
	}
	if installed != service.process {
		return "", errs.New(
			errs.KindStateConflict,
			"running Controller differs from the installed activation predecessor",
		)
	}
	return installed, nil
}
