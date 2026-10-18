package imagedelivery

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) checkRemoval(ctx context.Context, imageID, operationID string) error {
	images, err := service.registry.List(ctx)
	if err != nil {
		return err
	}
	for _, image := range images {
		if image.ID != imageID {
			continue
		}
		retention, err := service.retainedImages(ctx, operationID)
		if err != nil {
			return err
		}
		if reason := removalReason(image, retention); reason != "" {
			return errs.New(errs.KindResourceInUse, reason)
		}
		return nil
	}
	// Absence is already the requested outcome. An identical content-addressed
	// ID cannot select some later, different image on retry.
	return nil
}
