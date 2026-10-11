package component

import (
	"context"

	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *ReadService) installedSoftware(
	ctx context.Context,
	id string,
	revision int64,
) (*buildversion.ContainerIdentity, error) {
	accepted, err := service.repository.GetComponentInstalledSoftware(ctx, id, revision)
	if err != nil || accepted == nil {
		return accepted, err
	}
	observed, err := service.softwareInspector.InspectSoftware(ctx, *accepted)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		// Loss of optional local metadata does not erase durable installation proof.
		return accepted, nil
	}
	if observed.Image != accepted.Image || observed.ConfigDigest != accepted.ConfigDigest ||
		observed.SelectedTag != accepted.SelectedTag || observed.Validate() != nil {
		return nil, errs.New(errs.KindInternal, "Component software metadata changed its accepted image identity")
	}
	return &observed, nil
}
