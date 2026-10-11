package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A shared Zone needs an independently approved impact token before removal;
// the Blueprint's own Environment revision does not cover external consumers.
func (service *Service) requireBackingZoneOmissionApproval(
	ctx context.Context, projectID, environmentID string,
	parsed blueprintparser.Result, previous []composeidentity.Resource,
) error {
	checker, ok := service.repository.(interface {
		BlueprintBackingZoneHasExternalAttaches(context.Context, string, string, string) (bool, error)
	})
	if !ok {
		return errs.New(errs.KindInternal, "Blueprint Backing Zone impact reader is unavailable")
	}
	for _, prior := range previous {
		if _, present := parsed.Project.Networks[prior.Name]; present {
			continue
		}
		external, err := checker.BlueprintBackingZoneHasExternalAttaches(ctx, projectID, environmentID, prior.ID)
		if err != nil {
			return err
		}
		if external {
			return errs.New(errs.KindResourceInUse,
				"Blueprint omits a Backing Zone with external Attaches; remove it with impact approval first")
		}
	}
	return nil
}
