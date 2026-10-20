package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadBlueprintRuntimeProjection observes preparation at the caller's fixed
// snapshot. Missing output is not execution authority or a previous projection.
func (repository *EnvironmentBlueprintRepository) ReadBlueprintRuntimeProjection(
	ctx context.Context, environmentID, taskID string, revision int64,
) (keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection], bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{}, false, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindTask, taskID) != nil ||
		revision <= 0 {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{}, false,
			errs.New(errs.KindValidationFailed, "Blueprint runtime preparation read is invalid")
	}
	return blueprints.ReadAvailableRuntimeRevision(ctx, repository.store, environmentID, taskID, revision)
}
