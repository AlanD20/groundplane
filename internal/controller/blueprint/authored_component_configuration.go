package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentConfigurationPreparer interface {
	PrepareBlueprintComponentConfiguration(context.Context, etcd.TaskRecord, int64) (etcd.TaskRecord, error)
}

// Capture before sealing the child recipe. Publication compares this exact
// acknowledged predecessor; staging never makes the candidate files applied.
func (service *Service) prepareAuthoredComponentConfiguration(
	ctx context.Context,
	child etcd.TaskRecord,
	revision int64,
) (etcd.TaskRecord, error) {
	if len(child.Materializations) == 0 {
		return child, nil
	}
	preparer, ok := service.repository.(componentConfigurationPreparer)
	if !ok {
		return etcd.TaskRecord{}, errs.New(
			errs.KindInternal,
			"Blueprint Component configuration preparer is not configured",
		)
	}
	return preparer.PrepareBlueprintComponentConfiguration(ctx, child, revision)
}
