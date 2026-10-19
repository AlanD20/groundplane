package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
)

func (service *Service) Events(
	ctx context.Context,
	taskID, workID, attemptID string,
	after uint64,
	revision int64,
) (executiondiagnostics.Events, error) {
	return service.diagnostics.Events(ctx, taskID, workID, attemptID, after, revision)
}
