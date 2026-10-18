package executiondiagnostics

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReportResult is for operation owners interpreting an already committed result.
// It cannot alter execution or accept a late executor report as new authority.
func (repository *Repository) ReportResult(ctx context.Context, attempt workexecution.Attempt,
	sequence uint64, stage workexecution.Stage,
) error {
	if attempt.Validate() != nil || attempt.FinishedAt == nil || attempt.Result == nil {
		return invalid()
	}
	var err error
	for retry := 0; retry < 3; retry++ {
		err = repository.report(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor,
			sequence, stage, *attempt.FinishedAt, attempt.Result)
		if err == nil || ctx.Err() != nil || !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return err
		}
	}
	return err
}
