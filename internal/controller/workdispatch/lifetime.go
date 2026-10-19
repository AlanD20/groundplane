package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/controllerownership"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Start owns one complete local Controller execution lifetime. No registration,
// reconciliation or running worker outlives the process-ownership lease. It is
// called only after the coordinated storage/protocol cutover has been admitted.
func (runtime *Runtime) Start(ctx context.Context, capacity uint16, config RuntimeConfig) (result error) {
	if ctx == nil || capacity == 0 || capacity > 256 || config.Interval < 100*time.Millisecond ||
		config.CollectionInterval < time.Second || config.HistoryRetention != workexecution.TaskHistoryRetention ||
		config.DiagnosticRetention < time.Hour || config.DiagnosticRetention > config.HistoryRetention || config.CollectionBudget <= 0 {
		return errs.New(errs.KindInternal, "execution lifetime configuration is invalid")
	}
	ownership, err := controllerownership.Acquire(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := ownership.Close(); err != nil {
			result = errs.WrapJoined(errs.KindInternal, result, err)
		}
	}()
	identity, err := runtime.RegisterController(ctx, ownership, capacity)
	if err != nil {
		return err
	}
	issues, err := runtime.ReconcileStartup(ctx, ownership, identity)
	for _, issue := range issues {
		runtime.logger.ErrorContext(
			ctx,
			"interrupted execution retains unresolved effects",
			"task_id",
			issue.TaskID,
			"work_id",
			issue.WorkID,
			"error",
			issue.Err,
		)
	}
	if err != nil {
		return err
	}
	return runtime.Run(ctx, ownership, identity, config)
}
