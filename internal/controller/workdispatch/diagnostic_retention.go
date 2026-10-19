package workdispatch

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Diagnostic expiry has its own cursor and time budget. Task collection remains
// with operation owners and can retain unresolved recovery long after this tail
// has expired. Expiry cannot consume an execution slot or authorize effects.
func (runtime *Runtime) expireDiagnostics(ctx context.Context, config RuntimeConfig, report func(Issue)) error {
	ticker := time.NewTicker(config.CollectionInterval)
	defer ticker.Stop()
	after := ""
	for {
		bounded, cancel := context.WithTimeout(ctx, config.CollectionBudget)
		at := time.Now().UTC()
		// The history horizon enumerates retained terminal Tasks. ExpireTask
		// independently checks actual FinishedAt plus diagnostic retention.
		page, err := runtime.queue.Expired(bounded, after, at.Add(config.HistoryRetention))
		if err != nil && bounded.Err() == nil {
			report(Issue{Err: err})
		}
		if err == nil {
			finished := true
			for _, candidate := range page.Candidates {
				if bounded.Err() != nil {
					finished = false
					break
				}
				_, err := runtime.diagnostics.ExpireTask(
					bounded,
					candidate.Task.Task.ID,
					at,
					config.DiagnosticRetention,
				)
				if err != nil && bounded.Err() == nil && !errors.Is(err, errs.New(errs.KindTaskNotFound, "")) &&
					!errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					report(Issue{TaskID: candidate.Task.Task.ID, Err: err})
				}
				after = candidate.Key
			}
			if finished {
				after = page.Next
			}
		}
		cancel()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
