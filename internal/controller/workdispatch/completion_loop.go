package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Run is independent of worker capacity. A lost completion
// response leaves the indexed Task available for the next sweep; it does not
// repeat the check or create a replacement Task. Per-Task failures do not block
// completion of other checks, and each sweep has bounded storage and time use.
func (completions *Completions) Run(ctx context.Context,
	interval, retention time.Duration, report func(Issue),
) error {
	if ctx == nil || interval < 100*time.Millisecond || retention <= 0 || report == nil {
		return errs.New(errs.KindInternal, "completion loop configuration is invalid")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	after := ""
	for {
		page, err := completions.queue.Finishing(ctx, after)
		if err != nil {
			return err
		}
		for _, task := range page.Tasks {
			bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
			err := completions.Finish(bounded, task.Task.ID, time.Now().UTC(), retention)
			cancel()
			if err != nil {
				report(Issue{TaskID: task.Task.ID, Err: err})
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		after = page.Next
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
