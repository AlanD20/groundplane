package operationretention

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Run collects bounded pages independently of execution capacity. Failures are
// reported per Task and the scan advances, so one retained or damaged record
// does not prevent collection of unrelated expired checks.
func (collector *Collector) Run(ctx context.Context, interval time.Duration, report func(string, error)) error {
	if ctx == nil || interval < time.Second || report == nil {
		return errs.New(errs.KindInternal, "operation retention loop configuration is invalid")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	after := ""
	drafts := make([]string, len(collector.draftOwners))
	for {
		for index, owner := range collector.draftOwners {
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			next, err := owner.CollectDrafts(bounded, drafts[index], time.Now().UTC())
			cancel()
			if err != nil {
				report("", err)
			} else {
				drafts[index] = next
			}
		}
		page, err := collector.queue.Expired(ctx, after, time.Now().UTC())
		if err != nil {
			return err
		}
		for _, candidate := range page.Candidates {
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := collector.Collect(bounded, candidate.Task.Task.ID, time.Now().UTC())
			cancel()
			if err != nil {
				report(candidate.Task.Task.ID, err)
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
