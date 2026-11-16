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
	completionsAfter := ""
	acceptedAfter := ""
	credentialsAfter := ""
	preparationAfter := ""
	drafts := make([]string, len(collector.draftOwners))
	for {
		if collector.backingDrafts != nil {
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := collector.backingDrafts.CleanupBackingServiceCreationStages(bounded, time.Now().UTC(), 16)
			cancel()
			if err != nil {
				report("", err)
			}
		}
		if collector.preparations != nil {
			page, err := collector.queue.AcceptedPreparationCleanup(ctx, preparationAfter)
			if err != nil {
				report("", err)
			} else {
				for _, candidate := range page.Tasks {
					bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
					_, err := collector.preparations.CollectAcceptedPreparation(bounded, candidate.Task.ID, time.Now().UTC())
					cancel()
					if err != nil {
						report(candidate.Task.ID, err)
					}
				}
				preparationAfter = page.Next
			}
		}
		credentialContext, credentialCancel := context.WithTimeout(ctx, 10*time.Second)
		nextCredentials, credentialErr := collector.queue.CollectCredentialReferences(
			credentialContext,
			credentialsAfter,
			time.Now().UTC(),
		)
		credentialCancel()
		credentialsAfter = nextCredentials
		if credentialErr != nil {
			report("", credentialErr)
		}
		acceptedContext, acceptedCancel := context.WithTimeout(ctx, 10*time.Second)
		nextAccepted, acceptedErr := collector.queue.CollectAcceptedInputDrafts(
			acceptedContext,
			acceptedAfter,
			time.Now().UTC(),
		)
		acceptedCancel()
		if acceptedErr != nil {
			report("", acceptedErr)
		} else {
			acceptedAfter = nextAccepted
		}
		bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
		nextCompletions, err := collector.queue.CollectTaskCompletionReceipts(bounded, completionsAfter)
		cancel()
		if err != nil {
			report("", err)
		} else {
			completionsAfter = nextCompletions
		}
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
