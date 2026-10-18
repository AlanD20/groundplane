package apiclient

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Capture the exact failure selected by this CLI invocation. All pages share
// one read revision; the POST rejects a timeout/retry that changed it meanwhile.
func (c *Client) captureQueuedTaskRetry(ctx context.Context, taskID, workID string) (time.Time, error) {
	options := TaskDetailOptions{Limit: 100}
	for pages := 0; pages < 41; pages++ {
		page, err := c.ShowTask(ctx, taskID, options)
		if err != nil {
			return time.Time{}, err
		}
		if page.Task.ID != taskID || page.Work.Revision <= 0 || options.Revision != 0 && options.Revision != page.Work.Revision {
			return time.Time{}, errs.New(errs.KindInternal, "Task work returned a different snapshot")
		}
		for _, work := range page.Work.Items {
			if work.ID != workID {
				continue
			}
			if work.AttemptCount != 0 || work.Phase != "settled" || work.Outcome != "failed" || !work.Retry.Allowed || work.UpdatedAt.IsZero() {
				return time.Time{}, errs.New(errs.KindTaskNotRetryable, "selected work no longer has a retryable queued failure")
			}
			return work.UpdatedAt, nil
		}
		if !page.Work.More {
			return time.Time{}, errs.New(errs.KindStateConflict, "selected work was not found in the Task")
		}
		if page.Work.After == "" || page.Work.After <= options.After || page.Work.Revision <= 0 ||
			options.Revision != 0 && options.Revision != page.Work.Revision {
			return time.Time{}, errs.New(errs.KindInternal, "Task work pagination returned an invalid snapshot")
		}
		options.After, options.Revision = page.Work.After, page.Work.Revision
	}
	return time.Time{}, errs.New(errs.KindStateConflict, "Task exceeds the captured work scan limit")
}
