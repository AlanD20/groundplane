package taskinspection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type TaskPage struct {
	Tasks       []Summary
	More        bool
	After       string
	Revision    int64
	ScanLimited bool
}

type Filter struct {
	ResourceKind string
	Action       workexecution.Action
	Status       string
}

func (filter Filter) Validate() error {
	if !validResourceFilter(filter.ResourceKind) {
		return errs.New(errs.KindValidationFailed, "task resource filter is invalid")
	}
	if filter.Action != "" && !workexecution.ValidAction(filter.Action) {
		return errs.New(errs.KindValidationFailed, "task action filter is invalid")
	}
	switch filter.Status {
	case "", "queued", "preparing", "waiting", "running", "attention_required", "stopping", "finishing",
		"succeeded", "failed", "cancelled", "superseded":
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "task status filter is invalid")
	}
}

// List fills a filtered page without unbounded history scans. The cursor always
// identifies the last inspected Task, including non-matches, at one fixed
// revision. A scan-limited page can be empty while More remains true.
func (service *Service) List(ctx context.Context, scope, after string, limit int64, revision int64, filter Filter) (TaskPage, error) {
	if err := filter.Validate(); err != nil {
		return TaskPage{}, err
	}
	if limit < 1 || limit > 100 {
		return TaskPage{}, errs.New(errs.KindValidationFailed, "task page size must be between 1 and 100")
	}
	result := TaskPage{Tasks: []Summary{}, After: after, Revision: revision}
	const maximumScanned = 1000
	for scanned := 0; scanned < maximumScanned; {
		page, err := service.queue.ListTasks(ctx, scope, result.After, 100, result.Revision)
		if err != nil {
			return TaskPage{}, err
		}
		result.Revision = page.ReadRevision
		for index, snapshot := range page.Tasks {
			scanned++
			result.After = snapshot.Task.ID
			summary := summarize(snapshot.Task)
			if (filter.Action == "" || filter.Action == summary.Action) && (filter.Status == "" || filter.Status == summary.Status) && (filter.ResourceKind == "" || filter.ResourceKind == summary.ResourceKind) {
				result.Tasks = append(result.Tasks, summary)
			}
			if int64(len(result.Tasks)) == limit {
				result.More = index+1 < len(page.Tasks) || page.More
				return result, nil
			}
		}
		result.More = page.More
		if !page.More {
			return result, nil
		}
	}
	result.ScanLimited = true
	return result, nil
}
