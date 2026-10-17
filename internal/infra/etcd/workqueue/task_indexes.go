package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type TaskIndexPage struct {
	Tasks []TaskSnapshot
	Next  string
}

// Finishing scans only Tasks whose last work item has settled. The index is
// maintained with Task counters; completion never depends on a worker callback.
func (repository *Repository) Finishing(ctx context.Context, after string) (TaskIndexPage, error) {
	return repository.indexedTasks(ctx, after, root+"finishing/", taskReadyToFinish)
}

// Publishing discovers accepted graphs that still need publication, including
// after restart. Readers must use the captured plan, never current desired state.
func (repository *Repository) Publishing(ctx context.Context, after string) (TaskIndexPage, error) {
	return repository.indexedTasks(ctx, after, root+"publishing/", taskNeedsPublication)
}

func taskNeedsPublication(task workexecution.Task) bool {
	return !task.PublicationComplete && task.FinishedAt == nil
}

func (repository *Repository) indexedTasks(ctx context.Context, after, prefix string,
	member func(workexecution.Task) bool,
) (TaskIndexPage, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return TaskIndexPage{}, err
	}
	if after != "" && ids.Validate(ids.KindTask, after) != nil {
		return TaskIndexPage{}, invalid("task index cursor is invalid")
	}
	request := keyvalue.RangeRequest{Prefix: prefix, Limit: 32}
	if after != "" {
		request.StartExclusive = prefix + after
	}
	index, err := repository.store.Range(ctx, request)
	if err != nil {
		return TaskIndexPage{}, err
	}
	if index == nil || index.ReadRevision <= 0 || len(index.Values) > 32 || index.More && len(index.Values) == 0 {
		return TaskIndexPage{}, corrupt()
	}
	var page TaskIndexPage
	keys := make([]string, 0, len(index.Values))
	last := after
	for _, entry := range index.Values {
		id := string(entry.Value)
		if ids.Validate(ids.KindTask, id) != nil || entry.Key != prefix+id || id <= last ||
			entry.ModRevision <= 0 || entry.ModRevision > index.ReadRevision {
			return TaskIndexPage{}, corrupt()
		}
		keys = append(keys, TaskKey(id))
		last = id
	}
	if index.More {
		page.Next = last
	}
	if len(keys) == 0 {
		return page, nil
	}
	read, err := repository.read(ctx, keys, index.ReadRevision)
	if err != nil {
		return TaskIndexPage{}, err
	}
	for offset, value := range read.Values {
		if value == nil {
			return TaskIndexPage{}, corrupt()
		}
		task, err := DecodeTask(value.Value)
		if err != nil || task.ID != string(index.Values[offset].Value) || !member(task) {
			return TaskIndexPage{}, corrupt()
		}
		page.Tasks = append(page.Tasks, TaskSnapshot{Task: task, Revision: value.ModRevision})
	}
	return page, nil
}
