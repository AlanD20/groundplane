package workqueue

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

const taskCompletionPrefix = root + "task-completions/"

func taskCompletionKey(taskID string) string { return taskCompletionPrefix + taskID }

// prepareTaskCompletionReceipt joins the existing Task settlement/reopen CAS.
// No per-child fanout is needed. Only the tiny receipt can outlive Task history.
func prepareTaskCompletionReceipt(before, after workexecution.Task) (Change, error) {
	if before.FinishedAt == nil && after.FinishedAt == nil {
		return Change{}, nil
	}
	if before.FinishedAt != nil && after.FinishedAt != nil &&
		before.FinishedAt.Equal(*after.FinishedAt) && before.Outcome == after.Outcome {
		return Change{}, nil
	}
	change := Change{Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: taskCompletionKey(after.ID)}}}
	if after.FinishedAt != nil {
		fact := taskCompletion(after)
		if err := fact.Validate(); err != nil {
			return Change{}, err
		}
		value, err := encode("execution_task_completion", fact)
		if err != nil {
			return Change{}, err
		}
		change.Mutations[0].Type, change.Mutations[0].Value = keyvalue.MutationPut, value
	}
	return change, nil
}

// CollectTaskCompletionReceipts performs a bounded cursor sweep independent of
// operation-owned history collection. A concurrent child acceptance compares
// the existing parent Task revision; once that Task is gone no new reverse edge
// can be admitted. A pending child protects only this receipt, never logs/plans.
func (repository *Repository) CollectTaskCompletionReceipts(ctx context.Context, after string) (string, error) {
	if after != "" && ids.Validate(ids.KindTask, after) != nil {
		return "", invalid("Task completion collection cursor is invalid")
	}
	request := keyvalue.RangeRequest{Prefix: taskCompletionPrefix, Limit: 32}
	if after != "" {
		request.StartExclusive = taskCompletionKey(after)
	}
	page, err := repository.store.Range(ctx, request)
	if err != nil {
		return "", err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 32 || page.More && len(page.Values) == 0 {
		return "", corrupt()
	}
	last := after
	for _, value := range page.Values {
		id := strings.TrimPrefix(value.Key, taskCompletionPrefix)
		fact, err := recordcodec.Decode[workexecution.TaskCompletion](value.Value, "execution_task_completion")
		if err != nil || fact.Validate() != nil || fact.TaskID != id || id <= last ||
			value.ModRevision <= 0 || value.ModRevision > page.ReadRevision {
			return "", corrupt()
		}
		last = id
		change := Change{Conditions: []keyvalue.Condition{
			{Key: TaskKey(id)}, {Key: taskDependentPrefix(id), Prefix: true},
			{Key: value.Key, ModRevision: value.ModRevision},
		}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: value.Key}}}
		if _, err := repository.Commit(ctx, change); err != nil {
			return "", err
		}
	}
	if page.More {
		return last, nil
	}
	return "", nil
}
