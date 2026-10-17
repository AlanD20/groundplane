package workqueue

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

const taskListRoot = root + "task-list/"

func taskListKeys(task workexecution.Task) []string {
	keys := []string{taskListRoot + "all/" + task.ID}
	if task.Scope.TenantID == "" {
		keys = append(keys, taskListRoot+"platform/"+task.ID)
	}
	for _, scope := range []string{task.Scope.TenantID, task.Scope.ProjectID, task.Scope.EnvironmentID} {
		if scope != "" {
			keys = append(keys, taskListRoot+scope+"/"+task.ID)
		}
	}
	return keys
}

func publishTaskList(change *Change, task workexecution.Task) {
	for _, key := range taskListKeys(task) {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(task.ID)})
	}
}

type TaskPage struct {
	Tasks        []TaskSnapshot
	More         bool
	LastID       string
	ReadRevision int64
}

// ListTasks returns newest-first Task identities at a fixed snapshot. Scope is
// all, platform, or one stable Tenant/Project/Environment ID, never a slug.
func (repository *Repository) ListTasks(ctx context.Context, scope, after string, limit int64, revision int64) (TaskPage, error) {
	validScope := scope == "all" || scope == "platform" || ids.Validate(ids.KindTenant, scope) == nil ||
		ids.Validate(ids.KindProject, scope) == nil || ids.Validate(ids.KindEnvironment, scope) == nil
	if !validScope || after != "" && ids.Validate(ids.KindTask, after) != nil || limit < 1 || limit > 100 || revision < 0 {
		return TaskPage{}, invalid("Task list scope, cursor or page size is invalid")
	}
	prefix := taskListRoot + scope + "/"
	request := keyvalue.RangeRequest{Prefix: prefix, Limit: limit, Revision: revision, Descending: true}
	if after != "" {
		request.StartExclusive = prefix + after
	}
	page, err := repository.store.Range(ctx, request)
	if err != nil {
		return TaskPage{}, err
	}
	if page == nil || page.ReadRevision <= 0 || revision > 0 && revision != page.ReadRevision || int64(len(page.Values)) > limit || page.More && len(page.Values) == 0 {
		return TaskPage{}, corrupt()
	}
	result := TaskPage{Tasks: []TaskSnapshot{}, More: page.More, ReadRevision: page.ReadRevision}
	keys := make([]string, 0, len(page.Values))
	previous := after
	for _, value := range page.Values {
		id := string(value.Value)
		if ids.Validate(ids.KindTask, id) != nil || value.Key != prefix+id || previous != "" && strings.Compare(id, previous) >= 0 {
			return TaskPage{}, corrupt()
		}
		keys = append(keys, TaskKey(id))
		previous, result.LastID = id, id
	}
	if len(keys) == 0 {
		return result, nil
	}
	read, err := repository.read(ctx, keys, page.ReadRevision)
	if err != nil {
		return TaskPage{}, err
	}
	for index, value := range read.Values {
		if value == nil {
			return TaskPage{}, corrupt()
		}
		task, err := DecodeTask(value.Value)
		if err != nil {
			return TaskPage{}, err
		}
		if task.ID != string(page.Values[index].Value) || !taskScopeMatches(task.Scope, scope) {
			return TaskPage{}, corrupt()
		}
		result.Tasks = append(result.Tasks, TaskSnapshot{Task: task, Revision: value.ModRevision})
	}
	return result, nil
}

func taskScopeMatches(scope workexecution.Scope, selected string) bool {
	return selected == "all" || selected == "platform" && scope.TenantID == "" ||
		selected == scope.TenantID || selected == scope.ProjectID || selected == scope.EnvironmentID
}
