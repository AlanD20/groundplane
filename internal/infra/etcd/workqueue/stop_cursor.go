package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type StopCursor struct {
	TaskID string `json:"task_id"`
	After  string `json:"after,omitempty"`
}

type StopCursorSnapshot struct {
	Record   StopCursor
	Revision int64
}

type StopPage struct {
	Items []StopCursorSnapshot
	Next  string
}

func stopKey(taskID string) string { return root + "stopping/" + taskID }

func (repository *Repository) Stopping(ctx context.Context, after string) (StopPage, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return StopPage{}, err
	}
	if after != "" && ids.Validate(ids.KindTask, after) != nil {
		return StopPage{}, invalid("task stop cursor is invalid")
	}
	request := keyvalue.RangeRequest{Prefix: root + "stopping/", Limit: 64}
	if after != "" {
		request.StartExclusive = stopKey(after)
	}
	read, err := repository.store.Range(ctx, request)
	if err != nil {
		return StopPage{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) > 64 || read.More && len(read.Values) == 0 {
		return StopPage{}, corrupt()
	}
	var page StopPage
	last := after
	for _, value := range read.Values {
		cursor, err := recordcodec.Decode[StopCursor](value.Value, "execution_stop")
		if err != nil || ids.Validate(ids.KindTask, cursor.TaskID) != nil ||
			cursor.After != "" && ids.Validate(ids.KindWorkItem, cursor.After) != nil ||
			value.Key != stopKey(cursor.TaskID) || value.ModRevision <= 0 || cursor.TaskID <= last {
			return StopPage{}, corrupt()
		}
		page.Items = append(page.Items, StopCursorSnapshot{Record: cursor, Revision: value.ModRevision})
		last = cursor.TaskID
	}
	if read.More {
		page.Next = last
	}
	return page, nil
}

// AdvanceStop follows a fully processed membership page. Cursor advancement is
// revision-checked; a lost write simply replays already-revoked items. A Task's
// forward gate remains closed after this cursor is removed.
func AdvanceStop(cursor StopCursorSnapshot, task TaskSnapshot, after string, finished bool) (Change, error) {
	if cursor.Revision <= 0 || task.Revision <= 0 || cursor.Record.TaskID != task.Task.ID ||
		task.Task.Stop == nil || !task.Task.PublicationComplete ||
		!finished && (ids.Validate(ids.KindWorkItem, after) != nil || after <= cursor.Record.After) {
		return Change{}, invalid("task stop progress is invalid")
	}
	change := Change{Conditions: []keyvalue.Condition{
		{Key: stopKey(task.Task.ID), ModRevision: cursor.Revision},
		{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
	}}
	mutation := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: stopKey(task.Task.ID)}
	if !finished {
		value, err := encode("execution_stop", StopCursor{TaskID: task.Task.ID, After: after})
		if err != nil {
			return Change{}, err
		}
		mutation.Type, mutation.Value = keyvalue.MutationPut, value
	}
	change.Mutations = []keyvalue.Mutation{mutation}
	return change, nil
}
