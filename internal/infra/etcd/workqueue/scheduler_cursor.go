package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// SchedulerCursor preserves queue rotation across reconnects. It grants no work
// authority: assignments always compare their own resources and executor record.
type SchedulerCursor struct {
	Placement workexecution.Placement `json:"placement"`
	After     string                  `json:"after,omitempty"`
}

type SchedulerCursorSnapshot struct {
	Record   SchedulerCursor
	Revision int64
}

func schedulerCursorKey(target workexecution.Placement) string {
	return root + "scheduler-cursors/" + placement(target.Executor, target.HostID)
}

func (repository *Repository) ReadSchedulerCursor(ctx context.Context, target workexecution.Placement) (SchedulerCursorSnapshot, error) {
	if err := target.Validate(); err != nil {
		return SchedulerCursorSnapshot{}, err
	}
	read, err := repository.read(ctx, []string{schedulerCursorKey(target)}, 0)
	if err != nil {
		return SchedulerCursorSnapshot{}, err
	}
	if read.Values[0] == nil {
		return SchedulerCursorSnapshot{Record: SchedulerCursor{Placement: target}}, nil
	}
	record, err := recordcodec.Decode[SchedulerCursor](read.Values[0].Value, "execution_scheduler_cursor")
	if err != nil || record.Placement != target || record.After != "" && ids.Validate(ids.KindWorkItem, record.After) != nil {
		return SchedulerCursorSnapshot{}, corrupt()
	}
	return SchedulerCursorSnapshot{Record: record, Revision: read.Values[0].ModRevision}, nil
}

func PrepareSchedulerCursor(previous SchedulerCursorSnapshot, executor ExecutorSnapshot, after string) (Change, error) {
	if executor.Record.Validate() != nil || executor.Revision <= 0 || previous.Revision < 0 ||
		previous.Record.Placement.Executor != executor.Record.Identity.Executor ||
		previous.Record.Placement.HostID != executor.Record.Identity.HostID ||
		after != "" && ids.Validate(ids.KindWorkItem, after) != nil {
		return Change{}, invalid("scheduler cursor does not match its executor")
	}
	next := previous.Record
	next.After = after
	value, err := encode("execution_scheduler_cursor", next)
	if err != nil {
		return Change{}, err
	}
	key := schedulerCursorKey(next.Placement)
	return Change{
		Conditions: []keyvalue.Condition{{Key: key, ModRevision: previous.Revision},
			{Key: ExecutorKey(executor.Record.Identity), ModRevision: executor.Revision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	}, nil
}
