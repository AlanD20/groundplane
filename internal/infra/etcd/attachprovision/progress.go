package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// LoadProgress returns only acknowledged command evidence. Absence means that
// no command was authorized, not that a started process is known to have exited.
func (repository *InputRepository) LoadProgress(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.ProvisionInput,
) (*attachexecution.ProvisionProgress, []keyvalue.Condition, error) {
	attempt := view.Attempt.Attempt
	if view.Work.Item.Item.Operation != workexecution.Attach || view.Work.ReadRevision <= 0 ||
		!workexecution.SameAssignment(input.Attempt, attempt) || view.Checkpoint.Revision < 0 {
		return nil, nil, checkpointConflict("Attach progress does not match its saved execution")
	}
	authority, err := input.Authority()
	if err != nil {
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision}}
	if view.Checkpoint.Record == nil {
		if view.Checkpoint.Revision != 0 {
			return nil, nil, checkpointConflict("Attach checkpoint metadata has no saved evidence")
		}
		return nil, conditions, nil
	}
	if view.Checkpoint.Revision == 0 || attempt.StartedAt == nil || input.Attempt.StartedAt == nil ||
		!input.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return nil, nil, checkpointConflict("Attach checkpoint has no matching execution start")
	}
	key := ReportKey(attempt.ID, view.Checkpoint.Record.Sequence)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.Work.ReadRevision {
		return nil, nil, checkpointConflict("Attach checkpoint command evidence is unavailable")
	}
	report, err := attachexecution.DecodeProvisionReport(input, read.Values[0].Value)
	if err != nil {
		return nil, nil, err
	}
	expected, err := report.CheckpointAuthority(attempt, authority)
	if err != nil || !sameCheckpoint(expected, *view.Checkpoint.Record) {
		return nil, nil, checkpointConflict("Attach checkpoint differs from its saved command evidence")
	}
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return &report.Progress, conditions, nil
}
