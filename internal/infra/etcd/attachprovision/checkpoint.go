package attachprovision

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ReportPrefix(attemptID string) string {
	return "/v1/attach-provision-reports/" + attemptID + "/"
}
func ReportKey(attemptID string, sequence uint64) string {
	return ReportPrefix(attemptID) + strconv.FormatUint(sequence, 10)
}

// PrepareCheckpoint saves bounded command evidence. The caller joins authenticated
// Agent conditions before acknowledging; successful commands do not settle facts.
func (repository *InputRepository) PrepareCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.ProvisionInput, report attachexecution.ProvisionReport,
) (workqueue.Change, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if !workexecution.SameAssignment(input.Attempt, attempt) || attempt.StartedAt == nil ||
		input.Attempt.StartedAt == nil ||
		!input.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
		item.Operation != workexecution.Attach ||
		view.Work.ReadRevision <= 0 {
		return workqueue.Change{}, checkpointConflict("Attach provisioning report has no matching started execution")
	}
	source, err := repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	authority, err := input.Authority()
	if err != nil {
		return workqueue.Change{}, err
	}
	capture := source.Generation.Record
	if capture.SettlesInController() || authority.AttachID != capture.AttachID ||
		authority.BackingServiceID != capture.BackingServiceID ||
		input.Provision.EnvironmentId != capture.EnvironmentID ||
		input.Provision.TenantId != task.Scope.TenantID || input.Provision.ProjectId != task.Scope.ProjectID ||
		input.Provision.ServiceId != capture.ConsumerServiceID {
		return workqueue.Change{}, checkpointConflict("Attach provisioning report differs from its captured binding")
	}
	expected, err := capture.ProvisionAuthority()
	if err != nil {
		return workqueue.Change{}, err
	}
	if authority != expected {
		return workqueue.Change{}, checkpointConflict("Attach provisioning report changed its captured commands")
	}
	raw, err := attachexecution.EncodeProvisionReport(input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(raw)
	next, err := report.CheckpointAuthority(attempt, authority)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := ReportKey(attempt.ID, next.Sequence)
	keys := []string{key}
	if view.Checkpoint.Record != nil {
		keys = append(keys, ReportKey(attempt.ID, view.Checkpoint.Record.Sequence))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, checkpointConflict("Attach provisioning checkpoint read is incomplete")
	}
	var previous *attachexecution.ProvisionProgress
	if view.Checkpoint.Record != nil {
		value := read.Values[1]
		if value == nil || value.Key != keys[1] || value.ModRevision <= 0 {
			return workqueue.Change{}, checkpointConflict("Attach provisioning has no saved checkpoint report")
		}
		saved, err := attachexecution.DecodeProvisionReport(input, value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		cursor, err := saved.CheckpointAuthority(attempt, authority)
		if err != nil || !sameCheckpoint(cursor, *view.Checkpoint.Record) {
			return workqueue.Change{}, checkpointConflict("Attach provisioning report differs from its saved cursor")
		}
		previous = &saved.Progress
	}
	if existing := read.Values[0]; existing != nil {
		if existing.Key != key || existing.ModRevision <= 0 || !bytes.Equal(existing.Value, raw) ||
			view.Checkpoint.Record == nil || next.Sequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, checkpointConflict("Attach provisioning sequence has different saved evidence")
		}
		return workqueue.Change{
			Conditions: append(source.Conditions, keyvalue.Condition{Key: key, ModRevision: existing.ModRevision},
				keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision}),
			Mutations: []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)},
			},
		}, nil
	}
	if !view.ActiveAttempt || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return workqueue.Change{}, checkpointConflict("Attach provisioning no longer accepts new reports")
	}
	if report.Progress.InFlight &&
		(!view.CurrentExecutor || item.ForwardRevoked || task.Stop != nil || !time.Now().UTC().Before(attempt.Deadline)) {
		return workqueue.Change{}, checkpointConflict(
			"Attach provisioning cannot begin a command after stop, replacement or deadline",
		)
	}
	if err := attachexecution.ValidateProvisionTransition(attempt, authority, previous, report.Progress); err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, source.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision})
	if report.Progress.InFlight {
		current, desired, err := repository.ReadDesiredMatch(ctx, view.Work)
		if err != nil || !current {
			keyvalue.ZeroMutationBytes(change.Mutations)
			if err != nil {
				return workqueue.Change{}, err
			}
			return workqueue.Change{}, checkpointConflict("Attach provisioning was superseded before this command")
		}
		change.Conditions = append(change.Conditions, desired...)
		guards, err := repository.PendingBindingConditions(ctx, view.Work)
		if err != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, guards...)
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
		)
	}
	consumer, err := repository.prepareConsumerCheckpoint(ctx, view, capture, input, report)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, consumer.Conditions...)
	change.Mutations = append(change.Mutations, consumer.Mutations...)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)},
	)
	return change, nil
}

func sameCheckpoint(left, right workexecution.Checkpoint) bool {
	return left.AttemptID == right.AttemptID && left.Sequence == right.Sequence && left.StageID == right.StageID &&
		left.Evidence == right.Evidence && left.RecordedAt.Equal(right.RecordedAt)
}

func checkpointConflict(detail string) error { return errs.New(errs.KindStateConflict, detail) }
