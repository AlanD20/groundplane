package environmentprovision

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ResultKey(attemptID string) string {
	return "/v1/execution/environment-provision/results/" + attemptID
}
func RecoveryKey(workID string) string {
	return "/v1/execution/environment-provision/recovery/" + workID
}

type Receipt struct {
	Attempt workexecution.Attempt       `json:"attempt"`
	Report  environmentexecution.Result `json:"report"`
}

func (repository *Repository) ReadResult(
	ctx context.Context,
	attempt workexecution.Attempt,
	revision int64,
) (keyvalue.Versioned[Receipt], error) {
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{ResultKey(attempt.ID)}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[Receipt]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision ||
		len(read.Values) != 1 ||
		read.Values[0] == nil {
		return keyvalue.Versioned[Receipt]{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning result is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	receipt, err := recordcodec.Decode[Receipt](read.Values[0].Value, "environment_provision_result")
	if err != nil {
		return keyvalue.Versioned[Receipt]{}, err
	}
	expected, err := receipt.Report.Reference(receipt.Attempt)
	if err != nil || receipt.Attempt.Validate() != nil || !workexecution.SameAssignment(attempt, receipt.Attempt) ||
		(attempt.StartedAt == nil) != (receipt.Attempt.StartedAt == nil) ||
		attempt.StartedAt != nil && !attempt.StartedAt.Equal(*receipt.Attempt.StartedAt) ||
		receipt.Attempt.FinishedAt == nil || receipt.Attempt.Result == nil || *receipt.Attempt.Result != expected ||
		receipt.Attempt.FinishedAt.Before(receipt.Report.ObservedAt) {
		return keyvalue.Versioned[Receipt]{}, errs.New(
			errs.KindInternal,
			"environment provisioning result ownership is corrupt",
		)
	}
	return keyvalue.Versioned[Receipt]{
		Record:       receipt,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

// PrepareResult is committed with native slot/claim settlement. Unresolved
// helpers leave effects owned; successful cleanup never rewrites forward failure.
func (repository *Repository) PrepareResult(
	ctx context.Context,
	view workqueue.ResumeView,
	report environmentexecution.Result,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	input, captured, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	attempt := view.Attempt.Attempt
	completion, err := report.Completion(attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if attempt.Executor.HostID != input.HostID || report.ObservedAt.After(at) || attempt.FinishedAt != nil ||
		!view.ActiveAttempt || view.Checkpoint.Record != nil || view.Checkpoint.Revision != 0 ||
		completion.Outcome == workexecution.Succeeded && attempt.Direction == workexecution.Forward &&
			attempt.StartedAt == nil {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning result lacks its retained execution authority",
		)
	}
	if attempt.Direction == workexecution.Forward {
		if view.Work.Task.Task.Stop != nil {
			completion.Outcome = view.Work.Task.Task.Stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			completion.Outcome = workexecution.Superseded
		}
	}
	terminal := attempt
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &at, &completion.Evidence, completion.Outcome
	if err := terminal.Validate(); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, err := recordcodec.Encode("environment_provision_result", Receipt{Attempt: terminal, Report: report})
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{captured, {Key: ResultKey(attempt.ID)}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ResultKey(attempt.ID), Value: raw}}}
	if attempt.Direction == workexecution.Recovery {
		_, source, err := repository.LoadRecovery(ctx, view.Work)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		change.Conditions = append(change.Conditions, source)
	}
	if attempt.Direction == workexecution.Forward {
		state, err := repository.PrepareResourceResult(
			ctx,
			view.Work,
			report.HelpersAbsent && report.DirectoryReady && report.Failure == "",
		)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		// PrepareResourceResult includes the same immutable capture comparison.
		change.Conditions = append(change.Conditions, state.Conditions[1:]...)
		change.Mutations = append(change.Mutations, state.Mutations...)
	}
	return change, completion, nil
}

func (repository *Repository) LoadRecovery(
	ctx context.Context,
	view workqueue.View,
) (workexecution.Attempt, keyvalue.Condition, error) {
	key := RecoveryKey(view.Item.Item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return workexecution.Attempt{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return workexecution.Attempt{}, keyvalue.Condition{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning cleanup source is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	source, err := recordcodec.Decode[workexecution.Attempt](read.Values[0].Value, "environment_provision_recovery")
	if err != nil {
		return workexecution.Attempt{}, keyvalue.Condition{}, err
	}
	item := view.Item.Item
	if source.Validate() != nil || source.TaskID != view.Task.Task.ID || source.WorkItemID != item.ID ||
		source.Executor.HostID != item.Placement.HostID || item.Input == nil || source.Input != *item.Input ||
		source.Direction != workexecution.Forward || source.FinishedAt == nil || source.Result == nil || source.Outcome == workexecution.Succeeded ||
		source.Number > item.AttemptNumber || item.Result == nil || *source.Result != *item.Result {
		return workexecution.Attempt{}, keyvalue.Condition{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning cleanup source differs from the original failure",
		)
	}
	return source, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
