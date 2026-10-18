package resolverwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ResultKey(attemptID string) string { return "/v1/execution/resolver/results/" + attemptID }

type CompletionRecord struct {
	Attempt      workexecution.Attempt                   `json:"attempt"`
	Final        resolverexecution.Progress              `json:"final"`
	Verification *resolverexecution.Progress             `json:"verification,omitempty"`
	Proof        *agentpb.DNSResolverObservationEvidence `json:"proof,omitempty"`
}

func (record CompletionRecord) Validate(input resolverexecution.Input) error {
	attempt := record.Attempt
	if attempt.Validate() != nil || attempt.FinishedAt == nil || attempt.Result == nil || attempt.Direction != workexecution.Forward ||
		attempt.StartedAt == nil || attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled && attempt.Outcome != workexecution.Superseded ||
		record.Final.UpdatedAt.After(*attempt.FinishedAt) {
		return invalid("DNS resolver completion receipt has no matching applied forward result")
	}
	evidence, err := resolverexecution.CompletionEvidence(attempt, input, record.Final, record.Verification, record.Proof)
	if err != nil || evidence != *attempt.Result {
		return invalid("DNS resolver completion receipt differs from its accepted proof")
	}
	return nil
}

// PrepareCompleted joins actual serving evidence, the captured configuration,
// native completion and independent applied resource state in one transaction.
// Newer desired state is preserved even if a previously granted effect finishes.
func (repository *Repository) PrepareCompleted(ctx context.Context, view workqueue.ResumeView,
	proof *agentpb.DNSResolverObservationEvidence, at time.Time,
) (workqueue.Change, workexecution.Input, error) {
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Attempt.Attempt.FinishedAt != nil || view.Checkpoint.Record == nil ||
		view.Attempt.Attempt.Direction != workexecution.Forward {
		return workqueue.Change{}, workexecution.Input{}, invalid("DNS resolver completion requires its active final forward checkpoint")
	}
	stored, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	final, err := repository.ReadProgress(ctx, view, stored.Input, view.Checkpoint.Record.Sequence)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	var verification *resolverexecution.Progress
	if stored.Input.Enabled {
		if final.Sequence < 2 {
			return workqueue.Change{}, workexecution.Input{}, invalid("DNS resolver completion has no serving observation")
		}
		progress, err := repository.ReadProgress(ctx, view, stored.Input, final.Sequence-2)
		if err != nil {
			return workqueue.Change{}, workexecution.Input{}, err
		}
		verification = &progress
	}
	evidence, err := resolverexecution.CompletionEvidence(view.Attempt.Attempt, stored.Input, final, verification, proof)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	if final.UpdatedAt.After(at) {
		return workqueue.Change{}, workexecution.Input{}, invalid("DNS resolver observation is ahead of completion time")
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	completion, err := queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	outcome := forwardOutcome(view)
	completion.At, completion.Result = at, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: true}
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	_, terminal, err := workexecution.Finish(view.Work.Item.Item, view.Attempt.Attempt, completion.Result, at)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	receipt := CompletionRecord{Attempt: terminal, Final: final, Verification: verification, Proof: proof}
	if err := receipt.Validate(stored.Input); err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	raw, err := recordcodec.Encode("resolver_completion", receipt)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	if len(raw) > resolverexecution.MaximumReportBytes {
		return workqueue.Change{}, workexecution.Input{}, invalid("DNS resolver completion receipt exceeds its bound")
	}
	applied := Applied{HostID: stored.Input.HostID, ComponentID: stored.Input.ComponentID, Generation: stored.Input.Generation,
		Desired: stored.Capture.Desired, Configuration: stored.Input.Candidate, ServingProof: proof,
		BaselineSHA256: stored.Capture.BaselineSHA256, ProjectionSHA256: stored.Capture.Projection.InputSHA256, ObservedAt: at.UTC()}
	if stored.Input.Enabled {
		applied.FileOwner, err = resolverexecution.FileTransactionID(view.Attempt.Attempt, *stored.Input.Candidate)
		if err != nil {
			return workqueue.Change{}, workexecution.Input{}, err
		}
		applied.Hosts, applied.EnvironmentDNS = stored.Capture.Hosts, stored.Capture.EnvironmentDNS
		applied.PrivateListener, applied.ZoneListeners = stored.Capture.PrivateListener, stored.Capture.ZoneListeners
	} else {
		applied.Configuration, applied.FileOwner = stored.Input.Previous, stored.Input.PreviousFileOwner
	}
	state, err := repository.prepareApplied(ctx, view, stored, applied)
	if err != nil {
		return workqueue.Change{}, workexecution.Input{}, err
	}
	change.Conditions = append(change.Conditions, state.Conditions...)
	change.Mutations = append(change.Mutations, state.Mutations...)
	change.Conditions = append(change.Conditions, stored.Guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ResultKey(terminal.ID)},
		keyvalue.Condition{Key: workqueue.CheckpointKey(terminal.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(terminal.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ResultKey(terminal.ID), Value: raw})
	return change, evidence, nil
}

func forwardOutcome(view workqueue.ResumeView) workexecution.Outcome {
	if stop := view.Work.Task.Task.Stop; stop != nil {
		return stop.Outcome
	}
	if view.Work.Item.Item.ForwardRevoked {
		return workexecution.Superseded
	}
	return workexecution.Succeeded
}

func (repository *Repository) prepareApplied(ctx context.Context, view workqueue.ResumeView, stored StoredInput, applied Applied) (workqueue.Change, error) {
	key := componentrecord.RecordKey(stored.Input.ComponentID)
	read, err := repository.read(ctx, []string{key, AppliedKey(stored.Input.HostID, stored.Input.ComponentID)}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || keyvalue.RevisionOf(read.Values[1]) != stored.Capture.AppliedRevision {
		return workqueue.Change{}, invalid("DNS resolver applied predecessor changed before runtime settlement")
	}
	current, err := componentrecord.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	var services []string
	if applied.Desired.Enabled {
		services = []string{applied.Configuration.Runtime.Services[0].ServiceId}
	}
	current, err = componentrecord.SetRuntime(current, services, "", applied.Desired.Enabled)
	if err != nil {
		return workqueue.Change{}, err
	}
	component, err := componentrecord.EncodeRecord(current)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := EncodeApplied(applied)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: AppliedKey(stored.Input.HostID, stored.Input.ComponentID), ModRevision: stored.Capture.AppliedRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: component}, componentrecord.WriteFenceMutation(stored.Input.ComponentID),
			{Type: keyvalue.MutationPut, Key: AppliedKey(stored.Input.HostID, stored.Input.ComponentID), Value: value}}}, nil
}

// ReadCompleted reads only retained execution evidence, not the current applied
// head. A later successful change cannot invalidate a lost terminal ACK replay.
func (repository *Repository) ReadCompleted(ctx context.Context, view workqueue.ResumeView) (CompletionRecord, keyvalue.Condition, error) {
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil {
		return CompletionRecord{}, keyvalue.Condition{}, invalid("DNS resolver has no terminal completion receipt")
	}
	stored, err := repository.Load(ctx, view.Work)
	if err != nil {
		return CompletionRecord{}, keyvalue.Condition{}, err
	}
	key := ResultKey(attempt.ID)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return CompletionRecord{}, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || len(read.Values[0].Value) > resolverexecution.MaximumReportBytes {
		return CompletionRecord{}, keyvalue.Condition{}, invalid("DNS resolver completion receipt is unavailable")
	}
	record, err := recordcodec.Decode[CompletionRecord](read.Values[0].Value, "resolver_completion")
	if err != nil || record.Validate(stored.Input) != nil || !workexecution.SameAssignment(record.Attempt, attempt) ||
		record.Attempt.Outcome != attempt.Outcome || record.Attempt.Result == nil || *record.Attempt.Result != *attempt.Result ||
		record.Attempt.FinishedAt == nil || !record.Attempt.FinishedAt.Equal(*attempt.FinishedAt) {
		return CompletionRecord{}, keyvalue.Condition{}, invalid("DNS resolver completion receipt differs from its accepted terminal result")
	}
	return record, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
