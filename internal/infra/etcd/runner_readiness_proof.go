package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecordRunnerReadinessProof durably binds readiness to the exact running
// creation Task, lifecycle, container, and byte-identical runtime ownership.
// Task completion consumes this record in its terminal transaction.
func (repository *RunnerRepository) RecordRunnerReadinessProof(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner readiness task id is invalid",
		)
	}
	keys := []string{
		taskjournal.TaskStorageKey(taskID),
		runnerrecord.RunnerReadinessProofKey(taskID),
	}
	initial, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	if initial == nil || len(initial.Values) != len(keys) || initial.Values[0] == nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness task is unavailable",
		)
	}
	task, err := DecodeTaskRecord(initial.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	applies, err := taskOwnsRunnerCreation(task)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	if !applies || task.ID != taskID || task.Status != taskjournal.TaskStatusRunning {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness task is not running",
		)
	}
	stateKeys := []string{
		runnerrecord.RunnerKey(task.Target),
		runnerrecord.RunnerLifecycleKey(task.Target),
		runnerrecord.RunnerRuntimeOwnershipKey(task.Target),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: stateKeys, Revision: initial.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) ||
		state.Values[0] == nil || state.Values[1] == nil || state.Values[2] == nil || state.Values[3] != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness evidence is incomplete",
		)
	}
	runner, err := runnerrecord.DecodeRunnerAggregate(state.Values[0], state.Values[1])
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	ownership, err := runnerrecord.DecodeRunnerRuntimeOwnership(state.Values[2].Value)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	if runner.Desired.ID != task.Target || runner.CreateTaskID != task.ID ||
		runner.ProvisioningState != runnerrecord.RunnerProvisioningProvisioning || runner.ContainerID == "" ||
		ownership.RunnerID != runner.Desired.ID || ownership.RuntimeEpoch != runner.RuntimeEpoch {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness evidence does not match its lifecycle",
		)
	}
	proof := runnerrecord.RunnerReadinessProofRecord{
		RunnerID:                 runner.Desired.ID,
		TaskID:                   task.ID,
		RuntimeEpoch:             runner.RuntimeEpoch,
		ContainerID:              runner.ContainerID,
		RuntimeOwnershipRevision: state.Values[2].ModRevision,
		RuntimeOwnershipSHA256:   runnerrecord.RunnerRuntimeOwnershipSHA256(state.Values[2].Value),
	}
	proofValue, err := runnerrecord.EncodeRunnerReadinessProof(proof)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	defer clear(proofValue)
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: initial.Values[0].ModRevision},
		{Key: stateKeys[0], ModRevision: state.Values[0].ModRevision},
		{Key: stateKeys[1], ModRevision: state.Values[1].ModRevision},
		{Key: stateKeys[2], ModRevision: state.Values[2].ModRevision},
		{Key: stateKeys[3]},
		{Key: keys[1]},
	}
	transaction, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: keys[1], Value: proofValue,
	}})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, err
	}
	if transaction.Succeeded {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{
			Record: proof, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
	if len(transaction.FailureReads) != len(conditions) {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, errs.New(
			errs.KindInternal,
			"runner readiness compare evidence is incomplete",
		)
	}
	for index := 0; index < len(conditions)-1; index++ {
		if etcdstore.RevisionOf(transaction.FailureReads[index]) != conditions[index].ModRevision {
			return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, recordcodec.StateConflict("runner readiness", task.Target)
		}
	}
	existing := transaction.FailureReads[len(conditions)-1]
	if existing == nil {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, recordcodec.StateConflict("runner readiness", task.Target)
	}
	stored, err := runnerrecord.DecodeRunnerReadinessProof(existing.Value)
	if err != nil || stored != proof {
		return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{}, recordcodec.StateConflict("runner readiness", task.Target)
	}
	return etcdstore.Versioned[runnerrecord.RunnerReadinessProofRecord]{
		Record: stored, Revision: existing.ModRevision, ReadRevision: transaction.Revision,
	}, nil
}

func runnerReadinessProofMatches(
	proof runnerrecord.RunnerReadinessProofRecord,
	task TaskRecord,
	runner runnerrecord.RunnerRecord,
	ownership *etcdstore.KeyValue,
) bool {
	return ownership != nil && proof.RunnerID == runner.Desired.ID && proof.TaskID == task.ID &&
		proof.RuntimeEpoch == runner.RuntimeEpoch && proof.ContainerID == runner.ContainerID &&
		proof.RuntimeOwnershipRevision == ownership.ModRevision &&
		proof.RuntimeOwnershipSHA256 == runnerrecord.RunnerRuntimeOwnershipSHA256(ownership.Value)
}
