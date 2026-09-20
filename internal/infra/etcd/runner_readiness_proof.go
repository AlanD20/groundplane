package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runnerReadinessProofPrefix = "/v1/runtime/runner-readiness-proofs/"

// RunnerReadinessProofRecord is the single-use durable witness produced after
// the active Runner creation Task proves its exact materialized runtime ready.
type RunnerReadinessProofRecord struct {
	RunnerID                 string `json:"runner_id"`
	TaskID                   string `json:"task_id"`
	RuntimeEpoch             uint64 `json:"runtime_epoch"`
	ContainerID              string `json:"container_id"`
	RuntimeOwnershipRevision int64  `json:"runtime_ownership_revision"`
	RuntimeOwnershipSHA256   string `json:"runtime_ownership_sha256"`
}

// RecordRunnerReadinessProof durably binds readiness to the exact running
// creation Task, lifecycle, container, and byte-identical runtime ownership.
// Task completion consumes this record in its terminal transaction.
func (repository *RunnerRepository) RecordRunnerReadinessProof(
	ctx context.Context,
	taskID string,
) (Versioned[RunnerReadinessProofRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner readiness task id is invalid",
		)
	}
	keys := []string{
		taskKey(taskID),
		runnerReadinessProofKey(taskID),
	}
	initial, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	if initial == nil || len(initial.Values) != len(keys) || initial.Values[0] == nil {
		return Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness task is unavailable",
		)
	}
	task, err := decodeTaskRecord(initial.Values[0].Value)
	if err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	applies, err := taskOwnsRunnerCreation(task)
	if err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	if !applies || task.ID != taskID || task.Status != TaskStatusRunning {
		return Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness task is not running",
		)
	}
	stateKeys := []string{
		runnerKey(task.Target),
		runnerLifecycleKey(task.Target),
		runnerRuntimeOwnershipKey(task.Target),
		deletionTombstoneKey(string(DeletionTargetRunner), task.Target),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: stateKeys, Revision: initial.ReadRevision,
	})
	if err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) ||
		state.Values[0] == nil || state.Values[1] == nil || state.Values[2] == nil || state.Values[3] != nil {
		return Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness evidence is incomplete",
		)
	}
	runner, err := decodeRunnerAggregate(state.Values[0], state.Values[1])
	if err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	ownership, err := decodeRunnerRuntimeOwnership(state.Values[2].Value)
	if err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	if runner.Desired.ID != task.Target || runner.CreateTaskID != task.ID ||
		runner.ProvisioningState != RunnerProvisioningProvisioning || runner.ContainerID == "" ||
		ownership.RunnerID != runner.Desired.ID || ownership.RuntimeEpoch != runner.RuntimeEpoch {
		return Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness evidence does not match its lifecycle",
		)
	}
	proof := RunnerReadinessProofRecord{
		RunnerID:                 runner.Desired.ID,
		TaskID:                   task.ID,
		RuntimeEpoch:             runner.RuntimeEpoch,
		ContainerID:              runner.ContainerID,
		RuntimeOwnershipRevision: state.Values[2].ModRevision,
		RuntimeOwnershipSHA256:   runnerRuntimeOwnershipSHA256(state.Values[2].Value),
	}
	proofValue, err := encodeRunnerReadinessProof(proof)
	if err != nil {
		return Versioned[RunnerReadinessProofRecord]{}, err
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
		return Versioned[RunnerReadinessProofRecord]{}, err
	}
	if transaction.Succeeded {
		return Versioned[RunnerReadinessProofRecord]{
			Record: proof, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
	if len(transaction.FailureReads) != len(conditions) {
		return Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindInternal,
			"runner readiness compare evidence is incomplete",
		)
	}
	for index := 0; index < len(conditions)-1; index++ {
		if keyValueRevision(transaction.FailureReads[index]) != conditions[index].ModRevision {
			return Versioned[RunnerReadinessProofRecord]{}, stateConflict("runner readiness", task.Target)
		}
	}
	existing := transaction.FailureReads[len(conditions)-1]
	if existing == nil {
		return Versioned[RunnerReadinessProofRecord]{}, stateConflict("runner readiness", task.Target)
	}
	stored, err := decodeRunnerReadinessProof(existing.Value)
	if err != nil || stored != proof {
		return Versioned[RunnerReadinessProofRecord]{}, stateConflict("runner readiness", task.Target)
	}
	return Versioned[RunnerReadinessProofRecord]{
		Record: stored, Revision: existing.ModRevision, ReadRevision: transaction.Revision,
	}, nil
}

func validateRunnerReadinessProof(record RunnerReadinessProofRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || record.RuntimeEpoch == 0 ||
		!validLowerHex(record.ContainerID, runnerContainerIDEncodedLength) ||
		record.RuntimeOwnershipRevision <= 0 || !validLowerHex(record.RuntimeOwnershipSHA256, sha256.Size*2) {
		return errs.New(errs.KindValidationFailed, "runner readiness proof is invalid")
	}
	return nil
}

func encodeRunnerReadinessProof(record RunnerReadinessProofRecord) ([]byte, error) {
	if err := validateRunnerReadinessProof(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("runner_readiness_proof", record)
}

func decodeRunnerReadinessProof(value []byte) (RunnerReadinessProofRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerReadinessProofRecord{}, errs.New(errs.KindInternal, "runner readiness proof is corrupt")
	}
	record, err := decodeEnvelope[RunnerReadinessProofRecord](value, "runner_readiness_proof")
	if err != nil || validateRunnerReadinessProof(record) != nil {
		return RunnerReadinessProofRecord{}, errs.New(errs.KindInternal, "runner readiness proof is corrupt")
	}
	return record, nil
}

func runnerRuntimeOwnershipSHA256(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func runnerReadinessProofMatches(
	proof RunnerReadinessProofRecord,
	task TaskRecord,
	runner RunnerRecord,
	ownership *etcdstore.KeyValue,
) bool {
	return ownership != nil && proof.RunnerID == runner.Desired.ID && proof.TaskID == task.ID &&
		proof.RuntimeEpoch == runner.RuntimeEpoch && proof.ContainerID == runner.ContainerID &&
		proof.RuntimeOwnershipRevision == ownership.ModRevision &&
		proof.RuntimeOwnershipSHA256 == runnerRuntimeOwnershipSHA256(ownership.Value)
}

func runnerReadinessProofKey(taskID string) string { return runnerReadinessProofPrefix + taskID }
