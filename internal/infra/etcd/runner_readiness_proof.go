package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

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
) (etcdstore.Versioned[RunnerReadinessProofRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner readiness task id is invalid",
		)
	}
	keys := []string{
		taskjournal.TaskStorageKey(taskID),
		runnerReadinessProofKey(taskID),
	}
	initial, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	if initial == nil || len(initial.Values) != len(keys) || initial.Values[0] == nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness task is unavailable",
		)
	}
	task, err := decodeTaskRecord(initial.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	applies, err := taskOwnsRunnerCreation(task)
	if err != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	if !applies || task.ID != taskID || task.Status != taskjournal.TaskStatusRunning {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness task is not running",
		)
	}
	stateKeys := []string{
		runnerKey(task.Target),
		runnerLifecycleKey(task.Target),
		runnerRuntimeOwnershipKey(task.Target),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: stateKeys, Revision: initial.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	if state == nil || len(state.Values) != len(stateKeys) ||
		state.Values[0] == nil || state.Values[1] == nil || state.Values[2] == nil || state.Values[3] != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindStateConflict,
			"runner readiness evidence is incomplete",
		)
	}
	runner, err := decodeRunnerAggregate(state.Values[0], state.Values[1])
	if err != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	ownership, err := runnerrecord.DecodeRunnerRuntimeOwnership(state.Values[2].Value)
	if err != nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	if runner.Desired.ID != task.Target || runner.CreateTaskID != task.ID ||
		runner.ProvisioningState != runnerrecord.RunnerProvisioningProvisioning || runner.ContainerID == "" ||
		ownership.RunnerID != runner.Desired.ID || ownership.RuntimeEpoch != runner.RuntimeEpoch {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, errs.New(
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
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
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
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, err
	}
	if transaction.Succeeded {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{
			Record: proof, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
	if len(transaction.FailureReads) != len(conditions) {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, errs.New(
			errs.KindInternal,
			"runner readiness compare evidence is incomplete",
		)
	}
	for index := 0; index < len(conditions)-1; index++ {
		if keyValueRevision(transaction.FailureReads[index]) != conditions[index].ModRevision {
			return etcdstore.Versioned[RunnerReadinessProofRecord]{}, stateConflict("runner readiness", task.Target)
		}
	}
	existing := transaction.FailureReads[len(conditions)-1]
	if existing == nil {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, stateConflict("runner readiness", task.Target)
	}
	stored, err := decodeRunnerReadinessProof(existing.Value)
	if err != nil || stored != proof {
		return etcdstore.Versioned[RunnerReadinessProofRecord]{}, stateConflict("runner readiness", task.Target)
	}
	return etcdstore.Versioned[RunnerReadinessProofRecord]{
		Record: stored, Revision: existing.ModRevision, ReadRevision: transaction.Revision,
	}, nil
}

func validateRunnerReadinessProof(record RunnerReadinessProofRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || record.RuntimeEpoch == 0 ||
		!runnerrecord.ValidLowerHex(record.ContainerID, runnerrecord.RunnerContainerIDEncodedLength) ||
		record.RuntimeOwnershipRevision <= 0 || !runnerrecord.ValidLowerHex(record.RuntimeOwnershipSHA256, sha256.Size*2) {
		return errs.New(errs.KindValidationFailed, "runner readiness proof is invalid")
	}
	return nil
}

func encodeRunnerReadinessProof(record RunnerReadinessProofRecord) ([]byte, error) {
	if err := validateRunnerReadinessProof(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_readiness_proof", record)
}

func decodeRunnerReadinessProof(value []byte) (RunnerReadinessProofRecord, error) {
	if len(value) > runnerrecord.MaximumRunnerPersistenceBytes {
		return RunnerReadinessProofRecord{}, errs.New(errs.KindInternal, "runner readiness proof is corrupt")
	}
	record, err := recordcodec.Decode[RunnerReadinessProofRecord](value, "runner_readiness_proof")
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
	runner runnerrecord.RunnerRecord,
	ownership *etcdstore.KeyValue,
) bool {
	return ownership != nil && proof.RunnerID == runner.Desired.ID && proof.TaskID == task.ID &&
		proof.RuntimeEpoch == runner.RuntimeEpoch && proof.ContainerID == runner.ContainerID &&
		proof.RuntimeOwnershipRevision == ownership.ModRevision &&
		proof.RuntimeOwnershipSHA256 == runnerRuntimeOwnershipSHA256(ownership.Value)
}

func runnerReadinessProofKey(taskID string) string { return runnerReadinessProofPrefix + taskID }
