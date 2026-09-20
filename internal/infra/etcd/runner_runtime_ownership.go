package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) AttestRunnerRuntimeOwnership(
	ctx context.Context,
	current Versioned[RunnerRecord],
	containerID string,
	ownership RunnerRuntimeOwnershipRecord,
) (Versioned[RunnerRuntimeOwnershipRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || validateRunnerRecord(current.Record) != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner attestation target is invalid",
		)
	}
	replacement, err := BindRunnerContainerID(current.Record, current.Record.CreateTaskID, containerID)
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	if validateRunnerRuntimeOwnership(ownership) != nil || ownership.RunnerID != current.Record.Desired.ID ||
		ownership.RuntimeEpoch != replacement.RuntimeEpoch {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner runtime ownership does not match its lifecycle",
		)
	}
	lifecycleValue, err := encodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	defer clear(lifecycleValue)
	ownershipValue, err := encodeRunnerRuntimeOwnership(ownership)
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	defer clear(ownershipValue)
	conditions := []etcdstore.Condition{
		{Key: runnerKey(ownership.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(ownership.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(ownership.RunnerID)},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), ownership.RunnerID)},
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(ownership.RunnerID), Value: lifecycleValue},
		{Type: etcdstore.MutationPut, Key: runnerRuntimeOwnershipKey(ownership.RunnerID), Value: ownershipValue},
	})
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	if result.Succeeded {
		return Versioned[RunnerRuntimeOwnershipRecord]{
			Record: ownership, Revision: result.Revision, ReadRevision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindInternal,
			"runner attestation compare evidence is incomplete",
		)
	}
	if result.FailureReads[3] != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindResourceInUse,
			"runner deletion is in progress",
		)
	}
	if result.FailureReads[1] != nil && result.FailureReads[2] != nil &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[1].Value, lifecycleValue) &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[2].Value, ownershipValue) {
		stored, decodeErr := decodeRunnerRuntimeOwnership(result.FailureReads[2].Value)
		if decodeErr != nil {
			return Versioned[RunnerRuntimeOwnershipRecord]{}, decodeErr
		}
		return Versioned[RunnerRuntimeOwnershipRecord]{
			Record: stored, Revision: result.FailureReads[2].ModRevision, ReadRevision: result.Revision,
		}, nil
	}
	return Versioned[RunnerRuntimeOwnershipRecord]{}, stateConflict("runner runtime ownership", ownership.RunnerID)
}

func (repository *RunnerRepository) GetRunnerRuntimeOwnership(
	ctx context.Context,
	runnerID string,
) (Versioned[RunnerRuntimeOwnershipRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, runnerID); err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, runnerRuntimeOwnershipKey(runnerID))
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if result == nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner runtime ownership read is missing",
		)
	}
	if result.Entry == nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeRunnerRuntimeOwnership(result.Entry.Value)
	if err != nil || record.RunnerID != runnerID {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner runtime ownership is corrupt",
		)
	}
	return Versioned[RunnerRuntimeOwnershipRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *RunnerRepository) BeginFailedRunnerRuntimeCleanup(
	ctx context.Context,
	current Versioned[RunnerRecord],
) (Versioned[RunnerRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || validateRunnerRecord(current.Record) != nil ||
		current.Record.ProvisioningState != RunnerProvisioningFailed || current.Record.ContainerID == "" {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindValidationFailed, "failed runner cleanup target is invalid")
	}
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		runnerRuntimeOwnershipKey(current.Record.Desired.ID),
		deletionTombstoneKey(string(DeletionTargetRunner), current.Record.Desired.ID),
	}, Revision: current.ReadRevision})
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if evidence == nil || len(evidence.Values) != 2 || evidence.Values[0] == nil || evidence.Values[1] != nil {
		return Versioned[RunnerRecord]{}, errs.New(
			errs.KindStateConflict,
			"failed runner cleanup ownership is unavailable",
		)
	}
	ownership, err := decodeRunnerRuntimeOwnership(evidence.Values[0].Value)
	if err != nil || ownership.RunnerID != current.Record.Desired.ID ||
		ownership.RuntimeEpoch != current.Record.RuntimeEpoch {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindStateConflict, "failed runner cleanup ownership changed")
	}
	replacement, err := TakeRunnerRuntimeCleanupOwnership(current.Record)
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	lifecycleValue, err := encodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	defer clear(lifecycleValue)
	conditions := []etcdstore.Condition{
		{Key: runnerKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(current.Record.Desired.ID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(current.Record.Desired.ID), ModRevision: evidence.Values[0].ModRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), current.Record.Desired.ID)},
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: runnerLifecycleKey(current.Record.Desired.ID), Value: lifecycleValue,
	}})
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if result.Succeeded {
		replacement.LifecycleRevision = result.Revision
		return Versioned[RunnerRecord]{
			Record: replacement, Revision: current.Revision, ReadRevision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return Versioned[RunnerRecord]{}, errs.New(
			errs.KindInternal,
			"failed runner cleanup compare evidence is incomplete",
		)
	}
	if result.FailureReads[1] != nil && result.FailureReads[2] != nil && result.FailureReads[3] == nil &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[1].Value, lifecycleValue) &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[2].Value, evidence.Values[0].Value) {
		replacement.LifecycleRevision = result.FailureReads[1].ModRevision
		return Versioned[RunnerRecord]{
			Record: replacement, Revision: current.Revision, ReadRevision: result.Revision,
		}, nil
	}
	return Versioned[RunnerRecord]{}, stateConflict("failed runner cleanup ownership", current.Record.Desired.ID)
}

func (repository *RunnerRepository) DeleteRunnerRuntimeOwnershipAfterCleanup(
	ctx context.Context,
	current Versioned[RunnerRecord],
	expected RunnerRuntimeOwnershipRecord,
) (int64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || validateRunnerRecord(current.Record) != nil ||
		validateRunnerRuntimeOwnership(expected) != nil || expected.RunnerID != current.Record.Desired.ID ||
		current.Record.ContainerID == "" || current.Record.RuntimeEpoch <= expected.RuntimeEpoch {
		return 0, errs.New(errs.KindValidationFailed, "runner runtime cleanup proof does not match its lifecycle")
	}
	expectedValue, err := encodeRunnerRuntimeOwnership(expected)
	if err != nil {
		return 0, err
	}
	defer clear(expectedValue)
	lifecycleValue, err := encodeRunnerLifecycleRecord(current.Record.RunnerLifecycleRecord)
	if err != nil {
		return 0, err
	}
	defer clear(lifecycleValue)
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		runnerLifecycleKey(expected.RunnerID),
		runnerRuntimeOwnershipKey(expected.RunnerID),
		deletionTombstoneKey(string(DeletionTargetRunner), expected.RunnerID),
	}, Revision: current.ReadRevision})
	if err != nil {
		return 0, err
	}
	if evidence == nil || len(evidence.Values) != 3 || evidence.Values[0] == nil || evidence.Values[1] == nil ||
		evidence.Values[0].ModRevision != current.Record.LifecycleRevision ||
		!sameRunnerRuntimeOwnershipBytes(evidence.Values[0].Value, lifecycleValue) ||
		!sameRunnerRuntimeOwnershipBytes(evidence.Values[1].Value, expectedValue) {
		return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup ownership changed")
	}
	if evidence.Values[2] != nil {
		tombstone, decodeErr := decodeRunnerDeletionTombstone(evidence.Values[2].Value)
		if decodeErr != nil || tombstone.TargetID != expected.RunnerID {
			return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup tombstone changed")
		}
	} else if current.Record.ProvisioningState != RunnerProvisioningFailed {
		return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup lacks deletion ownership")
	}
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: runnerKey(expected.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(expected.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(expected.RunnerID), ModRevision: evidence.Values[1].ModRevision},
		{
			Key:         deletionTombstoneKey(string(DeletionTargetRunner), expected.RunnerID),
			ModRevision: keyValueRevision(evidence.Values[2]),
		},
	}, []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: runnerRuntimeOwnershipKey(expected.RunnerID)}})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, stateConflict("runner runtime cleanup ownership", expected.RunnerID)
	}
	return result.Revision, nil
}
