package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) AttestRunnerRuntimeOwnership(
	ctx context.Context,
	current etcdstore.Versioned[runnerrecord.RunnerRecord],
	containerID string,
	ownership runnerrecord.RunnerRuntimeOwnershipRecord,
) (etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || runnerrecord.ValidateRunnerRecord(current.Record) != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner attestation target is invalid",
		)
	}
	replacement, err := runnerrecord.BindRunnerContainerID(current.Record, current.Record.CreateTaskID, containerID)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, err
	}
	if runnerrecord.ValidateRunnerRuntimeOwnership(ownership) != nil || ownership.RunnerID != current.Record.Desired.ID ||
		ownership.RuntimeEpoch != replacement.RuntimeEpoch {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner runtime ownership does not match its lifecycle",
		)
	}
	lifecycleValue, err := runnerrecord.EncodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, err
	}
	defer clear(lifecycleValue)
	ownershipValue, err := runnerrecord.EncodeRunnerRuntimeOwnership(ownership)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, err
	}
	defer clear(ownershipValue)
	conditions := []etcdstore.Condition{
		{Key: runnerKey(ownership.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(ownership.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(ownership.RunnerID)},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), ownership.RunnerID)},
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(ownership.RunnerID), Value: lifecycleValue},
		{Type: etcdstore.MutationPut, Key: runnerRuntimeOwnershipKey(ownership.RunnerID), Value: ownershipValue},
	})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, err
	}
	if result.Succeeded {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{
			Record: ownership, Revision: result.Revision, ReadRevision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindInternal,
			"runner attestation compare evidence is incomplete",
		)
	}
	if result.FailureReads[3] != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, errs.New(
			errs.KindResourceInUse,
			"runner deletion is in progress",
		)
	}
	if result.FailureReads[1] != nil && result.FailureReads[2] != nil &&
		runnerrecord.SameRunnerRuntimeOwnershipBytes(result.FailureReads[1].Value, lifecycleValue) &&
		runnerrecord.SameRunnerRuntimeOwnershipBytes(result.FailureReads[2].Value, ownershipValue) {
		stored, decodeErr := runnerrecord.DecodeRunnerRuntimeOwnership(result.FailureReads[2].Value)
		if decodeErr != nil {
			return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, decodeErr
		}
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{
			Record: stored, Revision: result.FailureReads[2].ModRevision, ReadRevision: result.Revision,
		}, nil
	}
	return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, stateConflict("runner runtime ownership", ownership.RunnerID)
}

func (repository *RunnerRepository) GetRunnerRuntimeOwnership(
	ctx context.Context,
	runnerID string,
) (etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, runnerID); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, runnerRuntimeOwnershipKey(runnerID))
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner runtime ownership read is missing",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := runnerrecord.DecodeRunnerRuntimeOwnership(result.Entry.Value)
	if err != nil || record.RunnerID != runnerID {
		return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner runtime ownership is corrupt",
		)
	}
	return etcdstore.Versioned[runnerrecord.RunnerRuntimeOwnershipRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *RunnerRepository) BeginFailedRunnerRuntimeCleanup(
	ctx context.Context,
	current etcdstore.Versioned[runnerrecord.RunnerRecord],
) (etcdstore.Versioned[runnerrecord.RunnerRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || runnerrecord.ValidateRunnerRecord(current.Record) != nil ||
		current.Record.ProvisioningState != runnerrecord.RunnerProvisioningFailed || current.Record.ContainerID == "" {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindValidationFailed, "failed runner cleanup target is invalid")
	}
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		runnerRuntimeOwnershipKey(current.Record.Desired.ID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), current.Record.Desired.ID),
	}, Revision: current.ReadRevision})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if evidence == nil || len(evidence.Values) != 2 || evidence.Values[0] == nil || evidence.Values[1] != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(
			errs.KindStateConflict,
			"failed runner cleanup ownership is unavailable",
		)
	}
	ownership, err := runnerrecord.DecodeRunnerRuntimeOwnership(evidence.Values[0].Value)
	if err != nil || ownership.RunnerID != current.Record.Desired.ID ||
		ownership.RuntimeEpoch != current.Record.RuntimeEpoch {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(errs.KindStateConflict, "failed runner cleanup ownership changed")
	}
	replacement, err := runnerrecord.TakeRunnerRuntimeCleanupOwnership(current.Record)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	lifecycleValue, err := runnerrecord.EncodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	defer clear(lifecycleValue)
	conditions := []etcdstore.Condition{
		{Key: runnerKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(current.Record.Desired.ID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(current.Record.Desired.ID), ModRevision: evidence.Values[0].ModRevision},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), current.Record.Desired.ID)},
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: runnerLifecycleKey(current.Record.Desired.ID), Value: lifecycleValue,
	}})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, err
	}
	if result.Succeeded {
		replacement.LifecycleRevision = result.Revision
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{
			Record: replacement, Revision: current.Revision, ReadRevision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, errs.New(
			errs.KindInternal,
			"failed runner cleanup compare evidence is incomplete",
		)
	}
	if result.FailureReads[1] != nil && result.FailureReads[2] != nil && result.FailureReads[3] == nil &&
		runnerrecord.SameRunnerRuntimeOwnershipBytes(result.FailureReads[1].Value, lifecycleValue) &&
		runnerrecord.SameRunnerRuntimeOwnershipBytes(result.FailureReads[2].Value, evidence.Values[0].Value) {
		replacement.LifecycleRevision = result.FailureReads[1].ModRevision
		return etcdstore.Versioned[runnerrecord.RunnerRecord]{
			Record: replacement, Revision: current.Revision, ReadRevision: result.Revision,
		}, nil
	}
	return etcdstore.Versioned[runnerrecord.RunnerRecord]{}, stateConflict("failed runner cleanup ownership", current.Record.Desired.ID)
}

func (repository *RunnerRepository) DeleteRunnerRuntimeOwnershipAfterCleanup(
	ctx context.Context,
	current etcdstore.Versioned[runnerrecord.RunnerRecord],
	expected runnerrecord.RunnerRuntimeOwnershipRecord,
) (int64, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || runnerrecord.ValidateRunnerRecord(current.Record) != nil ||
		runnerrecord.ValidateRunnerRuntimeOwnership(expected) != nil || expected.RunnerID != current.Record.Desired.ID ||
		current.Record.ContainerID == "" || current.Record.RuntimeEpoch <= expected.RuntimeEpoch {
		return 0, errs.New(errs.KindValidationFailed, "runner runtime cleanup proof does not match its lifecycle")
	}
	expectedValue, err := runnerrecord.EncodeRunnerRuntimeOwnership(expected)
	if err != nil {
		return 0, err
	}
	defer clear(expectedValue)
	lifecycleValue, err := runnerrecord.EncodeRunnerLifecycleRecord(current.Record.RunnerLifecycleRecord)
	if err != nil {
		return 0, err
	}
	defer clear(lifecycleValue)
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		runnerLifecycleKey(expected.RunnerID),
		runnerRuntimeOwnershipKey(expected.RunnerID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), expected.RunnerID),
	}, Revision: current.ReadRevision})
	if err != nil {
		return 0, err
	}
	if evidence == nil || len(evidence.Values) != 3 || evidence.Values[0] == nil || evidence.Values[1] == nil ||
		evidence.Values[0].ModRevision != current.Record.LifecycleRevision ||
		!runnerrecord.SameRunnerRuntimeOwnershipBytes(evidence.Values[0].Value, lifecycleValue) ||
		!runnerrecord.SameRunnerRuntimeOwnershipBytes(evidence.Values[1].Value, expectedValue) {
		return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup ownership changed")
	}
	if evidence.Values[2] != nil {
		tombstone, decodeErr := decodeRunnerDeletionTombstone(evidence.Values[2].Value)
		if decodeErr != nil || tombstone.TargetID != expected.RunnerID {
			return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup tombstone changed")
		}
	} else if current.Record.ProvisioningState != runnerrecord.RunnerProvisioningFailed {
		return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup lacks deletion ownership")
	}
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: runnerKey(expected.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(expected.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(expected.RunnerID), ModRevision: evidence.Values[1].ModRevision},
		{
			Key:         deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), expected.RunnerID),
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
