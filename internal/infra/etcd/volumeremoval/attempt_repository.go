package volumeremoval

import (
	"bytes"
	"context"
	"crypto/sha256"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"

	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentVolumeRemovalRuntimeRepository) ReplayRootResponse(
	ctx context.Context,
	operationID string,
	locator idempotencyrecord.IdempotencyLocator,
	intentSHA256 [sha256.Size]byte,
) (string, bool, error) {
	state, err := repository.Resume(ctx, operationID)
	if err != nil {
		return "", false, err
	}
	if volumeRemovalRootLocator(state.Runtime.Record) != locator || state.Runtime.Record.IntentSHA256 != intentSHA256 {
		return "", false, errs.New(
			errs.KindIdempotencyMismatch,
			"Environment Volume removal idempotency intent changed",
		)
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(locator)
	if err != nil {
		return "", false, removalrecord.Corrupt()
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{markerKey}, Revision: state.Runtime.ReadRevision,
	})
	if err != nil {
		return "", false, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return "", false, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal root replay marker is missing",
		)
	}
	defer clearKeyValues(read.Values)
	if err := validateEnvironmentVolumeRemovalRootMarker(
		read.Values[0].Value, state.Runtime.Record,
	); err != nil {
		return "", false, err
	}
	return state.Runtime.Record.OriginTaskID, true, nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) loadAssignmentFence(
	ctx context.Context,
	input EnvironmentVolumeRemovalAssignment,
	revision int64,
	runtime removalrecord.Runtime,
	attempt removalrecord.Attempt,
) ([]etcdstore.Condition, error) {
	_, conditions, err := repository.loadAssignedTask(ctx, input, revision, runtime, attempt)
	return conditions, err
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) loadAssignedTask(
	ctx context.Context,
	input EnvironmentVolumeRemovalAssignment,
	revision int64,
	runtime removalrecord.Runtime,
	attempt removalrecord.Attempt,
) (etcd.TaskRecord, []etcdstore.Condition, error) {
	if input.OperationID != runtime.OperationID || input.TaskID != runtime.CurrentTaskID ||
		ids.Validate(ids.KindAssignment, input.AssignmentID) != nil ||
		ids.Validate(ids.KindAgent, input.AgentID) != nil || input.AgentGeneration == 0 || revision <= 0 {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Environment Volume removal assignment is invalid",
		)
	}
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{taskjournal.TaskStorageKey(input.TaskID), taskjournal.TaskAssignmentIndexKey(input.TaskID)},
		Revision: revision,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	if primary == nil || primary.ReadRevision != revision || len(primary.Values) != 2 ||
		primary.Values[0] == nil || primary.Values[1] == nil {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal assignment changed",
		)
	}
	defer clearKeyValues(primary.Values)
	task, err := etcd.DecodeTaskRecord(primary.Values[0].Value)
	if err != nil || task.ID != input.TaskID || task.Status != taskjournal.TaskStatusRunning ||
		validateEnvironmentVolumeRemovalTask(task, runtime, attempt) != nil {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal Task changed",
		)
	}
	assignment, err := taskassignments.DecodeTaskAssignment(primary.Values[1].Value)
	if err != nil || assignment.AssignmentID != input.AssignmentID ||
		assignment.TaskID != input.TaskID || assignment.Executor != taskjournal.TaskExecutorAgent ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal assignment changed",
		)
	}
	claimKey := taskjournal.TaskExecutionClaimKey(taskjournal.TaskExecutorAgent, input.AgentID, input.TaskID)
	claim, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{claimKey, taskjournal.TaskTimeoutIndexKey(input.TaskID, assignment.Deadline)},
		Revision: revision,
	})
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	if claim == nil || claim.ReadRevision != revision || len(claim.Values) != 2 ||
		claim.Values[0] == nil || claim.Values[1] == nil ||
		claim.Values[0].ModRevision != primary.Values[1].ModRevision ||
		claim.Values[1].ModRevision != primary.Values[1].ModRevision ||
		!bytes.Equal(claim.Values[0].Value, primary.Values[1].Value) ||
		!bytes.Equal(claim.Values[1].Value, primary.Values[1].Value) {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment Volume removal assignment copies changed",
		)
	}
	defer clearKeyValues(claim.Values)
	ownerFences, err := repository.loadOwnerFences(ctx, runtime, revision)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	return task, append(ownerFences, []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(input.TaskID), ModRevision: primary.Values[0].ModRevision},
		{Key: taskjournal.TaskAssignmentIndexKey(input.TaskID), ModRevision: primary.Values[1].ModRevision},
		{Key: claimKey, ModRevision: claim.Values[0].ModRevision},
		{
			Key:         taskjournal.TaskTimeoutIndexKey(input.TaskID, assignment.Deadline),
			ModRevision: claim.Values[1].ModRevision,
		},
	}...), nil
}

func (repository *EnvironmentVolumeRemovalRuntimeRepository) loadOwnerFences(
	ctx context.Context,
	runtime removalrecord.Runtime,
	revision int64,
) ([]etcdstore.Condition, error) {
	keys := []string{removalrecord.OwnerKey(runtime.VolumeID), removalrecord.EnvironmentLockKey(runtime.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindStateConflict, "volume removal ownership evidence is missing")
	}
	defer clearKeyValues(read.Values)
	conditions := make([]etcdstore.Condition, len(keys))
	for index, key := range keys {
		value := read.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 {
			return nil, errs.New(errs.KindStateConflict, "volume removal ownership is missing")
		}
		owner, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || owner.VolumeID != runtime.VolumeID || owner.EnvironmentID != runtime.EnvironmentID ||
			owner.OperationID != runtime.OperationID {
			return nil, errs.New(errs.KindStateConflict, "volume removal ownership changed")
		}
		conditions[index] = etcdstore.Condition{Key: key, ModRevision: value.ModRevision}
	}
	return conditions, nil
}

func validateEnvironmentVolumeRemovalRootMarker(
	value []byte,
	runtime removalrecord.Runtime,
) error {
	marker, err := idempotencyrecord.DecodeIdempotencyMarker(value, volumeRemovalRootLocator(runtime))
	if err != nil || marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != runtime.OriginTaskID || marker.Locator != volumeRemovalRootLocator(runtime) ||
		marker.Response.Status != http.StatusAccepted ||
		sha256.Sum256(marker.Response.Body) != runtime.RootResponseSHA256 {
		return errs.New(
			errs.KindStateConflict,
			"Environment Volume removal root replay evidence changed",
		)
	}
	return nil
}

func validateEnvironmentVolumeRemovalTransaction(
	store store,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	maximumOperations int,
) error {
	operations := len(conditions) + len(mutations)
	if operations == 0 || operations > maximumOperations {
		return errs.New(errs.KindInternal, "Environment Volume removal transaction operation budget exceeded")
	}
	valueBytes := 0
	for _, condition := range conditions {
		if len(condition.Key) == 0 || len(condition.Key) > maximumKeyBytes {
			return errs.New(errs.KindInternal, "Environment Volume removal transaction key is invalid")
		}
	}
	for _, mutation := range mutations {
		if len(mutation.Key) == 0 || len(mutation.Key) > maximumKeyBytes {
			return errs.New(errs.KindInternal, "Environment Volume removal transaction key is invalid")
		}
		if mutation.Type == etcdstore.MutationPut {
			valueBytes += len(mutation.Value)
		}
	}
	const maximumBytes = 900 * 1024
	if valueBytes+operations*maximumKeyBytes+operations*64+128 > maximumBytes {
		return errs.New(errs.KindInternal, "Environment Volume removal transaction byte budget exceeded")
	}
	if sizer, ok := store.(transactionSizer); ok {
		actual, err := sizer.TransactionSize(conditions, mutations)
		if err != nil {
			return err
		}
		if actual > maximumBytes {
			return errs.New(errs.KindInternal, "Environment Volume removal encoded transaction is oversized")
		}
	}
	return nil
}

func sameEnvironmentVolumeRemovalRuntime(
	left removalrecord.Runtime,
	right removalrecord.Runtime,
) bool {
	return left.OperationID == right.OperationID && left.EnvironmentID == right.EnvironmentID &&
		left.VolumeID == right.VolumeID && left.Key == right.Key &&
		left.DesiredRevisionID == right.DesiredRevisionID && left.DesiredGeneration == right.DesiredGeneration &&
		left.ImpactSHA256 == right.ImpactSHA256 && left.EvidenceManifestSHA256 == right.EvidenceManifestSHA256 &&
		left.IntentSHA256 == right.IntentSHA256 && left.RootLocator == right.RootLocator &&
		left.RootResponseSHA256 == right.RootResponseSHA256 && left.OriginTaskID == right.OriginTaskID &&
		left.CurrentTaskID == right.CurrentTaskID && left.PredecessorTaskID == right.PredecessorTaskID &&
		left.AttemptOrdinal == right.AttemptOrdinal && left.StepID == right.StepID &&
		left.Checkpoint == right.Checkpoint && left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}

func sameEnvironmentVolumeRemovalProgress(
	left removalrecord.Progress,
	right removalrecord.Progress,
) bool {
	return left.OperationID == right.OperationID && left.NextRequestOrdinal == right.NextRequestOrdinal &&
		left.DirectoryAbsent == right.DirectoryAbsent && left.UpdatedAt.Equal(right.UpdatedAt) &&
		equalVolumeRemovalStrings(left.ComponentStack, right.ComponentStack) && bytes.Equal(left.Cursor, right.Cursor)
}

func sameEnvironmentVolumeRemovalPendingPath(
	left removalrecord.PendingPath,
	right removalrecord.PendingPath,
) bool {
	return left.OperationID == right.OperationID && left.VolumeID == right.VolumeID && left.Key == right.Key &&
		left.IntentSHA256 == right.IntentSHA256 && left.RequestOrdinal == right.RequestOrdinal &&
		left.MutationBudget == right.MutationBudget && left.RequestSHA256 == right.RequestSHA256 &&
		left.TaskID == right.TaskID && left.AssignmentID == right.AssignmentID && left.AgentID == right.AgentID &&
		left.AgentGeneration == right.AgentGeneration && left.CreatedAt.Equal(right.CreatedAt) &&
		equalVolumeRemovalStrings(left.ComponentStack, right.ComponentStack) && bytes.Equal(left.Cursor, right.Cursor)
}

func sameEnvironmentVolumeRemovalCompletion(
	left removalrecord.Completion,
	right removalrecord.Completion,
) bool {
	return left.OperationID == right.OperationID && left.RequestOrdinal == right.RequestOrdinal &&
		left.RequestSHA256 == right.RequestSHA256 && left.ResponseSHA256 == right.ResponseSHA256 &&
		left.ResponseBytes == right.ResponseBytes &&
		left.MutationCount == right.MutationCount && left.DirectoryAbsent == right.DirectoryAbsent &&
		left.CompletedAt.Equal(right.CompletedAt) &&
		equalVolumeRemovalStrings(left.NextComponentStack, right.NextComponentStack) &&
		bytes.Equal(left.NextCursor, right.NextCursor)
}

func equalVolumeRemovalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
