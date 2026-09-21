package etcd

import (
	"context"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetryRunnerCreationWithTask is deliberately separate from generic Task
// retry because every attempt must fetch a fresh single-use registration token.
// Only the fact that a fresh token was present crosses this persistence seam.
func (repository *RunnerRepository) RetryRunnerCreationWithTask(
	ctx context.Context,
	sourceTaskID string,
	retry TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if ids.Validate(ids.KindTask, sourceTaskID) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "source task id is invalid")
	}
	if retry.RetryOf != sourceTaskID || retry.Executor != taskjournal.TaskExecutorController || retry.Type != taskjournal.TaskCreate ||
		retry.Status != taskjournal.TaskStatusPending || retry.IdempotencyKey == "" || len(retry.Params) != 2 ||
		retry.Params[taskjournal.TaskResourceKindParam] != TaskResourceRunner ||
		retry.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"runner creation retry has invalid durable input",
		)
	}
	if err := validateRunnerRetryMarkerEnvelope(retry, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	retry = bindRunnerTaskMarker(retry, marker)
	if err := validateTaskRecord(retry); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	existing, err := idempotency.Read(ctx, marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing != nil {
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionExisting, revision: existing.modRevision,
			marker: idempotencyrecord.CloneIdempotencyMarker(existing.marker),
		}, nil
	}
	sourceResult, err := repository.store.Get(ctx, taskjournal.TaskStorageKey(sourceTaskID))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if sourceResult == nil || sourceResult.Entry == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindTaskNotFound, "source task was not found")
	}
	source, err := decodeTaskRecord(sourceResult.Entry.Value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	applies, err := taskOwnsRunnerCreation(source)
	if err != nil || !applies {
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"source task is not a runner creation",
		)
	}
	if !runnerRetryableTerminal(source.Status) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"source runner creation task is not retryable",
		)
	}
	current, err := repository.GetRunner(ctx, source.Target)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if retry.RetryOf != source.ID || retry.OperationID != source.OperationID ||
		retry.Owner != source.Owner ||
		retry.Executor != taskjournal.TaskExecutorController || retry.Type != taskjournal.TaskCreate ||
		retry.Target != source.Target || retry.Status != taskjournal.TaskStatusPending ||
		retry.IdempotencyKey != source.IdempotencyKey || retry.PlanID != source.PlanID ||
		retry.PlanHash != source.PlanHash || retry.RenderGeneration != source.RenderGeneration ||
		retry.TimeoutSeconds != source.TimeoutSeconds || !runnerTaskStepsEqual(retry.Steps, source.Steps) ||
		!runnerTaskMaterializationsEqual(retry.Materializations, source.Materializations) ||
		len(retry.Params) != 2 || retry.Params[taskjournal.TaskResourceKindParam] != TaskResourceRunner ||
		retry.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"runner creation retry has invalid durable input",
		)
	}
	if err := validateRunnerRetryMarker(current.Record.Desired, retry, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement, err := runnerrecord.RetryRunnerProvisioning(current.Record, source.ID, retry.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	parents, err := repository.resolveRunnerParents(ctx, current.Record.Desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	allocation, err := repository.readRunnerAllocationEvidence(ctx, current.Record, current.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	recordValue, err := runnerrecord.EncodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(recordValue)
	taskValue, err := encodeTaskRecord(retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(retry.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(retry.ID)},
		{Key: taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID)},
		{Key: taskjournal.TaskActiveOperationKey(retry.OperationID)},
		{Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID)},
		{Key: taskjournal.TaskStorageKey(source.ID), ModRevision: sourceResult.Entry.ModRevision},
		{Key: runnerKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(current.Record.Desired.ID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(current.Record.Desired.ID)},
		{
			Key: runnerOwnerKey(
				current.Record.Desired.OwnerKind,
				current.Record.Desired.OwnerID,
				current.Record.Desired.ID,
			),
			ModRevision: allocation.owner.ModRevision,
		},
		{
			Key:         runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug),
			ModRevision: allocation.slug.ModRevision,
		},
		{Key: runnerrecord.RunnerTenantQuotaKey(current.Record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
		{Key: runnerrecord.RunnerHostSlotKey(current.Record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
		{Key: runnerrecord.SystemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
		{Key: hierarchyrecord.TenantKey(current.Record.Desired.TenantID), ModRevision: parents.tenant.Revision},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), current.Record.Desired.ID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), current.Record.Desired.TenantID)},
	}
	if current.Record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		conditions = append(conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(current.Record.Desired.OwnerID), ModRevision: parents.project.Revision},
			etcdstore.Condition{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), current.Record.Desired.OwnerID)},
		)
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(retry.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(current.Record.Desired.ID), Value: recordValue},
	}
	classifier := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "runner creation retry compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, decodeErr := idempotencyrecord.DecodeTaskReference(values[2].Value)
			if decodeErr != nil {
				return decodeErr
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				retry.OperationID,
				activeTaskID,
			)
		}
		return recordcodec.StateConflict("runner creation retry", current.Record.Desired.ID)
	}
	initiation, err := newInheritedTaskInitiation(etcdstore.Versioned[TaskRecord]{
		Record: source, Revision: sourceResult.Entry.ModRevision, ReadRevision: sourceResult.ReadRevision,
	}, retry.Actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(retry, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func runnerTaskStepsEqual(left []taskjournal.TaskStepRecord, right []taskjournal.TaskStepRecord) bool {
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

func runnerTaskMaterializationsEqual(left []materializationrecord.Record, right []materializationrecord.Record) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !runnerTaskMaterializationEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func runnerTaskMaterializationEqual(left materializationrecord.Record, right materializationrecord.Record) bool {
	if left.StepID != right.StepID || left.MaterializationID != right.MaterializationID ||
		left.EnvironmentID != right.EnvironmentID || left.Destination != right.Destination ||
		left.ServiceID != right.ServiceID || left.ServiceName != right.ServiceName ||
		left.OutputKind != right.OutputKind || left.UID != right.UID || left.GID != right.GID ||
		left.Mode != right.Mode || left.Length != right.Length || left.SHA256 != right.SHA256 ||
		left.Source.Kind != right.Source.Kind ||
		!runnerComparablePointersEqual(left.Source.BlueprintFile, right.Source.BlueprintFile) ||
		!runnerComparablePointersEqual(left.Source.ComponentFile, right.Source.ComponentFile) ||
		!runnerComparablePointersEqual(left.Source.EntryValue, right.Source.EntryValue) {
		return false
	}
	return runnerGeneratedEnvironmentPointersEqual(
		left.Source.GeneratedEnvironment, right.Source.GeneratedEnvironment,
	)
}

func runnerComparablePointersEqual[T comparable](left *T, right *T) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func runnerGeneratedEnvironmentPointersEqual(
	left *materializationrecord.GeneratedEnvironmentValueReference,
	right *materializationrecord.GeneratedEnvironmentValueReference,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.FormatVersion != right.FormatVersion || len(left.Values) != len(right.Values) {
		return false
	}
	for index := range left.Values {
		if left.Values[index] != right.Values[index] {
			return false
		}
	}
	return true
}
