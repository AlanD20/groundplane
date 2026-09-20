package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"net/netip"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// hostResolutionReconciliationChange is deliberately a persistence-only
// contribution. It is joined to the owning Route/Component terminal
// transaction; no handler or scheduler writes the projection directly.
type hostResolutionReconciliationChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

const platformComponentTaskActivePrefix = "/v1/indexes/platform-component-tasks/by-component/"

func platformComponentTaskActiveKey(componentID string) string {
	return platformComponentTaskActivePrefix + componentID
}

func newPlatformDNSResolverTask(componentID string, createdAt time.Time) TaskRecord {
	return TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: PlatformTaskOwner(), Actor: TaskActorSystem, Executor: TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), RenderGeneration: 1, Type: TaskUpdate, Target: componentID,
		Params: map[string]string{
			TaskResourceKindParam:       TaskResourceComponent,
			TaskAutomaticReconcileParam: "true",
		},
		Steps: []TaskStepRecord{
			{Kind: TaskStepOperation, ID: ids.New(ids.KindStep)}, {Kind: TaskStepOperation, ID: ids.New(ids.KindStep)},
		}, TimeoutSeconds: 480,
		Status: TaskStatusPending, NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}

func platformResolverTaskSteps(input PlatformComponentTaskRenderInput) []TaskStepRecord {
	count := 2
	if input.EnsureService {
		count = 4
	} else if input.DisableService {
		count = 2
	}
	steps := make([]TaskStepRecord, count)
	for index := range steps {
		steps[index] = TaskStepRecord{Kind: TaskStepOperation, ID: ids.New(ids.KindStep)}
	}
	return steps
}

func isPlatformDNSResolverTask(task TaskRecord) bool {
	return task.Owner == PlatformTaskOwner() && task.Actor == TaskActorSystem &&
		task.Executor == TaskExecutorAgent && task.Type == TaskUpdate &&
		task.Params[TaskResourceKindParam] == TaskResourceComponent &&
		task.Params[TaskAutomaticReconcileParam] == "true" &&
		ids.Validate(ids.KindComponent, task.Target) == nil
}

func isPlatformDNSResolverTaskAttempt(task TaskRecord) bool {
	if task.Owner != PlatformTaskOwner() || task.Executor != TaskExecutorAgent || task.Type != TaskUpdate ||
		task.Params[TaskResourceKindParam] != TaskResourceComponent ||
		ids.Validate(ids.KindComponent, task.Target) != nil {
		return false
	}
	return task.Actor == TaskActorOperator ||
		task.Actor == TaskActorSystem && task.Params[TaskAutomaticReconcileParam] == "true"
}

// PublishPlatformDNSResolverTask publishes the first Platform-owned resolver
// Agent Task together with its pinned render input and projection. It is kept
// repository-private in spirit: callers must supply the already-rendered,
// typed input and cannot create a public DNS operation.
func (repository *TaskRepository) PublishPlatformDNSResolverTask(
	ctx context.Context,
	current Versioned[componentrecord.Record],
	projection HostResolutionProjectionRecord,
	task TaskRecord,
	renderInput PlatformComponentTaskRenderInput,
	marker IdempotencyMarker,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validatePlatformComponentRecord(current.Record); err != nil {
		return err
	}
	if task.Owner != PlatformTaskOwner() || task.Actor != TaskActorSystem || task.Executor != TaskExecutorAgent ||
		task.Type != TaskUpdate || task.Status != TaskStatusPending || task.Target != current.Record.Desired.ID ||
		task.Params[TaskResourceKindParam] != TaskResourceComponent ||
		task.Params[TaskAutomaticReconcileParam] != "true" || len(task.Params) != 2 {
		return errs.New(errs.KindValidationFailed, "platform DNS resolver Task shape is invalid")
	}
	if task.PlanID != renderInput.PlanID || task.ID != renderInput.TaskID || task.Target != renderInput.ComponentID ||
		task.PlanHash != renderInput.ExecutionPlanSHA256 ||
		renderInput.HostResolutionInputRevision != projection.InputRevision {
		return errs.New(errs.KindValidationFailed, "platform DNS resolver render input is not pinned")
	}
	desiredDigest, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		return err
	}
	if renderInput.DesiredSHA256 != desiredDigest {
		return errs.New(errs.KindValidationFailed, "platform DNS resolver desired state is not pinned")
	}
	task, err = bindPlatformComponentTaskMarker(task, marker)
	if err != nil {
		return err
	}
	task.Params[TaskPlatformComponentDesiredSHA256Param] = desiredDigest
	if err := validateTaskRecord(task); err != nil {
		return err
	}
	if err := validatePlatformComponentTaskRenderInput(renderInput); err != nil {
		return err
	}
	projectionValue, err := encodeHostResolutionProjectionRecord(projection)
	if err != nil {
		return err
	}
	defer clear(projectionValue)
	renderValue, err := encodePlatformComponentTaskRenderInput(renderInput)
	if err != nil {
		return err
	}
	defer clear(renderValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return err
	}
	defer clear(reference)
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformComponentOwnerKey(task.Target),
			platformComponentKindKey(current.Record.Desired.Kind),
			platformComponentBootstrapKey(task.Target),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return err
	}
	if indexes == nil || len(indexes.Values) != 3 || indexes.Values[0] == nil || indexes.Values[1] == nil {
		return errs.New(errs.KindInternal, "platform Component indexes are missing")
	}
	if indexes.Values[2] == nil || indexes.Values[2].ModRevision != current.Revision ||
		string(indexes.Values[2].Value) != task.Target {
		return errs.New(errs.KindStateConflict, "platform Component bootstrap provenance is unavailable")
	}
	conditions := []etcdstore.Condition{
		{Key: componentrecord.RecordKey(task.Target), ModRevision: current.Revision},
		{Key: platformComponentOwnerKey(task.Target), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: platformComponentBootstrapKey(task.Target), ModRevision: indexes.Values[2].ModRevision},
		{Key: hostResolutionProjectionKey},
		{Key: platformComponentTaskActiveKey(task.Target)},
		{Key: platformComponentTaskRenderInputKey(task.PlanID)},
		{Key: taskKey(task.ID)}, {Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)}, {Key: taskQueueKey(task.Executor, task.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: hostResolutionProjectionKey, Value: projectionValue},
		{Type: etcdstore.MutationPut, Key: platformComponentTaskRenderInputKey(task.PlanID), Value: renderValue},
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: platformComponentTaskActiveKey(task.Target), Value: []byte(task.ID)},
		{Type: etcdstore.MutationDelete, Key: platformComponentBootstrapKey(task.Target)},
	}
	initiation, err := newPlatformTaskInitiation(TaskActorSystem)
	if err != nil {
		return err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "platform DNS resolver Task publication conflicted")
		})
	if err != nil {
		return err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return err
	}
	result, err := idempotency.Apply(ctx, marker, plan)
	if err != nil {
		return err
	}
	return requireAppliedPlatformComponentTask(result)
}

// preparePlatformDNSResolverTaskContribution seals one automatic Task into
// the caller's transaction. A nil active value creates a new fence; a
// non-nil active value replaces the terminal Task's fence with its successor.
func (repository *TaskRepository) preparePlatformDNSResolverTaskContribution(
	ctx context.Context,
	current Versioned[componentrecord.Record],
	projection HostResolutionProjectionRecord,
	task TaskRecord,
	active *etcdstore.KeyValue,
	sealedPredecessor *TaskRecord,
	priorObservation *ComponentObservationRecord,
	baseConditions []etcdstore.Condition,
) (hostResolutionReconciliationChange, error) {
	if repository.platformResolverTaskPreparer == nil {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal, "platform resolver Task preparer is not configured",
		)
	}
	if active != nil && (active.Key != platformComponentTaskActiveKey(current.Record.Desired.ID) ||
		active.ModRevision <= 0 || string(active.Value) == "") {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"platform resolver active fence is corrupt",
		)
	}
	desiredSHA256, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	task = cloneTaskRecord(task)
	if task.RetryOf != "" {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindValidationFailed,
			"automatic platform resolver successor cannot be a retry",
		)
	}
	if task.Params == nil {
		task.Params = make(map[string]string, 2)
	}
	task.Params[TaskPlatformComponentDesiredSHA256Param] = desiredSHA256
	ensureService := len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy
	task.Steps = platformResolverTaskSteps(PlatformComponentTaskRenderInput{EnsureService: ensureService})
	var finalLineage *platformResolverLiveLineage
	if sealedPredecessor != nil {
		if active == nil || string(active.Value) != sealedPredecessor.ID {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor does not own the active fence",
			)
		}
		predecessorRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{taskKey(sealedPredecessor.ID)}, Revision: active.ModRevision,
		})
		if readErr != nil {
			return hostResolutionReconciliationChange{}, readErr
		}
		if predecessorRead == nil || len(predecessorRead.Values) != 1 || predecessorRead.Values[0] == nil {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor Task is missing",
			)
		}
		predecessor, decodeErr := decodeTaskRecord(predecessorRead.Values[0].Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		predecessorInputRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{platformComponentTaskRenderInputKey(predecessor.PlanID)}, Revision: active.ModRevision,
		})
		if readErr != nil {
			return hostResolutionReconciliationChange{}, readErr
		}
		if predecessorInputRead == nil || len(predecessorInputRead.Values) != 1 ||
			predecessorInputRead.Values[0] == nil {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor render input is missing",
			)
		}
		predecessorInput, decodeErr := decodePlatformComponentTaskRenderInput(predecessorInputRead.Values[0].Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		if sealedPredecessor == nil || predecessor.ID != sealedPredecessor.ID ||
			!platformResolverTaskInputBelongsToTask(predecessor, predecessorInput) ||
			predecessorInput.PlanID != predecessor.PlanID || predecessorInput.ComponentID != task.Target {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver predecessor lineage is corrupt",
			)
		}
		if sealedPredecessor.ID != predecessor.ID ||
			sealedPredecessor.PlanID != predecessor.PlanID || sealedPredecessor.Target != predecessor.Target {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver sealed predecessor is missing",
			)
		}
		lineage, proven := platformResolverFinalLiveLineage(*sealedPredecessor, predecessorInput)
		if !proven {
			return hostResolutionReconciliationChange{}, nil
		}
		finalLineage = &lineage
		priorObservation = platformResolverObservationForLineage(predecessorInput, lineage, priorObservation)
	}
	renderInput, err := repository.platformResolverTaskPreparer(ctx, current, projection, task, priorObservation)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if finalLineage != nil {
		renderInput.PriorObservationModRevision = finalLineage.priorObservationModRevision
		if renderInput.PriorObservationRevision != finalLineage.priorObservationRevision ||
			renderInput.PredecessorTaskID != finalLineage.predecessorTaskID ||
			renderInput.ExpectedPreviousArtifactSHA256 != finalLineage.expectedPreviousArtifactSHA256 ||
			renderInput.ExpectedPreviousArtifactID != finalLineage.expectedPreviousArtifactID ||
			renderInput.ExpectedPreviousGeneration != finalLineage.expectedPreviousGeneration {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver successor predecessor authority changed during planning",
			)
		}
	}
	task.PlanHash = renderInput.ExecutionPlanSHA256
	if err := validateTaskRecord(task); err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if task.Params[TaskPlatformComponentDesiredSHA256Param] != renderInput.DesiredSHA256 ||
		renderInput.HostResolutionInputRevision != projection.InputRevision ||
		renderInput.HostResolutionSHA256 != projection.InputSHA256 {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input is not pinned",
		)
	}
	if err := validatePlatformComponentTaskRenderInput(renderInput); err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if renderInput.TaskID != task.ID || renderInput.PlanID != task.PlanID || renderInput.ComponentID != task.Target {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input identity changed",
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformComponentOwnerKey(current.Record.Desired.ID),
			platformComponentKindKey(current.Record.Desired.Kind),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if indexes == nil || indexes.ReadRevision != current.ReadRevision || len(indexes.Values) != 2 ||
		indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(
			indexes.Values[0].Value,
		) != current.Record.Desired.ID || string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"platform resolver Component indexes are missing",
		)
	}
	renderValue, err := encodePlatformComponentTaskRenderInput(renderInput)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		clear(renderValue)
		return hostResolutionReconciliationChange{}, err
	}
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		clear(renderValue)
		clear(taskValue)
		return hostResolutionReconciliationChange{}, err
	}
	conditions := append([]etcdstore.Condition(nil), baseConditions...)
	for _, condition := range []etcdstore.Condition{
		{Key: componentrecord.RecordKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: platformComponentOwnerKey(current.Record.Desired.ID), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: platformComponentTaskRenderInputKey(task.PlanID)},
		{Key: taskKey(task.ID)}, {Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)}, {Key: taskQueueKey(task.Executor, task.ID)},
		{Key: taskWorkspacePlatformIndexKey(task.ID)},
	} {
		conditions = appendHostResolutionCondition(conditions, condition)
	}
	activeCondition := etcdstore.Condition{Key: platformComponentTaskActiveKey(current.Record.Desired.ID)}
	if active != nil {
		activeCondition.ModRevision = active.ModRevision
	}
	conditions = appendHostResolutionCondition(conditions, activeCondition)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: platformComponentTaskRenderInputKey(task.PlanID), Value: renderValue},
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskWorkspacePlatformIndexKey(task.ID), Value: []byte(task.ID)},
		{Type: etcdstore.MutationPut, Key: platformComponentTaskActiveKey(current.Record.Desired.ID), Value: []byte(task.ID)},
	}
	return hostResolutionReconciliationChange{
		applies: true, conditions: conditions, mutations: mutations,
		values: [][]byte{renderValue, taskValue, reference, []byte(task.ID)},
	}, nil
}

type platformResolverLiveLineage struct {
	priorObservationModRevision    int64
	priorObservationRevision       uint64
	predecessorTaskID              string
	expectedPreviousArtifactSHA256 string
	expectedPreviousArtifactID     string
	expectedPreviousGeneration     uint64
}

func platformResolverObservationForLineage(
	input PlatformComponentTaskRenderInput,
	lineage platformResolverLiveLineage,
	observation *ComponentObservationRecord,
) *ComponentObservationRecord {
	if observation != nil && observation.Revision == lineage.priorObservationRevision &&
		observation.TaskID == lineage.predecessorTaskID &&
		observation.CorefileSHA256 == lineage.expectedPreviousArtifactSHA256 &&
		(lineage.expectedPreviousArtifactSHA256 == "" || observation.DNSResolverProof != nil &&
			observation.DNSResolverProof.ArtifactID == lineage.expectedPreviousArtifactID &&
			observation.DNSResolverProof.RenderGeneration == lineage.expectedPreviousGeneration) {
		return observation
	}
	if lineage.expectedPreviousArtifactSHA256 == "" {
		return &ComponentObservationRecord{}
	}
	composeArtifact := input.ComposeArtifact
	if input.RollbackComposeArtifact != nil &&
		lineage.expectedPreviousArtifactID == input.ExpectedPreviousArtifactID {
		composeArtifact = input.RollbackComposeArtifact
	}
	return &ComponentObservationRecord{
		ComponentID: input.ComponentID, ServiceID: input.GeneratedServiceID,
		PlanID: input.OwnershipPlanID, ComposeArtifactID: input.ComposeArtifactID,
		Enabled: true, Healthy: true, RenderGeneration: lineage.expectedPreviousGeneration,
		OwnershipGeneration: input.OwnershipGeneration,
		CorefileSHA256:      lineage.expectedPreviousArtifactSHA256,
		TaskID:              lineage.predecessorTaskID, Revision: lineage.priorObservationRevision,
		DNSResolverProof: &TaskDNSResolverObservationEvidence{
			ComponentID: input.ComponentID, ServiceID: input.GeneratedServiceID,
			ArtifactID:       lineage.expectedPreviousArtifactID,
			ArtifactSHA256:   lineage.expectedPreviousArtifactSHA256,
			RenderGeneration: lineage.expectedPreviousGeneration,
		},
		ComposeArtifact: composeArtifact,
	}
}

func platformResolverFinalLiveLineage(
	predecessor TaskRecord,
	input PlatformComponentTaskRenderInput,
) (platformResolverLiveLineage, bool) {
	if predecessor.Result == nil || predecessor.Result.Kind != TaskResultCompose ||
		predecessor.Result.ReconciliationRequired {
		return platformResolverLiveLineage{}, false
	}
	result := predecessor.Result
	switch predecessor.Status {
	case TaskStatusCompleted:
		evidence := result.DNSResolverCandidateObservation
		if input.DisableService || evidence == nil || result.DNSResolverRollbackObservation != nil ||
			evidence.ComponentID != input.ComponentID || evidence.ServiceID != input.GeneratedServiceID ||
			evidence.ArtifactID != input.ArtifactID || evidence.ArtifactSHA256 != input.ArtifactSHA256 ||
			evidence.RenderGeneration != uint64(predecessor.RenderGeneration) {
			return platformResolverLiveLineage{}, false
		}
		return platformResolverLiveLineage{
			priorObservationRevision:       input.PriorObservationRevision + 1,
			predecessorTaskID:              predecessor.ID,
			expectedPreviousArtifactSHA256: evidence.ArtifactSHA256,
			expectedPreviousArtifactID:     evidence.ArtifactID,
			expectedPreviousGeneration:     evidence.RenderGeneration,
		}, true
	case TaskStatusFailed, TaskStatusTimedOut, TaskStatusAborted:
		evidence := result.DNSResolverRollbackObservation
		if input.ExpectedPreviousArtifactSHA256 == "" {
			if evidence != nil {
				return platformResolverLiveLineage{}, false
			}
		} else if evidence == nil || evidence.ComponentID != input.ComponentID ||
			evidence.ServiceID != input.GeneratedServiceID ||
			evidence.ArtifactID != input.ExpectedPreviousArtifactID ||
			evidence.ArtifactSHA256 != input.ExpectedPreviousArtifactSHA256 ||
			evidence.RenderGeneration != input.ExpectedPreviousGeneration {
			return platformResolverLiveLineage{}, false
		}
		return platformResolverLiveLineage{
			priorObservationModRevision:    input.PriorObservationModRevision,
			priorObservationRevision:       input.PriorObservationRevision,
			predecessorTaskID:              input.PredecessorTaskID,
			expectedPreviousArtifactSHA256: input.ExpectedPreviousArtifactSHA256,
			expectedPreviousArtifactID:     input.ExpectedPreviousArtifactID,
			expectedPreviousGeneration:     input.ExpectedPreviousGeneration,
		}, true
	default:
		return platformResolverLiveLineage{}, false
	}
}

func clearHostResolutionReconciliationChange(change hostResolutionReconciliationChange) {
	for _, value := range change.values {
		clear(value)
	}
}

// preparePlatformDNSResolverTaskRetry reuses the exact immutable plan input
// and only transfers the private active-attempt fence to the new Task. The
// input's TaskID remains the originating system attempt as durable provenance.
func (repository *TaskRepository) preparePlatformDNSResolverTaskRetry(
	ctx context.Context,
	source Versioned[TaskRecord],
	retry TaskRecord,
) (hostResolutionReconciliationChange, error) {
	if !isPlatformDNSResolverTaskAttempt(source.Record) {
		return hostResolutionReconciliationChange{}, nil
	}
	if source.ReadRevision <= 0 || source.Revision <= 0 || retry.RetryOf != source.Record.ID ||
		retry.Actor != TaskActorOperator || retry.PlanID != source.Record.PlanID ||
		retry.PlanHash != source.Record.PlanHash || retry.Target != source.Record.Target ||
		retry.Params[TaskPlatformComponentDesiredSHA256Param] !=
			source.Record.Params[TaskPlatformComponentDesiredSHA256Param] {
		return hostResolutionReconciliationChange{}, nil
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformComponentTaskRenderInputKey(source.Record.PlanID),
			platformComponentTaskActiveKey(source.Record.Target),
		},
		Revision: source.ReadRevision,
	})
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if state == nil || state.ReadRevision != source.ReadRevision || len(state.Values) != 2 || state.Values[0] == nil {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver retry input is missing",
		)
	}
	input, err := decodePlatformComponentTaskRenderInput(state.Values[0].Value)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if input.PlanID != source.Record.PlanID || input.ComponentID != source.Record.Target ||
		input.ExecutionPlanSHA256 != source.Record.PlanHash || input.ExecutionPlanSHA256 != retry.PlanHash ||
		input.DesiredSHA256 != source.Record.Params[TaskPlatformComponentDesiredSHA256Param] {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver retry changed its pinned input",
		)
	}
	origin := source
	if input.TaskID != source.Record.ID {
		originRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{taskKey(input.TaskID)}, Revision: source.ReadRevision,
		})
		if readErr != nil {
			return hostResolutionReconciliationChange{}, readErr
		}
		if originRead == nil || originRead.ReadRevision != source.ReadRevision || len(originRead.Values) != 1 ||
			originRead.Values[0] == nil {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver retry origin is missing",
			)
		}
		originRecord, decodeErr := decodeTaskRecord(originRead.Values[0].Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		origin = Versioned[TaskRecord]{
			Record: originRecord, Revision: originRead.Values[0].ModRevision, ReadRevision: source.ReadRevision,
		}
	}
	if !isPlatformDNSResolverTaskAttempt(origin.Record) || origin.Record.ID != input.TaskID ||
		origin.Record.PlanID != input.PlanID || origin.Record.Target != input.ComponentID {
		return hostResolutionReconciliationChange{}, nil
	}
	if state.Values[1] != nil {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver already has an active successor",
		)
	}
	conditions := []etcdstore.Condition{
		{Key: platformComponentTaskRenderInputKey(input.PlanID), ModRevision: state.Values[0].ModRevision},
		{Key: platformComponentTaskActiveKey(input.ComponentID)},
	}
	if origin.Record.ID != source.Record.ID {
		conditions = append(conditions, etcdstore.Condition{Key: taskKey(origin.Record.ID), ModRevision: origin.Revision})
	}
	value := []byte(retry.ID)
	return hostResolutionReconciliationChange{
		applies:    true,
		conditions: conditions,
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: platformComponentTaskActiveKey(input.ComponentID), Value: value,
		}},
		values: [][]byte{value},
	}, nil
}

func (repository *TaskRepository) platformResolverAtRevision(
	ctx context.Context,
	revision int64,
) (Versioned[componentrecord.Record], error) {
	componentIDs := make([]string, 0, 1)
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: platformComponentOwnerPrefix, StartExclusive: start,
			Limit: MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return Versioned[componentrecord.Record]{}, err
		}
		if page == nil || page.ReadRevision != revision {
			return Versioned[componentrecord.Record]{}, errs.New(
				errs.KindInternal,
				"platform Component scan did not preserve its fixed revision",
			)
		}
		for _, value := range page.Values {
			if !strings.HasPrefix(value.Key, platformComponentOwnerPrefix) {
				clearRangeKeyValues(page.Values)
				return Versioned[componentrecord.Record]{}, errs.New(
					errs.KindInternal,
					"platform Component scan contains an invalid key",
				)
			}
			componentID := strings.TrimPrefix(value.Key, platformComponentOwnerPrefix)
			if ids.Validate(ids.KindComponent, componentID) != nil || string(value.Value) != componentID {
				clearRangeKeyValues(page.Values)
				return Versioned[componentrecord.Record]{}, errs.New(
					errs.KindInternal,
					"platform Component owner index is corrupt",
				)
			}
			componentIDs = append(componentIDs, componentID)
		}
		if !page.More {
			clearRangeKeyValues(page.Values)
			break
		}
		if len(page.Values) == 0 {
			return Versioned[componentrecord.Record]{}, errs.New(errs.KindInternal, "platform Component scan did not advance")
		}
		start = page.Values[len(page.Values)-1].Key
		clearRangeKeyValues(page.Values)
	}
	sort.Strings(componentIDs)
	if len(componentIDs) == 0 {
		return Versioned[componentrecord.Record]{}, errs.New(
			errs.KindComponentNotFound,
			"platform dns-resolver Component is missing",
		)
	}
	keys := make([]string, len(componentIDs))
	for index, componentID := range componentIDs {
		keys[index] = componentrecord.RecordKey(componentID)
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return Versioned[componentrecord.Record]{}, err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != len(keys) {
		return Versioned[componentrecord.Record]{}, errs.New(errs.KindInternal, "platform Component scan is incomplete")
	}
	candidates := make([]Versioned[componentrecord.Record], 0, len(componentIDs))
	for index, componentValue := range state.Values {
		if componentValue == nil {
			return Versioned[componentrecord.Record]{}, errs.New(
				errs.KindStateConflict,
				"platform dns-resolver Component is missing",
			)
		}
		component, decodeErr := componentrecord.DecodeRecord(componentValue.Value)
		if decodeErr != nil {
			return Versioned[componentrecord.Record]{}, decodeErr
		}
		if component.Desired.ID != componentIDs[index] || component.Desired.Owner != core.ComponentOwnerPlatform ||
			component.Desired.OwnerID != "" {
			return Versioned[componentrecord.Record]{}, errs.New(
				errs.KindInternal,
				"platform Component owner index is corrupt",
			)
		}
		candidates = append(candidates, Versioned[componentrecord.Record]{
			Record: component, Revision: componentValue.ModRevision, ReadRevision: revision,
		})
	}
	if repository.platformResolverSelector != nil {
		selected, selectErr := repository.platformResolverSelector(ctx, candidates)
		if selectErr != nil {
			return Versioned[componentrecord.Record]{}, selectErr
		}
		for _, candidate := range candidates {
			if candidate.Record.Desired.ID == selected.Record.Desired.ID && candidate.Revision == selected.Revision &&
				selected.ReadRevision == revision {
				return selected, nil
			}
		}
		return Versioned[componentrecord.Record]{}, errs.New(
			errs.KindInternal,
			"platform dns-resolver selector returned an unscanned Component",
		)
	}
	if len(candidates) != 1 {
		return Versioned[componentrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"multiple platform dns-resolver Components are registered",
		)
	}
	return candidates[0], nil
}

func (repository *TaskRepository) platformResolverActiveAtRevision(
	ctx context.Context,
	componentID string,
	revision int64,
) (*etcdstore.KeyValue, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{platformComponentTaskActiveKey(componentID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, errs.New(errs.KindInternal, "platform resolver active fence read is incomplete")
	}
	active := read.Values[0]
	if active == nil {
		return nil, nil
	}
	if active.Key != platformComponentTaskActiveKey(componentID) ||
		ids.Validate(ids.KindTask, string(active.Value)) != nil {
		return nil, errs.New(errs.KindInternal, "platform resolver active fence is corrupt")
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskKey(string(active.Value))}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != 1 || state.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "platform resolver active Task is missing")
	}
	activeTask, err := decodeTaskRecord(state.Values[0].Value)
	if err != nil || !isPlatformDNSResolverTaskAttempt(activeTask) || activeTask.ID != string(active.Value) ||
		activeTask.Target != componentID || (activeTask.Status != TaskStatusPending && activeTask.Status != TaskStatusRunning) {
		return nil, errs.New(errs.KindStateConflict, "platform resolver active Task is invalid")
	}
	return active, nil
}

func (repository *TaskRepository) platformResolverTaskInputAtRevision(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (PlatformComponentTaskRenderInput, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{platformComponentTaskRenderInputKey(task.PlanID)}, Revision: revision,
	})
	if err != nil {
		return PlatformComponentTaskRenderInput{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input is missing",
		)
	}
	input, err := decodePlatformComponentTaskRenderInput(read.Values[0].Value)
	if err != nil {
		return PlatformComponentTaskRenderInput{}, err
	}
	if input.PlanID != task.PlanID || input.ComponentID != task.Target || task.PlanHash != input.ExecutionPlanSHA256 ||
		!platformResolverTaskInputBelongsToTask(task, input) ||
		task.Params[TaskPlatformComponentDesiredSHA256Param] != input.DesiredSHA256 {
		return PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input is not pinned",
		)
	}
	return input, nil
}

func platformResolverTaskInputBelongsToTask(
	task TaskRecord,
	input PlatformComponentTaskRenderInput,
) bool {
	return input.TaskID == task.ID || task.RetryOf != ""
}

// prepareHostResolutionReconciliation scans the complete Route collection at
// one journal revision and overlays the terminal change being committed. The
// projection therefore never combines Route and Component state from
// different MVCC views.
func (repository *TaskRepository) prepareHostResolutionReconciliation(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
	baseConditions []etcdstore.Condition,
	platformChange platformComponentTaskChange,
) (hostResolutionReconciliationChange, error) {
	resource := task.Params[TaskResourceKindParam]
	if resource != TaskResourceRoute && resource != TaskResourceComponent {
		return hostResolutionReconciliationChange{}, nil
	}
	if revision <= 0 {
		return hostResolutionReconciliationChange{}, errs.New(errs.KindInternal, "host-resolution revision is invalid")
	}
	routes, err := repository.scanRoutesAtRevision(ctx, revision)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	removeRouteID, routeOverride, componentOverride, err :=
		repository.hostResolutionTerminalOverlay(ctx, task, terminalStatus, revision)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if platformChange.promoted != nil {
		componentOverride[platformChange.promoted.Desired.ID] = *platformChange.promoted
	}
	providerIDs := make(map[string]struct{})
	for _, scanned := range routes {
		if scanned.record.Observed.Status == routerecord.ObservedServed {
			providerIDs[scanned.record.Observed.Provider.ComponentID] = struct{}{}
		}
	}
	for _, route := range routeOverride {
		if route.Observed.Status == routerecord.ObservedServed {
			providerIDs[route.Observed.Provider.ComponentID] = struct{}{}
		}
	}
	components, componentConditions, err := repository.hostResolutionComponents(ctx, providerIDs, revision)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	for id, record := range componentOverride {
		components[id] = record
	}
	hostRoutes := make([]HostResolutionRouteRecord, 0, len(routes))
	conditions := append([]etcdstore.Condition(nil), baseConditions...)
	for _, condition := range componentConditions {
		conditions = appendHostResolutionCondition(conditions, condition)
	}
	for _, scanned := range routes {
		route := scanned.record
		if route.Desired.ID == removeRouteID {
			continue
		}
		if replacement, found := routeOverride[route.Desired.ID]; found {
			route = replacement
		}
		if route.Desired.Host == "" || route.Observed.Status != routerecord.ObservedServed {
			continue
		}
		provider := components[route.Observed.Provider.ComponentID]
		if !validHostResolutionProvider(provider) {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"served Route provider is not enabled, healthy, and addressable",
			)
		}
		serviceID := provider.Runtime.GeneratedServices[0]
		hostRoutes = append(hostRoutes, HostResolutionRouteRecord{
			EnvironmentID: route.EnvironmentID, DesiredRevisionID: task.ID,
			AppliedRevision: route.Observed.Provider.InputRevision, RouteID: route.Desired.ID,
			ServiceID: serviceID, Hostname: route.Desired.Host,
			IPv4: provider.Runtime.PinnedIPv4,
		})
	}
	currentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hostResolutionProjectionKey}, Revision: revision,
	})
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if currentRead == nil || currentRead.ReadRevision != revision || len(currentRead.Values) != 1 {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"host-resolution projection read is empty",
		)
	}
	current := currentRead.Values[0]
	var stored *HostResolutionProjectionRecord
	if current != nil {
		decoded, decodeErr := decodeHostResolutionProjectionRecord(current.Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		stored = &decoded
	}
	if isPlatformDNSResolverTaskAttempt(task) {
		preserveHostResolutionDesiredRevisionIDs(stored, hostRoutes)
	}
	publication, err := prepareHostResolutionProjectionPublication(current, revision, hostRoutes)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if publication.record.InputRevision == 0 {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindInternal,
			"host-resolution projection is invalid",
		)
	}
	conditions = appendHostResolutionCondition(conditions, publication.conditions[0])
	projectionChanged := true
	if stored != nil {
		if stored.InputSHA256 == publication.record.InputSHA256 {
			publication.clear()
			publication.record = *stored
			projectionChanged = false
		}
	}
	change := hostResolutionReconciliationChange{applies: true, conditions: conditions}
	if projectionChanged {
		change.mutations = append(change.mutations, publication.mutations...)
		change.values = append(change.values, publication.values...)
	} else {
		publication.clear()
	}

	resolverAttempt := isPlatformDNSResolverTaskAttempt(task)
	if repository.platformResolverTaskPreparer == nil && !resolverAttempt {
		return change, nil
	}
	resolver, err := repository.platformResolverAtRevision(ctx, revision)
	if err != nil {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, err
	}
	if resolverAttempt && resolver.Record.Desired.ID != task.Target {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver Component changed",
		)
	}
	if override, found := componentOverride[resolver.Record.Desired.ID]; found {
		resolver.Record = override
	}
	active, err := repository.platformResolverActiveAtRevision(ctx, resolver.Record.Desired.ID, revision)
	if err != nil {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, err
	}
	if resolverAttempt && (active == nil || string(active.Value) != task.ID) {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict, "platform resolver active Task ownership changed",
		)
	}
	resolverTask := resolverAttempt
	if active != nil && !resolverTask {
		change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
			Key: active.Key, ModRevision: active.ModRevision,
		})
		return change, nil
	}
	if !resolver.Record.Desired.Enabled {
		if resolverTask {
			change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
				Key: active.Key, ModRevision: active.ModRevision,
			})
			change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: active.Key})
		}
		return change, nil
	}
	if resolverTask {
		if active == nil {
			clearHostResolutionReconciliationChange(change)
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver active Task is missing",
			)
		}
		input, inputErr := repository.platformResolverTaskInputAtRevision(ctx, task, revision)
		if inputErr != nil {
			clearHostResolutionReconciliationChange(change)
			return hostResolutionReconciliationChange{}, inputErr
		}
		stale := stored == nil || stored.InputRevision != input.HostResolutionInputRevision ||
			stored.InputSHA256 != input.HostResolutionSHA256
		if !stale {
			change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
				Key: active.Key, ModRevision: active.ModRevision,
			})
			change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: active.Key})
			return change, nil
		}
		successor := newPlatformDNSResolverTask(
			resolver.Record.Desired.ID,
			task.UpdatedAt.Add(time.Nanosecond),
		)
		contribution, contributionErr := repository.preparePlatformDNSResolverTaskContribution(
			ctx, resolver, publication.record, successor, active, &task,
			platformChange.observation, change.conditions,
		)
		if contributionErr != nil {
			clearHostResolutionReconciliationChange(change)
			return hostResolutionReconciliationChange{}, contributionErr
		}
		if !contribution.applies {
			change.conditions = appendHostResolutionCondition(change.conditions, etcdstore.Condition{
				Key: active.Key, ModRevision: active.ModRevision,
			})
			change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: active.Key})
			return change, nil
		}
		change.conditions = contribution.conditions
		change.mutations = append(change.mutations, contribution.mutations...)
		change.values = append(change.values, contribution.values...)
		return change, nil
	}
	if repository.platformResolverTaskPreparer == nil {
		return change, nil
	}
	successor := newPlatformDNSResolverTask(resolver.Record.Desired.ID, time.Now().UTC())
	contribution, contributionErr := repository.preparePlatformDNSResolverTaskContribution(
		ctx, resolver, publication.record, successor, nil, nil, nil, change.conditions,
	)
	if contributionErr != nil {
		clearHostResolutionReconciliationChange(change)
		return hostResolutionReconciliationChange{}, contributionErr
	}
	change.conditions = contribution.conditions
	change.mutations = append(change.mutations, contribution.mutations...)
	change.values = append(change.values, contribution.values...)
	return change, nil
}

func preserveHostResolutionDesiredRevisionIDs(
	current *HostResolutionProjectionRecord,
	routes []HostResolutionRouteRecord,
) {
	if current == nil {
		return
	}
	previous := make(map[string]HostResolutionRouteRecord, len(current.Routes))
	for _, route := range current.Routes {
		previous[route.EnvironmentID+"\x00"+route.RouteID] = route
	}
	for index := range routes {
		prior, found := previous[routes[index].EnvironmentID+"\x00"+routes[index].RouteID]
		if !found || prior.AppliedRevision != routes[index].AppliedRevision ||
			prior.ServiceID != routes[index].ServiceID || prior.Hostname != routes[index].Hostname ||
			prior.IPv4 != routes[index].IPv4 {
			continue
		}
		routes[index].DesiredRevisionID = prior.DesiredRevisionID
	}
}

type scannedRoute struct {
	record routerecord.Record
}

func (repository *TaskRepository) scanRoutesAtRevision(ctx context.Context, revision int64) ([]scannedRoute, error) {
	result := make([]scannedRoute, 0)
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentComposeProjectionPrefix, StartExclusive: start,
			Limit: MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision {
			return nil, errs.New(errs.KindInternal, "Route scan did not preserve its fixed revision")
		}
		for _, value := range page.Values {
			if !strings.HasPrefix(value.Key, environmentComposeProjectionPrefix) {
				clearRangeKeyValues(page.Values)
				return nil, errs.New(errs.KindInternal, "Applied Environment projection scan contains an invalid key")
			}
			environmentID := strings.TrimPrefix(value.Key, environmentComposeProjectionPrefix)
			if strings.Contains(environmentID, "/") || recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
				clearRangeKeyValues(page.Values)
				return nil, recordcodec.CorruptRecord()
			}
			projection, decodeErr := decodeEnvironmentComposeProjection(value.Value)
			if decodeErr != nil || projection.EnvironmentID != environmentID {
				clearRangeKeyValues(page.Values)
				return nil, corruptEnvironmentComposeProjection()
			}
			versioned := Versioned[EnvironmentComposeProjection]{
				Record: projection, Revision: value.ModRevision, ReadRevision: page.ReadRevision,
			}
			for _, desired := range projection.DesiredRoutes {
				record, joinErr := routeRecordFromDesiredProjection(ctx, repository.store, versioned, desired)
				if joinErr != nil {
					clearRangeKeyValues(page.Values)
					return nil, joinErr
				}
				result = append(result, scannedRoute{record: record.Record})
			}
		}
		if !page.More {
			clearRangeKeyValues(page.Values)
			return result, nil
		}
		if len(page.Values) == 0 {
			return nil, errs.New(errs.KindInternal, "Route scan did not advance")
		}
		start = page.Values[len(page.Values)-1].Key
		clearRangeKeyValues(page.Values)
	}
}

func (repository *TaskRepository) hostResolutionComponents(
	ctx context.Context,
	idsByComponent map[string]struct{},
	revision int64,
) (map[string]componentrecord.Record, []etcdstore.Condition, error) {
	keys := make([]string, 0, len(idsByComponent))
	for id := range idsByComponent {
		keys = append(keys, componentrecord.RecordKey(id))
	}
	sort.Strings(keys)
	result := make(map[string]componentrecord.Record, len(keys))
	if len(keys) == 0 {
		return result, nil, nil
	}
	stateKeys := append([]string{componentrecord.WriteFenceKey}, keys...)
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != len(stateKeys) {
		return nil, nil, errs.New(errs.KindInternal, "host-resolution Component read is incomplete")
	}
	fence := etcdstore.Condition{Key: componentrecord.WriteFenceKey}
	if state.Values[0] != nil {
		fence.ModRevision = state.Values[0].ModRevision
	}
	for index, value := range state.Values[1:] {
		if value == nil {
			return nil, nil, errs.New(errs.KindStateConflict, "host-resolution provider Component is missing")
		}
		record, decodeErr := componentrecord.DecodeRecord(value.Value)
		if decodeErr != nil {
			return nil, nil, decodeErr
		}
		id := strings.TrimPrefix(keys[index], componentrecord.RecordPrefix)
		if record.Desired.ID != id {
			return nil, nil, recordcodec.CorruptRecord()
		}
		result[id] = record
	}
	return result, []etcdstore.Condition{fence}, nil
}

func validHostResolutionProvider(record componentrecord.Record) bool {
	if record.Desired.Owner != core.ComponentOwnerEnvironment ||
		record.Desired.Kind != core.ComponentKindIngressCaddy || !record.Desired.Enabled || !record.Runtime.Healthy ||
		len(
			record.Runtime.GeneratedServices,
		) != 1 || ids.Validate(ids.KindService, record.Runtime.GeneratedServices[0]) != nil {
		return false
	}
	address, err := netip.ParseAddr(record.Runtime.PinnedIPv4)
	return err == nil && address.Is4() && !address.Is4In6() && !address.IsUnspecified() &&
		!address.IsMulticast() && address.String() == record.Runtime.PinnedIPv4
}

func appendHostResolutionCondition(conditions []etcdstore.Condition, candidate etcdstore.Condition) []etcdstore.Condition {
	for _, existing := range conditions {
		if existing.Key != candidate.Key {
			continue
		}
		return conditions
	}
	return append(conditions, candidate)
}

func (repository *TaskRepository) hostResolutionTerminalOverlay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (string, map[string]routerecord.Record, map[string]componentrecord.Record, error) {
	removal := ""
	routeOverride := make(map[string]routerecord.Record)
	componentOverride := make(map[string]componentrecord.Record)
	switch task.Params[TaskResourceKindParam] {
	case TaskResourceRoute:
		mutationRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{routeMutationIntentKey(task.ID)}, Revision: revision,
		})
		if err != nil {
			return "", nil, nil, err
		}
		if mutationRead == nil || mutationRead.ReadRevision != revision || len(mutationRead.Values) != 1 {
			return "", nil, nil, errs.New(errs.KindInternal, "host-resolution Route intent read is incomplete")
		}
		if mutationRead.Values[0] == nil {
			removalRead, removalErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{routeRemovalIntentKey(task.ID)}, Revision: revision,
			})
			if removalErr != nil {
				return "", nil, nil, removalErr
			}
			if removalRead == nil || removalRead.ReadRevision != revision || len(removalRead.Values) != 1 {
				return "", nil, nil, errs.New(
					errs.KindInternal,
					"host-resolution Route removal intent read is incomplete",
				)
			}
			if removalRead.Values[0] != nil {
				intent, decodeErr := decodeRouteRemovalIntent(removalRead.Values[0].Value)
				if decodeErr != nil {
					return "", nil, nil, decodeErr
				}
				if terminalStatus == TaskStatusCompleted {
					removal = intent.RouteID
				}
			}
		} else {
			intent, decodeErr := decodeRouteMutationIntent(mutationRead.Values[0].Value)
			if decodeErr != nil {
				return "", nil, nil, decodeErr
			}
			if terminalStatus == TaskStatusCompleted {
				route := intent.Route
				if intent.Provider != nil {
					route.Observed.Provider = routerecord.ProviderObservation{
						ComponentID: intent.Provider.ComponentID, DefinitionDigest: intent.Provider.DefinitionDigest,
						CatalogDigest: intent.Provider.CatalogDigest, InputRevision: intent.Provider.InputRevision,
						InputGeneration: intent.Provider.InputGeneration,
					}
					route.Observed.Status = routerecord.ObservedServed
				}
				routeOverride[route.Desired.ID] = route
			}
		}
	case TaskResourceComponent:
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{componentTaskIntentKey(task.ID)}, Revision: revision,
		})
		if err != nil {
			return "", nil, nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return "", nil, nil, errs.New(errs.KindInternal, "host-resolution Component intent read is incomplete")
		}
		if read.Values[0] != nil {
			intent, decodeErr := decodeComponentTaskIntent(read.Values[0].Value)
			if decodeErr != nil {
				return "", nil, nil, decodeErr
			}
			for _, candidate := range intent.Candidates {
				componentOverride[candidate.Candidate.Desired.ID] = candidate.Current
				if terminalStatus == TaskStatusCompleted {
					promoted, promoteErr := componentrecord.SetRuntime(candidate.Candidate,
						candidate.Candidate.Runtime.GeneratedServices, candidate.Candidate.Runtime.PinnedIPv4,
						candidate.Candidate.Desired.Enabled)
					if promoteErr != nil {
						return "", nil, nil, promoteErr
					}
					componentOverride[candidate.Candidate.Desired.ID] = promoted
				}
			}
			if intent.RouteProjection != nil {
				for _, candidate := range intent.RouteProjection.Routes {
					route, routeErr := repository.routeAtRevision(ctx, candidate.Desired.ID, revision)
					if routeErr != nil {
						return "", nil, nil, routeErr
					}
					if route.EnvironmentID != intent.EnvironmentID ||
						route.DesiredGeneration != candidate.DesiredGeneration ||
						!routeDesiredEqual(route.Desired, candidate.Desired) {
						return "", nil, nil, errs.New(
							errs.KindStateConflict,
							"Component Route desired state changed during host reconciliation",
						)
					}
					if intent.RouteProjection.Provider == nil && terminalStatus != TaskStatusCompleted {
						continue
					}
					status := routerecord.ObservedUnserved
					provider := routerecord.ProviderObservation{}
					if intent.RouteProjection.Provider != nil {
						status = routerecord.ObservedDegraded
						if terminalStatus == TaskStatusCompleted {
							status = routerecord.ObservedServed
						}
						pin := intent.RouteProjection.Provider
						provider = routerecord.ProviderObservation{
							ComponentID: pin.ComponentID, DefinitionDigest: pin.DefinitionDigest,
							CatalogDigest: pin.CatalogDigest, InputRevision: pin.InputRevision,
							InputGeneration: pin.InputGeneration,
						}
					}
					replacement, replaceErr := routerecord.SetObservation(route, routerecord.Observation{
						Status: status, DesiredGeneration: route.DesiredGeneration, Provider: provider,
					})
					if replaceErr != nil {
						return "", nil, nil, replaceErr
					}
					routeOverride[route.Desired.ID] = replacement
				}
			}
		}
	}
	return removal, routeOverride, componentOverride, nil
}

func (repository *TaskRepository) routeAtRevision(
	ctx context.Context,
	routeID string,
	revision int64,
) (routerecord.Record, error) {
	routes, err := repository.scanRoutesAtRevision(ctx, revision)
	if err != nil {
		return routerecord.Record{}, err
	}
	for _, route := range routes {
		if route.record.Desired.ID == routeID {
			return route.record, nil
		}
	}
	return routerecord.Record{}, errs.New(errs.KindStateConflict, "host-resolution Route changed during reconciliation")
}
