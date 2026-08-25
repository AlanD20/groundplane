package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const backupRunRoute = "/environments/{id}/backup-run"

// BackupRunPrepareInput contains only identities and the selected fixed
// revision. The repository fills it with immutable policy, hierarchy, source,
// target, connector, credential, key, and config evidence after idempotency
// replay has been resolved.
type BackupRunPrepareInput struct {
	EnvironmentID string
	TaskID        string
	OperationID   string
	PlanID        string
	StepIDs       []string
	FixedRevision int64
	CreatedAt     time.Time
}

// BackupRunPrepared is returned by the repository only after all fixed-
// revision evidence has been validated. Publication remains opaque and
// one-shot.
type BackupRunPrepared struct {
	Run         etcd.BackupRunRecord
	Owner       etcd.TaskOwner
	Publication backupRunPublication
}

type backupRunPublication interface {
	Publish(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	Clear()
}

// BackupRunRepository is the narrow durable seam needed by the bodyless
// manual backup service. Implementations must not read state until after the
// idempotency coordinator has completed ResolveExisting.
type BackupRunRepository interface {
	PrepareBackupRun(context.Context, BackupRunPrepareInput) (BackupRunPrepared, error)
}

type durableBackupRunRepository struct {
	runtime *etcd.BackupRuntimeRepository
	facts   *AttachFactService
}

func newDurableBackupRunRepository(
	runtime *etcd.BackupRuntimeRepository,
	facts *AttachFactService,
) (*durableBackupRunRepository, error) {
	if runtime == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "backup run repository dependencies are required")
	}
	return &durableBackupRunRepository{runtime: runtime, facts: facts}, nil
}

func (repository *durableBackupRunRepository) PrepareBackupRun(
	ctx context.Context,
	input BackupRunPrepareInput,
) (BackupRunPrepared, error) {
	prepared, err := repository.runtime.PrepareManualBackupRun(ctx, etcd.ManualBackupRunInput{
		EnvironmentID: input.EnvironmentID,
		TaskID:        input.TaskID, OperationID: input.OperationID, PlanID: input.PlanID,
		FixedRevision: input.FixedRevision, CreatedAt: input.CreatedAt,
	}, repository.facts.ResolveBackupIdentity)
	if err != nil {
		return BackupRunPrepared{}, err
	}
	return BackupRunPrepared{
		Run: prepared.Run, Owner: prepared.Owner, Publication: prepared.Publication,
	}, nil
}

// BackupRunPlanBuilder seals a task/run pair without adding caller-controlled
// source selection or any plaintext materialization.
type BackupRunPlanBuilder interface {
	BuildBackupRunPlan(controller.BackupRunPlanInput) (*agentpb.ExecutionPlan, error)
}

// BackupRunPlanBuilderFunc adapts the package-level Controller builder to the
// application service.
type BackupRunPlanBuilderFunc func(controller.BackupRunPlanInput) (*agentpb.ExecutionPlan, error)

func (builder BackupRunPlanBuilderFunc) BuildBackupRunPlan(
	input controller.BackupRunPlanInput,
) (*agentpb.ExecutionPlan, error) {
	return builder(input)
}

// BackupRunIdempotency is the protected bodyless intent lifecycle used by the
// manual backup service. It is deliberately shaped like the other app
// mutation services so marker replay occurs before any durable state reads.
type backupRunIdempotency interface {
	Prepare(context.Context, string) (backupRunEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		backupRunEvidence,
	) (idempotentintent.Resolution, bool, error)
	NewMarker(
		backupRunEvidence,
		etcd.IdempotencyLocator,
		etcd.IdempotencyResponse,
		string,
		time.Time,
	) (etcd.IdempotencyMarker, error)
	ResolveKnown(
		context.Context,
		backupRunEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		backupRunEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type backupRunEvidence struct {
	candidate idempotentintent.ProtectedEvidence
}

// durableBackupRunIdempotency is the production adapter for the protected
// bodyless intent. The request body is explicitly NoBody; the idempotency key
// remains only in the locator and is never persisted as task parameters.
type durableBackupRunIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableBackupRunIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableBackupRunIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "backup run idempotency dependencies are required")
	}
	return &durableBackupRunIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableBackupRunIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
) (backupRunEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  backupRunRoute,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:   []idempotentintent.PathBinding{{Name: "id", Value: environmentID}},
		Query:  idempotentintent.Object(),
		Body:   idempotentintent.NoBody(),
	})
	if err != nil {
		return backupRunEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return backupRunEvidence{}, err
	}
	return backupRunEvidence{candidate: candidate}, nil
}

func (service *durableBackupRunIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence backupRunEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableBackupRunIdempotency) NewMarker(
	evidence backupRunEvidence,
	locator etcd.IdempotencyLocator,
	response etcd.IdempotencyResponse,
	taskID string,
	now time.Time,
) (etcd.IdempotencyMarker, error) {
	intent, err := evidence.candidate.DurableRecord()
	if err != nil {
		return etcd.IdempotencyMarker{}, err
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: intent, Response: response,
		TaskID: taskID, CreatedAt: now, UpdatedAt: now,
	}
	marker.Intent.Ciphertext = append([]byte(nil), intent.Ciphertext...)
	marker.Response.Body = append([]byte(nil), response.Body...)
	return marker, nil
}

func (service *durableBackupRunIdempotency) ResolveKnown(
	ctx context.Context,
	evidence backupRunEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableBackupRunIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence backupRunEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(
		ctx,
		service.repository,
		locator,
		evidence.candidate,
		original,
	)
}

// BackupRunService coordinates the exact bodyless manual backup intent.
type BackupRunService struct {
	repository  BackupRunRepository
	plans       BackupRunPlanBuilder
	idempotency backupRunIdempotency
	now         func() time.Time
}

// NewBackupRunService constructs the application-level manual backup
// publisher. The idempotency dependency is injected so replay ordering is
// testable without an HTTP server.
func NewBackupRunService(
	repository BackupRunRepository,
	plans BackupRunPlanBuilder,
	idempotency backupRunIdempotency,
) *BackupRunService {
	return &BackupRunService{
		repository:  repository,
		plans:       plans,
		idempotency: idempotency,
		now:         time.Now,
	}
}

// RunBackup publishes one operator/manual backup. An optional fixed revision
// is accepted for callers that already selected a read revision; zero leaves
// revision selection to the repository after replay resolution.
func (service *BackupRunService) RunBackup(
	ctx context.Context,
	environmentID, idempotencyKey string,
	fixedRevision ...int64,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.repository == nil || service.plans == nil ||
		service.idempotency == nil {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"backup run service is not configured",
		)
	}
	if environmentID == "" || idempotencyKey == "" {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"environment and idempotency key are required",
		)
	}
	if len(fixedRevision) > 1 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"at most one fixed revision may be supplied",
		)
	}
	revision := int64(0)
	if len(fixedRevision) == 1 {
		revision = fixedRevision[0]
	}

	locator := etcd.IdempotencyLocator{
		Method: http.MethodPost, Route: backupRunRoute, Key: idempotencyKey,
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
	}
	evidence, err := service.idempotency.Prepare(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer evidence.candidate.Destroy()
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return resolution.Response, nil
	}

	createdAt := service.now().UTC().Truncate(time.Millisecond)
	taskID, operationID, planID := ids.New(
		ids.KindTask,
	), ids.New(
		ids.KindOperation,
	), ids.New(
		ids.KindPlan,
	)
	prepared, err := service.repository.PrepareBackupRun(ctx, BackupRunPrepareInput{
		EnvironmentID: environmentID, TaskID: taskID, OperationID: operationID,
		PlanID: planID, FixedRevision: revision, CreatedAt: createdAt,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer prepared.Publication.Clear()

	steps := make([]etcd.TaskStepRecord, len(prepared.Run.Sources))
	for index := range steps {
		steps[index] = etcd.TaskStepRecord{ID: ids.New(ids.KindStep)}
	}
	task := etcd.TaskRecord{
		ID:                taskID,
		OperationID:       operationID,
		Owner:             prepared.Owner,
		Actor:             etcd.TaskActorOperator,
		Executor:          etcd.TaskExecutorAgent,
		PlanID:            planID,
		Type:              etcd.TaskBackup,
		Target:            environmentID,
		Steps:             steps,
		TimeoutSeconds:    6 * 60 * 60,
		Status:            etcd.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
	}
	sealed, err := service.plans.BuildBackupRunPlan(controller.BackupRunPlanInput{
		Task: task, Run: prepared.Run, Upload: controller.BackupRunUploadAuthorities(prepared.Run),
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	// The sealed digest is task metadata, not a parameter or materialization.
	// The persistence seam validates this exact digest before publication.
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)

	responseBody, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        append([]byte(nil), responseBody...),
	}
	marker, err := service.idempotency.NewMarker(evidence, locator, response, taskID, createdAt)
	clear(responseBody)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, publishErr := prepared.Publication.Publish(ctx, task, sealed, marker)
	if publishErr != nil {
		if !errors.Is(publishErr, errs.New(errs.KindStorageUnavailable, "")) &&
			!errors.Is(publishErr, context.DeadlineExceeded) {
			return etcd.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, publishErr)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		return resolution.Response, nil
	}
	resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionApplied {
		resolution.Response = response
	}
	return resolution.Response, nil
}
