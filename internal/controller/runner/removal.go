package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	runnerRemoveRoute          = "/runners/{id}"
	runnerRemoveTimeoutSeconds = int64(300)
)

type removalRepository interface {
	GetRunner(context.Context, string) (etcdstore.Versioned[runnerrecord.RunnerRecord], error)
	BeginRunnerRemovalWithTask(
		context.Context,
		etcdstore.Versioned[runnerrecord.RunnerRecord],
		deletionrecord.DeletionTombstoneRecord,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

// RemovalService fences one managed Runner and publishes its native cleanup
// Task. Durable allocation remains owned until exact host absence is proven.
type RemovalService struct {
	repository  removalRepository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	now         func() time.Time
}

func NewRemovalService(
	repository removalRepository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
) (*RemovalService, error) {
	if repository == nil || idempotency == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "Runner removal service is not configured")
	}
	return &RemovalService{
		repository: repository, idempotency: idempotency, coordinator: coordinator, now: time.Now,
	}, nil
}

func (service *RemovalService) RemoveRunner(
	ctx context.Context,
	runnerID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal context is required")
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Runner id is invalid")
	}
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetRunner, ID: runnerID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, runnerRemoveRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replay(ctx, locator, runnerID)
	}
	current, err := service.repository.GetRunner(ctx, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if current.Record.ProvisioningState == runnerrecord.RunnerProvisioningProvisioning {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Runner provisioning must finish before removal",
		)
	}
	locator = runnerRemovalLocator(current.Record.Desired, idempotencyKey)
	evidence, err := service.protectIntent(ctx, locator, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal replay is invalid")
		}
		return cloneResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	task := newRunnerRemovalTask(current.Record, idempotencyKey, now)
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetRunner, TargetID: runnerID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginRunnerRemovalWithTask(ctx, current, tombstone, task, marker)
	if mutationErr != nil {
		if !unknownMutationOutcome(mutationErr) {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, evidence, mutationErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal resolution is invalid")
	}
}

func (service *RemovalService) replay(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	runnerID string,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.protectIntent(ctx, locator, runnerID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal replay index is inconsistent")
	}
	return cloneResponse(resolution.Response), nil
}

func (service *RemovalService) protectIntent(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	runnerID string,
) (requestidempotency.ProtectedEvidence, error) {
	scopeKind := requestidempotency.ScopeTenant
	if locator.ScopeKind == idempotencyrecord.IdempotencyScopeProject {
		scopeKind = requestidempotency.ScopeProject
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: runnerRemoveRoute,
		Scope: requestidempotency.Scope{Kind: scopeKind, ID: locator.ScopeID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: runnerID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func runnerRemovalLocator(desired runnerrecord.RunnerDesiredRecord, key string) idempotencyrecord.IdempotencyLocator {
	scopeKind := idempotencyrecord.IdempotencyScopeTenant
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		scopeKind = idempotencyrecord.IdempotencyScopeProject
	}
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: scopeKind, ScopeID: desired.OwnerID,
		Method: http.MethodDelete, Route: runnerRemoveRoute, Key: key,
	}
}

func newRunnerRemovalTask(record runnerrecord.RunnerRecord, key string, now time.Time) etcd.TaskRecord {
	owner := taskjournal.TaskOwner{WorkspaceType: taskjournal.TaskWorkspaceTenant, TenantID: record.Desired.TenantID}
	if record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		owner.ProjectID = record.Desired.OwnerID
	}
	planInput := strings.Join([]string{
		"groundplane.runner-remove.v1", record.Desired.ID, record.Desired.TenantID,
		string(record.Desired.OwnerKind), record.Desired.OwnerID,
		record.Allocation.NetworkCIDR,
	}, "\x00")
	planDigest := sha256.Sum256([]byte(planInput))
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: key,
		Owner: owner, Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: hex.EncodeToString(planDigest[:]), RenderGeneration: 1,
		Type: taskjournal.TaskRemove, Target: record.Desired.ID,
		Params: runnerrecord.RunnerRemovalTaskParams(record),
		Steps: []taskjournal.TaskStepRecord{
			{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
		}, TimeoutSeconds: runnerRemoveTimeoutSeconds,
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
}
