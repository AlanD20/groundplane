package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	GetRunner(context.Context, string) (etcd.Versioned[etcd.RunnerRecord], error)
	BeginRunnerRemovalWithTask(
		context.Context,
		etcd.Versioned[etcd.RunnerRecord],
		etcd.DeletionTombstoneRecord,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal context is required")
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Runner id is invalid")
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetRunner, ID: runnerID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, runnerRemoveRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replay(ctx, locator, runnerID)
	}
	current, err := service.repository.GetRunner(ctx, runnerID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if current.Record.ProvisioningState == etcd.RunnerProvisioningProvisioning {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Runner provisioning must finish before removal",
		)
	}
	locator = runnerRemovalLocator(current.Record.Desired, idempotencyKey)
	evidence, err := service.protectIntent(ctx, locator, runnerID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal replay is invalid")
		}
		return cloneResponse(resolution.Response), nil
	}

	now := service.now().UTC()
	task := newRunnerRemovalTask(current.Record, idempotencyKey, now)
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: &target, Intent: durable, Response: response,
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	tombstone := etcd.DeletionTombstoneRecord{
		TargetKind: etcd.DeletionTargetRunner, TargetID: runnerID, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: etcd.DeletionPhaseFinalizing, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.repository.BeginRunnerRemovalWithTask(ctx, current, tombstone, task, marker)
	if mutationErr != nil {
		if !unknownMutationOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx, service.idempotency, locator, evidence, mutationErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal resolution is invalid")
	}
}

func (service *RemovalService) replay(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	runnerID string,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.protectIntent(ctx, locator, runnerID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, existing, err := service.coordinator.ResolveExisting(
		ctx, service.idempotency, locator, evidence,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Runner removal replay index is inconsistent")
	}
	return cloneResponse(resolution.Response), nil
}

func (service *RemovalService) protectIntent(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	runnerID string,
) (requestidempotency.ProtectedEvidence, error) {
	scopeKind := requestidempotency.ScopeTenant
	if locator.ScopeKind == etcd.IdempotencyScopeProject {
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

func runnerRemovalLocator(desired etcd.RunnerDesiredRecord, key string) etcd.IdempotencyLocator {
	scopeKind := etcd.IdempotencyScopeTenant
	if desired.OwnerKind == etcd.RunnerOwnerProject {
		scopeKind = etcd.IdempotencyScopeProject
	}
	return etcd.IdempotencyLocator{
		ScopeKind: scopeKind, ScopeID: desired.OwnerID,
		Method: http.MethodDelete, Route: runnerRemoveRoute, Key: key,
	}
}

func newRunnerRemovalTask(record etcd.RunnerRecord, key string, now time.Time) etcd.TaskRecord {
	owner := etcd.TaskOwner{WorkspaceType: etcd.TaskWorkspaceTenant, TenantID: record.Desired.TenantID}
	if record.Desired.OwnerKind == etcd.RunnerOwnerProject {
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
		Owner: owner, Actor: etcd.TaskActorOperator, Executor: etcd.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), PlanHash: hex.EncodeToString(planDigest[:]), RenderGeneration: 1,
		Type: etcd.TaskRemove, Target: record.Desired.ID,
		Params: etcd.RunnerRemovalTaskParams(record),
		Steps: []etcd.TaskStepRecord{
			{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)},
		}, TimeoutSeconds: runnerRemoveTimeoutSeconds,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
}
