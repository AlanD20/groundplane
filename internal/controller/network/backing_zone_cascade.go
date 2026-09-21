package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backingZoneCascadePollInterval = 250 * time.Millisecond

type backingZoneCascadeRepository interface {
	GetZone(context.Context, string) (etcdstore.Versioned[zonerecord.Record], error)
	GetDeletionTombstone(
		context.Context,
		deletionrecord.DeletionTargetKind,
		string,
	) (etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord], bool, error)
	ListAttachesByBackingNetworkAtRevision(
		context.Context,
		string,
		string,
		int64,
	) ([]etcdstore.Versioned[attachrecord.Record], error)
	GetTask(context.Context, string) (etcdstore.Versioned[etcd.TaskRecord], error)
	GetSystemTaskInitiation(context.Context, string) (etcd.TaskInitiation, error)
	GetZoneRemovalIntent(context.Context, string) (etcdstore.Versioned[etcd.ZoneRemovalIntent], bool, error)
	HandoffBackingZoneDeletion(
		context.Context,
		etcdstore.Versioned[zonerecord.Record],
		string,
		etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord],
		etcd.ZoneRemovalIntent,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type backingZoneCascadeDetaches interface {
	DetachAttachWithInitiation(
		context.Context,
		string,
		string,
		etcd.TaskInitiation,
	) (idempotencyrecord.IdempotencyResponse, error)
}

type backingZoneCascadeRetries interface {
	RetryTaskWithInitiation(
		context.Context,
		string,
		string,
		etcd.TaskInitiation,
	) (idempotencyrecord.IdempotencyResponse, error)
}

type backingZoneCascadeService struct {
	repository  backingZoneCascadeRepository
	detaches    backingZoneCascadeDetaches
	retries     backingZoneCascadeRetries
	plans       zoneDeletionPlanResolver
	idempotency zoneDeletionIdempotency
	now         func() time.Time
	wait        func(context.Context) error
}

func newBackingZoneCascadeService(
	repository backingZoneCascadeRepository,
	detaches backingZoneCascadeDetaches,
	retries backingZoneCascadeRetries,
	plans zoneDeletionPlanResolver,
	idempotency zoneDeletionIdempotency,
) (*backingZoneCascadeService, error) {
	if repository == nil || detaches == nil || retries == nil || plans == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "backing Zone cascade is not configured")
	}
	return &backingZoneCascadeService{
		repository:  repository,
		detaches:    detaches,
		retries:     retries,
		plans:       plans,
		idempotency: idempotency,
		now:         time.Now,
		wait: func(ctx context.Context) error {
			timer := time.NewTimer(backingZoneCascadePollInterval)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}, nil
}

func (service *backingZoneCascadeService) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "backing Zone cascade context is required")
	}
	if ok, err := isBackingZoneCascadeTask(task); err != nil || !ok {
		if err != nil {
			return err
		}
		return errs.New(errs.KindValidationFailed, "backing Zone cascade Task is invalid")
	}
	for {
		zone, err := service.repository.GetZone(ctx, task.Target)
		if err != nil {
			if errors.Is(err, errs.New(errs.KindZoneNotFound, "")) {
				return nil
			}
			return err
		}
		if zone.Record.Desired.OwnerKind != core.ZoneOwnerBackingProject ||
			zone.Record.EnvironmentID != task.Params[etcd.TaskZoneEnvironmentParam] {
			return errs.New(errs.KindStateConflict, "backing Zone cascade target changed")
		}
		attaches, err := service.repository.ListAttachesByBackingNetworkAtRevision(
			ctx,
			zone.Record.Desired.OwnerID,
			zone.Record.Desired.ID,
			zone.ReadRevision,
		)
		if err != nil {
			return err
		}
		ordered, err := orderBackingZoneCascadeAttaches(attaches)
		if err != nil {
			return err
		}
		if len(ordered) == 0 {
			return service.finish(ctx, task, zone)
		}
		if err := service.advanceAttach(ctx, task, ordered[0].Record); err != nil {
			if errors.Is(err, errs.New(errs.KindAttachNotFound, "")) {
				continue
			}
			return err
		}
	}
}

func (service *backingZoneCascadeService) advanceAttach(
	ctx context.Context,
	parent etcd.TaskRecord,
	attach attachrecord.Record,
) error {
	var response idempotencyrecord.IdempotencyResponse
	var err error
	initiation, err := service.repository.GetSystemTaskInitiation(ctx, parent.ID)
	if err != nil {
		return err
	}
	if initiation.Owner() != parent.Owner || initiation.Actor() != etcd.TaskActorSystem {
		return errs.New(errs.KindStateConflict, "backing Zone cascade initiation changed")
	}
	switch {
	case attach.Status == core.AttachReady ||
		(attach.Status == core.AttachFailed && attach.Operation == attachrecord.AttachOperationProvision):
		response, err = service.detaches.DetachAttachWithInitiation(
			ctx,
			attach.ID,
			cascadeIdempotencyKey("detach", parent.ID, attach.ID),
			initiation,
		)
	case attach.Status == core.AttachFailed && attach.Operation == attachrecord.AttachOperationDetach:
		response, err = service.retries.RetryTaskWithInitiation(
			ctx,
			attach.TaskID,
			cascadeIdempotencyKey("retry", parent.ID, attach.ID),
			initiation,
		)
	case attach.Status == core.AttachPending || attach.Status == core.AttachProvisioning:
		_, err = service.waitForTask(ctx, attach.TaskID)
		return err
	case attach.Status == core.AttachDetaching:
		terminal, waitErr := service.waitForTask(ctx, attach.TaskID)
		if waitErr != nil {
			return waitErr
		}
		return requireCascadeChildSuccess(terminal.Record)
	default:
		return errs.New(errs.KindStateConflict, "attach lifecycle cannot advance during backing Zone removal")
	}
	if err != nil {
		return err
	}
	childID, err := cascadeTaskID(response)
	if err != nil {
		return err
	}
	terminal, err := service.waitForTask(ctx, childID)
	if err != nil {
		return err
	}
	return requireCascadeChildSuccess(terminal.Record)
}

func (service *backingZoneCascadeService) finish(
	ctx context.Context,
	parent etcd.TaskRecord,
	zone etcdstore.Versioned[zonerecord.Record],
) error {
	tombstone, found, err := service.repository.GetDeletionTombstone(
		ctx,
		deletionrecord.DeletionTargetZone,
		parent.Target,
	)
	if err != nil {
		return err
	}
	if !found {
		_, readErr := service.repository.GetZone(ctx, parent.Target)
		if errors.Is(
			readErr,
			errs.New(errs.KindZoneNotFound, ""),
		) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		return errs.New(errs.KindStateConflict, "backing Zone cascade fence is missing")
	}
	childID := tombstone.Record.TaskID
	if childID == parent.ID {
		if deadline, ok := ctx.Deadline(); ok &&
			time.Until(deadline) <= time.Duration(zoneDeletionTimeoutSeconds+5)*time.Second {
			return errs.New(
				errs.KindStateConflict,
				"backing Zone cascade has insufficient time for final network removal",
			)
		}
		childID, err = service.publishFinalRemoval(ctx, parent, zone, tombstone)
		if err != nil {
			return err
		}
	}
	child, err := service.waitForTask(ctx, childID)
	if err != nil {
		return err
	}
	return requireCascadeChildSuccess(child.Record)
}

func (service *backingZoneCascadeService) publishFinalRemoval(
	ctx context.Context,
	parent etcd.TaskRecord,
	zone etcdstore.Versioned[zonerecord.Record],
	tombstone etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord],
) (string, error) {
	now := service.now().UTC()
	child := etcd.TaskRecord{
		ID:                ids.New(ids.KindTask),
		OperationID:       ids.New(ids.KindOperation),
		Owner:             parent.Owner,
		Actor:             etcd.TaskActorSystem,
		Executor:          taskjournal.TaskExecutorAgent,
		PlanID:            parent.PlanID,
		Type:              taskjournal.TaskRemove,
		Target:            parent.Target,
		TimeoutSeconds:    zoneDeletionTimeoutSeconds,
		Status:            taskjournal.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	operationID := parent.Params[etcd.TaskZoneRemovalOperationParam]
	storedIntent, found, err := service.repository.GetZoneRemovalIntent(ctx, operationID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errs.New(errs.KindStateConflict, "backing Zone removal intent is missing")
	}
	intent, err := etcd.TransferZoneRemovalIntent(storedIntent.Record, child.ID, now)
	if err != nil {
		return "", err
	}
	serviceSteps := make([]string, len(intent.AffectedServiceIDs))
	for index := range serviceSteps {
		serviceSteps[index] = ids.New(ids.KindStep)
	}
	child, err = service.plans.PrepareZoneRemovalTask(ctx, child, intent, taskplanning.ZoneRemovalTaskProcedureIDs{
		ArtifactID:     zoneStableIDFromRevision(ids.KindConfig, intent.Claim.RevisionID),
		ServiceStepIDs: serviceSteps, NetworkStepID: ids.New(ids.KindStep),
	})
	if err != nil {
		return "", err
	}
	key := cascadeIdempotencyKey("finalize", parent.ID, parent.Target)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   zone.Record.EnvironmentID,
		Method:    http.MethodDelete,
		Route:     zoneDeletionRoute,
		Key:       key,
	}
	evidence, err := service.idempotency.Prepare(
		ctx,
		locator,
		parent.Target,
		parent.Params[etcd.TaskZoneImpactTokenParam],
	)
	if err != nil {
		return "", err
	}
	defer clear(evidence.durable.Ciphertext)
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: child.ID})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable, Response: response,
		TaskID: child.ID, CreatedAt: now, UpdatedAt: now,
	}
	result, err := service.repository.HandoffBackingZoneDeletion(ctx, zone, parent.ID, tombstone, intent, child, marker)
	if err != nil {
		return "", err
	}
	resolution, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return "", err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return child.ID, nil
	case requestidempotency.ResolutionReplay:
		return cascadeTaskID(resolution.Response)
	default:
		return "", errs.New(errs.KindInternal, "backing Zone final handoff resolution is invalid")
	}
}

func (service *backingZoneCascadeService) waitForTask(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[etcd.TaskRecord], error) {
	for {
		current, err := service.repository.GetTask(ctx, taskID)
		if err != nil {
			return etcdstore.Versioned[etcd.TaskRecord]{}, err
		}
		switch current.Record.Status {
		case taskjournal.TaskStatusCompleted, taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted, taskjournal.TaskStatusTimedOut:
			return current, nil
		case taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning:
			if err := service.wait(ctx); err != nil {
				return etcdstore.Versioned[etcd.TaskRecord]{}, err
			}
		default:
			return etcdstore.Versioned[etcd.TaskRecord]{}, errs.New(
				errs.KindInternal,
				"cascade child Task status is invalid",
			)
		}
	}
}

func orderBackingZoneCascadeAttaches(
	attaches []etcdstore.Versioned[attachrecord.Record],
) ([]etcdstore.Versioned[attachrecord.Record], error) {
	byID := make(map[string]etcdstore.Versioned[attachrecord.Record], len(attaches))
	indegree := make(map[string]int, len(attaches))
	edges := make(map[string][]string, len(attaches))
	for _, attach := range attaches {
		if _, exists := byID[attach.Record.ID]; exists {
			return nil, errs.New(errs.KindInternal, "backing Zone cascade contains duplicate Attach identity")
		}
		byID[attach.Record.ID] = attach
		indegree[attach.Record.ID] = 0
	}
	for _, attach := range attaches {
		for _, grantID := range attach.Record.GrantAttachIDs {
			if _, exists := byID[grantID]; !exists {
				return nil, errs.New(errs.KindInternal, "backing Zone cascade grant target is missing")
			}
			edges[attach.Record.ID] = append(edges[attach.Record.ID], grantID)
			indegree[grantID]++
		}
	}
	ready := make([]string, 0, len(attaches))
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	slices.Sort(ready)
	result := make([]etcdstore.Versioned[attachrecord.Record], 0, len(attaches))
	for len(ready) != 0 {
		id := ready[0]
		ready = ready[1:]
		result = append(result, byID[id])
		for _, target := range edges[id] {
			indegree[target]--
			if indegree[target] == 0 {
				ready = append(ready, target)
				slices.Sort(ready)
			}
		}
	}
	if len(result) != len(attaches) {
		return nil, errs.New(errs.KindInternal, "backing Zone cascade Attach grants contain a cycle")
	}
	return result, nil
}

func cascadeIdempotencyKey(kind string, parentID string, targetID string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{"v1", kind, parentID, targetID}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func cascadeTaskID(response idempotencyrecord.IdempotencyResponse) (string, error) {
	if response.Status != http.StatusAccepted {
		return "", errs.New(errs.KindInternal, "cascade child response status is invalid")
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(
		response.Body,
		&accepted,
	); err != nil ||
		ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return "", errs.New(errs.KindInternal, "cascade child response is invalid")
	}
	return accepted.TaskID, nil
}

func requireCascadeChildSuccess(task etcd.TaskRecord) error {
	if task.Status == taskjournal.TaskStatusCompleted {
		return nil
	}
	return errs.Newf(errs.KindStateConflict, "cascade child Task %s ended with status %s", task.ID, task.Status)
}

// isBackingZoneCascadeTask validates the closed Controller executor contract.
func isBackingZoneCascadeTask(task etcd.TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController ||
		task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceBackingZone {
		return false, nil
	}
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindNetwork, task.Target) != nil || len(task.Params) != 3 ||
		ids.Validate(ids.KindEnvironment, task.Params[etcd.TaskZoneEnvironmentParam]) != nil ||
		len(task.Params[etcd.TaskZoneImpactTokenParam]) != sha256.Size*2 {
		return false, errs.New(errs.KindValidationFailed, "backing Zone cascade Task parameters are invalid")
	}
	if _, err := hex.DecodeString(task.Params[etcd.TaskZoneImpactTokenParam]); err != nil {
		return false, errs.New(errs.KindValidationFailed, "backing Zone cascade impact token is invalid")
	}
	return true, nil
}
