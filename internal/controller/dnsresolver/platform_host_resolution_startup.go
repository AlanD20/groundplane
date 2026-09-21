package dnsresolver

import (
	"context"
	"encoding/json"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnsurePlatformResolverTask is the clean-start entry point. The repository
// owns the single transaction; this function only selects the persisted
// platform Component and seals its generic resolver render input.
func EnsurePlatformResolverTask(
	ctx context.Context,
	components *etcd.ComponentRepository,
	tasks *etcd.TaskRepository,
	planner *PlatformRenderPlanner,
	coordinator *requestidempotency.Coordinator,
) error {
	if ctx == nil || components == nil || tasks == nil || planner == nil || coordinator == nil {
		return errs.New(errs.KindInternal, "platform resolver startup dependencies are required")
	}
	_, found, err := planner.projections.GetHostResolutionProjection(ctx)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	page, err := components.ListPlatformComponents(ctx, etcdstore.PageRequest{Limit: etcdstore.MaximumPageLimit})
	if err != nil {
		return err
	}
	current, err := planner.SelectResolver(ctx, page.Items)
	if err != nil {
		return err
	}
	if current.Revision <= 0 {
		return errs.New(errs.KindComponentNotFound, "platform dns-resolver Component is not initialized")
	}
	inputRevision := current.ReadRevision
	if inputRevision <= 0 {
		return errs.New(errs.KindInternal, "platform resolver startup revision is unavailable")
	}
	empty, err := resolutionrecord.NewHostResolutionProjectionRecord(inputRevision, nil)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	ensureService := len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy
	task := startupResolverTask(current.Record.Desired.ID, now, ensureService)
	desired, err := componentrecord.ProjectRecord(current.Record)
	if err != nil {
		return err
	}
	bootstrapProvenance, err := components.HasPlatformComponentBootstrapProvenance(ctx, current)
	if err != nil {
		return err
	}
	if !bootstrapProvenance {
		return errs.New(errs.KindStateConflict, "platform resolver bootstrap provenance is unavailable")
	}
	renderInput, err := planner.PrepareBootstrapConfigTaskAtProjection(ctx, current, desired, task, empty)
	if err != nil {
		return err
	}
	task, err = finalizePlatformComponentTask(task, renderInput)
	if err != nil {
		return err
	}
	task.IdempotencyKey = task.OperationID
	intent, err := platformComponentLifecycleIntent(
		ctx, coordinator, task.Target, "update", platformComponentUpdateRoute,
	)
	if err != nil {
		return err
	}
	defer clear(intent.durable.Ciphertext)
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: idempotencyrecord.IdempotencyLocator{
			ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost,
			Route: platformComponentUpdateRoute, Key: task.IdempotencyKey,
		},
		Intent: intent.durable,
		Response: idempotencyrecord.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
		},
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	return tasks.PublishPlatformDNSResolverTask(ctx, current, empty, task, renderInput, marker)
}

func finalizePlatformComponentTask(
	task etcd.TaskRecord,
	input platformcomponents.PlatformComponentTaskRenderInput,
) (etcd.TaskRecord, error) {
	if input.TaskID != task.ID || input.PlanID != task.PlanID || input.ComponentID != task.Target {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "platform Component plan identity changed")
	}
	if _, err := componentDigest(input.ExecutionPlanSHA256); err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = input.ExecutionPlanSHA256
	return task, nil
}

func startupResolverTask(componentID string, createdAt time.Time, ensureService bool) etcd.TaskRecord {
	steps := []taskjournal.TaskStepRecord{
		{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
		{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
	}
	if ensureService {
		steps = append(steps,
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)},
		)
	}
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: taskjournal.TaskUpdate, Target: componentID,
		Params: map[string]string{
			taskjournal.TaskResourceKindParam: taskjournal.TaskResourceComponent,
			etcd.TaskAutomaticReconcileParam:  "true",
		},
		Steps:          steps,
		TimeoutSeconds: 480, Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}
