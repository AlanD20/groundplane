package dnsresolver

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
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
	coordinator *idempotentintent.Coordinator,
) error {
	if ctx == nil || components == nil || tasks == nil || planner == nil || coordinator == nil {
		return errs.New(errs.KindInternal, "platform resolver startup dependencies are required")
	}
	_, found, err := components.GetHostResolutionProjection(ctx)
	if err != nil {
		return err
	}
	if found {
		return nil
	}
	page, err := components.ListPlatformComponents(ctx, etcd.PageRequest{Limit: etcd.MaximumPageLimit})
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
	empty, err := etcd.NewHostResolutionProjectionRecord(inputRevision, nil)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	ensureService := len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy
	task := startupResolverTask(current.Record.Desired.ID, now, ensureService)
	desired, err := etcd.ProjectComponentRecord(current.Record)
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
	task.PlanHash = renderInput.PlanSHA256
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
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost,
			Route: platformComponentUpdateRoute, Key: task.IdempotencyKey,
		},
		Intent: intent.durable,
		Response: etcd.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
		},
		TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	return tasks.PublishPlatformDNSResolverTask(ctx, current, empty, task, renderInput, marker)
}

func startupResolverTask(componentID string, createdAt time.Time, ensureService bool) etcd.TaskRecord {
	steps := []etcd.TaskStepRecord{
		{ID: ids.New(ids.KindStep)},
		{ID: ids.New(ids.KindStep)},
	}
	if ensureService {
		steps = append(steps,
			etcd.TaskStepRecord{ID: ids.New(ids.KindStep)},
			etcd.TaskStepRecord{ID: ids.New(ids.KindStep)},
		)
	}
	return etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: etcd.PlatformTaskOwner(), Actor: etcd.TaskActorSystem,
		Executor: etcd.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan), RenderGeneration: 1,
		Type: etcd.TaskUpdate, Target: componentID,
		Params: map[string]string{
			etcd.TaskResourceKindParam:       etcd.TaskResourceComponent,
			etcd.TaskAutomaticReconcileParam: "true",
		},
		Steps:          steps,
		TimeoutSeconds: 480, Status: etcd.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}
