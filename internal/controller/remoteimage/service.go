// Package remoteimage owns explicit, digest-only Fetch on an enrolled remote host.
package remoteimage

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	intent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remoteimages"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Route = "/hosts/{id}/images/fetch"
const timeout = time.Duration(imagefetch.TimeoutSeconds) * time.Second

type Registry interface {
	ResolvePublicImage(ctx context.Context, requested, architecture string) (imagefetch.Plan, error)
}

type Service struct {
	reader    hostbindings.Reader
	placement *hostplacement.Resolver
	queue     *workqueue.Repository
	submitter *acceptedaction.Submitter
	registry  Registry
	intents   *intent.Coordinator
	replays   *etcd.IdempotencyRepository
}

func New(reader hostbindings.Reader, queue *workqueue.Repository, submitter *acceptedaction.Submitter,
	registry Registry, intents *intent.Coordinator, replays *etcd.IdempotencyRepository) (*Service, error) {
	placement, err := hostplacement.New(reader)
	if err != nil {
		return nil, err
	}
	if queue == nil || submitter == nil || registry == nil || intents == nil || replays == nil {
		return nil, errs.New(errs.KindInternal, "Remote Fetch requires native work and registry authority")
	}
	return &Service{reader: reader, placement: placement, queue: queue, submitter: submitter,
		registry: registry, intents: intents, replays: replays}, nil
}

func (service *Service) Fetch(ctx context.Context, hostID, requested, key string,
	after []string) (idempotency.IdempotencyResponse, error) {
	selected, err := remotefetch.Reference(requested)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if ids.Validate(ids.KindHost, hostID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Remote Fetch requires a stable Host ID",
		)
	}
	taskID := ids.New(ids.KindTask)
	if err := workexecution.ValidateTaskDependencies(taskID, after); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	afterValues := make([]intent.Value, len(after))
	for i, task := range after {
		afterValues[i] = intent.String(task)
	}
	version, digest, err := intent.Canonicalize(ctx, intent.CanonicalIntentV1{
		Method: http.MethodPost, Route: Route, Scope: intent.Scope{Kind: intent.ScopePlatform},
		Path: []intent.PathBinding{{Name: "id", Value: hostID}}, Query: intent.Object(),
		Body: intent.JSONBody(intent.Object(
			intent.Field{Name: "image", Value: intent.String(selected)},
			intent.Field{Name: "after", Value: intent.List(afterValues...)},
		)),
	})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	protected, err := service.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost, Route: Route, Key: key,
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if prior, found, err := service.intents.ResolveExisting(ctx, service.replays, locator, protected); err != nil {
		return idempotency.IdempotencyResponse{}, err
	} else if found {
		return intent.CloneResponse(prior.Response), nil
	}
	local, err := hostbindings.ReadLocalIdentity(ctx, service.reader, 0)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	authority, err := service.placement.ResolveRemote(ctx, hostID, local.ReadRevision, workexecution.RemoteImage)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	now := time.Now().UTC()
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionFetch,
		TargetID: hostID, TargetName: selected, After: append([]string(nil), after...), CreatedAt: now, UpdatedAt: now,
		Deadline: now.Add(acceptedaction.QueueTimeout)}
	durable, err := protected.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	return acceptedaction.Submit(ctx, service.submitter, acceptedaction.Submission{
		Task: task, Kind: remotefetch.Kind, Locator: locator, Intent: durable, Evidence: protected,
		Guards: authority.Conditions,
	}, acceptedFetch{Executor: authority.Identity, Image: selected})
}

type acceptedFetch struct {
	Executor workexecution.Identity `json:"executor"`
	Image    string                 `json:"image"`
}

func (service *Service) AcceptedPreparation() taskdependencies.Registration {
	return acceptedaction.Registration(service.submitter, remotefetch.Kind, workexecution.ActionFetch, ids.KindHost,
		service.prepare)
}

func (service *Service) prepare(ctx context.Context, snapshot workqueue.TaskSnapshot,
	revision int64, at time.Time, request acceptedFetch) (taskdependencies.Prepared, error) {
	task := snapshot.Task
	if task.Action != workexecution.ActionFetch || task.TargetID != request.Executor.HostID ||
		task.TargetName != request.Image {
		return taskdependencies.Prepared{}, conflict()
	}
	selected, err := service.placement.ResolveRemote(ctx, task.TargetID, revision, workexecution.RemoteImage)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if selected.Identity != request.Executor {
		return taskdependencies.Prepared{}, conflict()
	}
	plan, err := service.registry.ResolvePublicImage(ctx, request.Image, selected.Platform.Architecture)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if plan.Requested != request.Image || plan.Architecture != selected.Platform.Architecture {
		return taskdependencies.Prepared{}, conflict()
	}
	change, err := remoteimages.PreparePlan(
		remoteimages.Plan{TaskID: task.ID, Executor: selected.Identity, Image: plan},
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	change.Conditions = append(change.Conditions, selected.Conditions...)
	_, digest, err := imagefetch.Encode(plan)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared := workexecution.CloneTask(task)
	ref := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, remotefetch.Kind),
		SHA256: digest,
	}
	prepared.Intent, prepared.ExpectedItems, prepared.UpdatedAt = ref, 1, at
	item := workexecution.Item{
		ID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, remotefetch.Kind), TaskID: task.ID,
		Operation: workexecution.RemoteImage, TargetID: task.TargetID, TargetName: task.TargetName,
		Placement: selected.Placement, Claims: []workexecution.Claim{{HostID: task.TargetID, ScopeID: task.TargetID}},
		Input: &ref, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: acceptedaction.ExecutionDeadline(prepared, at, timeout),
	}
	return taskdependencies.Prepared{Task: prepared, Graph: []workexecution.Item{item}, Authority: change}, nil
}

func conflict() error {
	return errs.New(errs.KindStateConflict, "Remote Fetch Host or immutable image authority changed")
}
