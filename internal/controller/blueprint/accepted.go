package blueprint

import (
	"context"
	"net/http"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedBlueprint struct {
	Bundle           core.BlueprintBundle
	ExpectedRevision string
	PreserveRoutes   bool
	Locator          idempotency.IdempotencyLocator
	Intent           idempotency.ProtectedIntentRecord
	SecretIDs        map[string]string
}

func blueprintIntentAddress(
	environmentID string,
	after []string,
	expectedRevision string,
	preserveRoutes bool,
) desiredrevision.IntentAddress {
	return desiredrevision.IntentAddress{Method: http.MethodPut, Route: environmentBlueprintRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path: []requestidempotency.PathBinding{
			{Name: "id", Value: environmentID},
		}, After: after, ExpectedRevision: expectedRevision, PreserveRoutes: preserveRoutes}
}

func (service *Service) acceptBlueprint(ctx context.Context, environmentID string, bundle core.BlueprintBundle,
	expectedRevision, idempotencyKey string, preserveRoutes bool, after []string,
) (idempotency.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(
		ctx,
		blueprintIntentAddress(environmentID, after, expectedRevision, preserveRoutes),
		bundle,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.Durable.Ciphertext)
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    http.MethodPut,
		Route:     environmentBlueprintRoute,
		Key:       idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if existing {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	tenant, err := service.authoredOwnerTenant(ctx, project.Record)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	// Syntax and stable Secret identity are operator inputs. Values, images,
	// runtime facts, current desired head and resource reservations remain deferred.
	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{EnvironmentID: environmentID,
		Tenant: tenant.Record.Slug, Project: project.Record.Slug, Environment: environment.Record.Name}, bundle)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	secretIDs := make(map[string]string)
	var retainedSecrets []string
	for _, entry := range parsed.Extensions.Entries {
		key := entry.Source.SecretRef
		if key == "" {
			continue
		}
		id, err := service.entryGeneration.ResolveSecretID(ctx, project.Record.ID, key)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		secretIDs[key] = id
		retainedSecrets = append(retainedSecrets, id)
	}
	for _, component := range parsed.Extensions.Components {
		if id := component.Settings.SecretID; id != "" {
			retainedSecrets = append(retainedSecrets, id)
		}
	}
	sort.Strings(retainedSecrets)
	retainedSecrets = slices.Compact(retainedSecrets)
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionApply,
		TargetID:   environmentID,
		TargetName: environment.Record.Name,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environmentID,
		},
		After: append(
			[]string(nil),
			after...),
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(service.executionTimeout),
	}
	input := acceptedBlueprint{Bundle: bundle, ExpectedRevision: expectedRevision, PreserveRoutes: preserveRoutes,
		Locator: locator, Intent: evidence.Durable, SecretIDs: secretIDs}
	submission := evidence.Submission(task, "blueprint-apply", locator)
	submission.SecretIDs = retainedSecrets
	return acceptedaction.Submit(ctx, service.acceptedActions, submission, input)
}

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.acceptedActions, "blueprint-apply",
		workexecution.ActionApply, ids.KindEnvironment, service.prepareAcceptedBlueprint)}
}

func (service *Service) prepareAcceptedBlueprint(ctx context.Context, task workqueue.TaskSnapshot, _ int64, _ time.Time,
	input acceptedBlueprint,
) (taskdependencies.Prepared, error) {
	defer clear(input.Intent.Ciphertext)
	for key, expected := range input.SecretIDs {
		actual, err := service.entryGeneration.ResolveSecretID(ctx, task.Task.Scope.ProjectID, key)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		if actual != expected {
			return taskdependencies.Prepared{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Secret identity changed while the Task was queued",
			)
		}
	}
	return service.prepareAuthoredOnce(imagefence.WithScope(ctx), task, input)
}

// TaskAcceptanceIdentity resolves ancestry for an operation that will derive its
// Blueprint only after its prerequisite Tasks finish. It reads no desired head.
func (service *Service) TaskAcceptanceIdentity(
	ctx context.Context,
	environmentID string,
) (workexecution.Scope, string, time.Duration, error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return workexecution.Scope{}, "", 0, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return workexecution.Scope{}, "", 0, err
	}
	scope := workexecution.Scope{
		TenantID:      project.Record.TenantID,
		ProjectID:     project.Record.ID,
		EnvironmentID: environmentID,
	}
	if err := scope.Validate(); err != nil {
		return workexecution.Scope{}, "", 0, err
	}
	return scope, environment.Record.Name, service.executionTimeout, nil
}

// PrepareAcceptedComponentBlueprint compiles a component-specific patch built
// from current authored state after the whole-Task gate, retaining its original
// public request identity and Task rather than submitting another Apply.
func (service *Service) PrepareAcceptedComponentBlueprint(ctx context.Context, task workqueue.TaskSnapshot,
	bundle core.BlueprintBundle, expectedRevision string, locator idempotency.IdempotencyLocator,
	intent idempotency.ProtectedIntentRecord,
) (taskdependencies.Prepared, error) {
	return service.prepareAuthoredOnce(imagefence.WithScope(ctx), task, acceptedBlueprint{
		Bundle: bundle, ExpectedRevision: expectedRevision, PreserveRoutes: true, Locator: locator, Intent: intent})
}
