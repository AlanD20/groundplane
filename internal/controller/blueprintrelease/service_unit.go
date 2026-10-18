package blueprintrelease

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareServiceUnitInput carries only immutable authored inputs and the work
// identity. PrepareServiceUnit reads current runtime and predecessor authority
// itself from one fixed Release-planning revision.
type PrepareServiceUnitInput struct {
	ServiceID     string
	Projection    projectionrecord.EnvironmentComposeProjection
	Scripts       []scriptrecord.Record
	Files         []taskmaterialization.Record
	Work          workqueue.View
	OperationID   string
	PlanID        string
	AllocateNamed func(ids.Kind, string) string
	CreatedAt     time.Time
}

// PrepareServiceUnit force-selects exactly one Service from the supplied exact
// Environment projection. It never substitutes the current desired head for
// that projection or accepts caller-constructed predecessor membership.
func (service *Service) PrepareServiceUnit(
	ctx context.Context,
	input PrepareServiceUnitInput,
) (prepared Prepared, resultErr error) {
	if err := validateServiceUnitInput(ctx, service, input); err != nil {
		return Prepared{}, err
	}
	conditions, err := service.ledger.BlueprintWorkProjectionConditions(ctx, input.Work, input.Projection)
	if err != nil {
		return Prepared{}, err
	}
	prerequisites, err := service.ledger.BlueprintWorkPrerequisiteConditions(ctx, input.Work, input.Projection.BlueprintRequirements)
	if err != nil {
		return Prepared{}, err
	}
	conditions = append(conditions, prerequisites...)
	defer func() {
		if resultErr == nil {
			prepared.sourceConditions = append(prepared.sourceConditions, conditions...)
		}
	}()
	scope, err := service.ledger.LoadPlanningScopeForCapturedInput(ctx, input.Projection, input.Work.ReadRevision)
	if err != nil {
		return Prepared{}, err
	}
	if err := exactServiceUnitProjection(scope.Compose.Record, input.Projection); err != nil {
		return Prepared{}, err
	}
	captured, err := service.ledger.ReadBlueprintProxyServiceRolloutReference(ctx, input.Projection.EnvironmentID,
		input.Projection.RevisionID, input.ServiceID, scope.ReadRevision)
	if err != nil {
		return Prepared{}, err
	}
	if captured != nil {
		return service.prepareCapturedProxyServiceUnit(ctx, input, scope, *captured)
	}
	replacement, err := service.ledger.ReadBlueprintProxyReplacementImage(ctx, scope.Environment.Record.ID,
		input.Projection.RevisionID, input.ServiceID, scope.ReadRevision)
	if err != nil {
		return Prepared{}, err
	}
	// File ownership is acquired with native work publication, not by making a
	// hidden pending Task. Final preparation captures restoration at that boundary.
	preparedInput, err := service.prepareServiceUnitInput(ctx, input, scope, replacement)
	if err != nil {
		return Prepared{}, err
	}
	return service.prepareSelected(ctx, preparedInput, preparedInput.ServiceChanges)
}

// Both ordinary publication and pre-router capture use this one input owner.
// Scope is a real fixed storage snapshot; projection selection is checked by
// the caller against either acknowledged runtime or the inert candidate root.
func (service *Service) prepareServiceUnitInput(ctx context.Context,
	input PrepareServiceUnitInput, scope releasequeries.ReleasePlanningScope,
	replacement *componentplanning.ProxyReplacementImage,
) (prepareInput, error) {
	expectedScope := workexecution.Scope{TenantID: scope.Project.Record.TenantID,
		ProjectID: scope.Project.Record.ID, EnvironmentID: scope.Environment.Record.ID}
	if input.Work.Task.Task.Scope != expectedScope || input.Work.Item.Item.TargetID != input.ServiceID {
		return prepareInput{}, errs.New(errs.KindStateConflict, "Blueprint Service unit hierarchy changed")
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{input.ServiceID})
	if err != nil {
		return prepareInput{}, err
	}
	if len(planning) != 1 || planning[0].Service.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return prepareInput{}, errs.New(errs.KindStateConflict, "Blueprint Service unit is not runnable")
	}
	if err := service.validateServiceUnitScripts(ctx, scope, input.ServiceID, input.Scripts); err != nil {
		return prepareInput{}, err
	}
	attaches, err := service.ledger.LoadPlanningAttaches(ctx, scope)
	if err != nil {
		return prepareInput{}, err
	}
	current := planning[0].Service
	change, err := serviceUnitDesiredChange(input.Projection, input.ServiceID, current)
	if err != nil {
		return prepareInput{}, err
	}
	workloads, err := service.prepareSelectedWorkloads(
		ctx,
		scope,
		input.Projection,
		[]blueprints.EnvironmentBlueprintServiceChange{change},
		replacement,
	)
	if err != nil {
		return prepareInput{}, err
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		input.Projection.ComposeArtifact,
		artifact,
	); err != nil || ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil ||
		artifact.GetOwnerId() != input.Projection.EnvironmentID {
		return prepareInput{}, errs.New(errs.KindStateConflict, "Blueprint Service unit artifact is invalid")
	}
	preparedInput := prepareInput{
		DesiredRevisionID: input.Projection.RevisionID,
		IntendedAttaches:  attaches,
		Workloads:         workloads,
		Tenant:            scope.Tenant,
		Project:           scope.Project,
		Environment:       scope.Environment,
		Projection:        projectionrecord.CloneEnvironmentComposeProjection(input.Projection),
		ServiceChanges:    []blueprints.EnvironmentBlueprintServiceChange{change},
		Scripts:           append([]scriptrecord.Record(nil), input.Scripts...),
		Files:             taskmaterialization.Clone(input.Files),
		Work:              input.Work,
		OperationID:       input.OperationID,
		PlanID:            input.PlanID,
		Artifact:          proto.Clone(artifact).(*agentpb.ComposeArtifact),
		AllocateNamed:     input.AllocateNamed,
		CreatedAt:         input.CreatedAt,
	}
	if err := service.validatePrepareInput(ctx, preparedInput); err != nil {
		return prepareInput{}, err
	}
	return preparedInput, nil
}

// Native capture and its later publication select the same authored definition;
// the current record contributes runtime intent and the exact publication fence.
func serviceUnitDesiredChange(projection projectionrecord.EnvironmentComposeProjection, serviceID string,
	current etcdstore.Versioned[servicerecord.ServiceRecord],
) (blueprints.EnvironmentBlueprintServiceChange, error) {
	for _, selected := range projection.DesiredServices {
		if selected.Desired.ID != serviceID {
			continue
		}
		candidate, err := servicerecord.ReplaceServiceDesired(current.Record, selected.Desired)
		if err != nil {
			return blueprints.EnvironmentBlueprintServiceChange{}, err
		}
		return blueprints.EnvironmentBlueprintServiceChange{Current: &current, Record: candidate}, nil
	}
	return blueprints.EnvironmentBlueprintServiceChange{}, errs.New(
		errs.KindStateConflict,
		"Blueprint Service unit is absent from its captured input",
	)
}

func validateServiceUnitInput(ctx context.Context, service *Service, input PrepareServiceUnitInput) error {
	task, item := input.Work.Task.Task, input.Work.Item.Item
	if ctx == nil || service == nil || service.ledger == nil || input.AllocateNamed == nil ||
		ids.Validate(ids.KindService, input.ServiceID) != nil ||
		task.Validate() != nil || item.Validate() != nil || input.Work.ReadRevision <= 0 ||
		input.Work.Task.Revision <= 0 || input.Work.Item.Revision <= 0 ||
		task.Action != workexecution.ActionApply || task.ID != input.Projection.RevisionID ||
		task.Scope.EnvironmentID != input.Projection.EnvironmentID || item.TaskID != task.ID ||
		item.Operation != workexecution.Release || item.TargetID != input.ServiceID ||
		item.Placement.Executor != workexecution.Agent || item.AttemptNumber != 0 ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil || item.ForwardRevoked ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil || ids.Validate(ids.KindPlan, input.PlanID) != nil ||
		input.CreatedAt.Before(task.CreatedAt) || !input.CreatedAt.Before(item.Deadline) ||
		!input.CreatedAt.Equal(input.CreatedAt.UTC()) {
		return errs.New(errs.KindValidationFailed, "Blueprint Service unit preparation is invalid")
	}
	return nil
}

func exactServiceUnitProjection(
	current projectionrecord.EnvironmentComposeProjection,
	expected projectionrecord.EnvironmentComposeProjection,
) error {
	currentValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(current)
	if err != nil {
		return err
	}
	defer clear(currentValue)
	expectedValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(expected)
	if err != nil {
		return err
	}
	defer clear(expectedValue)
	if !bytes.Equal(currentValue, expectedValue) {
		return errs.New(errs.KindStateConflict, "Blueprint Service unit projection changed")
	}
	return nil
}

func (service *Service) validateServiceUnitScripts(
	ctx context.Context,
	scope releasequeries.ReleasePlanningScope,
	serviceID string,
	scripts []scriptrecord.Record,
) error {
	expected, err := service.ledger.ListPlanningHookScriptIDs(ctx, scope, serviceID, domain.OperationDeploy)
	if err != nil {
		return err
	}
	actual := make(map[string]struct{}, len(scripts))
	for _, script := range scripts {
		if scriptrecord.ValidateRecord(script) != nil || script.EnvironmentID != scope.Environment.Record.ID ||
			script.ServiceID != serviceID || script.ScriptSetGeneration != scope.Compose.Record.RevisionID {
			return errs.New(errs.KindStateConflict, "Blueprint Service unit Script membership changed")
		}
		if _, duplicate := actual[script.Desired.ID]; duplicate {
			return errs.New(errs.KindStateConflict, "Blueprint Service unit Script membership is duplicated")
		}
		actual[script.Desired.ID] = struct{}{}
	}
	if len(expected) != len(actual) {
		return errs.New(errs.KindStateConflict, "Blueprint Service unit Script membership changed")
	}
	for _, scriptID := range expected {
		if _, exists := actual[scriptID]; !exists {
			return errs.New(errs.KindStateConflict, "Blueprint Service unit Script membership changed")
		}
	}
	return nil
}
