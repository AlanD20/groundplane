package blueprintrelease

import (
	"bytes"
	"context"
	"maps"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareServiceUnitInput carries only immutable authored inputs and the child
// identity. PrepareServiceUnit reads current runtime and predecessor authority
// itself from one fixed Release-planning revision.
type PrepareServiceUnitInput struct {
	ServiceID     string
	Projection    projectionrecord.EnvironmentComposeProjection
	Scripts       []scriptrecord.Record
	Task          etcd.TaskRecord
	PrefixSteps   []*agentpb.ExecutionStep
	AllocateNamed func(ids.Kind, string) string
	CreatedAt     time.Time
}

// PrepareServiceUnit force-selects exactly one Service from the supplied exact
// Environment projection. It never substitutes the current desired head for
// that projection or accepts caller-constructed predecessor membership.
func (service *Service) PrepareServiceUnit(
	ctx context.Context,
	input PrepareServiceUnitInput,
) (Prepared, error) {
	if err := validateServiceUnitInput(ctx, service, input); err != nil {
		return Prepared{}, err
	}
	scope, err := service.ledger.LoadPlanningScope(ctx, input.Projection.EnvironmentID)
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
	expectedOwner, err := taskjournal.EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		return prepareInput{}, err
	}
	if input.Task.Owner != expectedOwner || input.Task.Target != scope.Environment.Record.ID {
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
	); err != nil || artifact.GetArtifactId() != input.Task.Params[taskcontract.EnvironmentBlueprintArtifactParam] ||
		artifact.GetOwnerId() != input.Projection.EnvironmentID {
		return prepareInput{}, errs.New(errs.KindStateConflict, "Blueprint Service unit artifact is invalid")
	}
	task := input.Task
	task.Params = maps.Clone(input.Task.Params)
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
		Task:              task,
		PrefixSteps:       append([]*agentpb.ExecutionStep(nil), input.PrefixSteps...),
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
	parentID := input.Task.Params[taskjournal.TaskBlueprintParentParam]
	if ctx == nil || service == nil || service.ledger == nil || input.AllocateNamed == nil ||
		ids.Validate(ids.KindService, input.ServiceID) != nil ||
		ids.Validate(ids.KindTask, parentID) != nil || parentID == input.Task.ID ||
		parentID != input.Projection.RevisionID ||
		input.Task.Params[blueprints.EnvironmentDesiredRevisionParam] != input.Projection.RevisionID ||
		input.Task.Actor != taskjournal.TaskActorSystem || input.Task.Executor != taskjournal.TaskExecutorAgent ||
		input.Task.Status != taskjournal.TaskStatusPending ||
		input.Task.RenderGeneration <= 0 || uint64(input.Task.RenderGeneration) != input.Projection.RenderGeneration ||
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
