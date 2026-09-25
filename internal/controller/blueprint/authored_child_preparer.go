package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Prepare constructs one private Service child from the parent's exact sealed
// runtime projection. Publication remains the coordinator's responsibility;
// it rechecks the desired unit and execution epoch atomically.
func (service *Service) Prepare(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (blueprintcoordinator.PreparedChild, error) {
	if service == nil || ctx == nil || parent.Executor != taskjournal.TaskExecutorBlueprint ||
		parent.Status != taskjournal.TaskStatusRunning || unit.Target.Kind != ids.KindService || unit.Removal {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Service child request is invalid",
		)
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	// Entries, Attaches, Components, Scripts and Requirements have separate unit
	// effects. This preparer must not issue a Release until those effects and
	// their exact runtime inputs can be passed to the child plan.
	if len(input.desired.Input.Entries) != 0 || len(input.desired.Input.Attachments) != 0 ||
		len(input.desired.Input.Components) != 0 || len(input.desired.Input.Scripts) != 0 ||
		len(input.desired.Input.Requires) != 0 {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child prerequisites are not prepared",
		)
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjectionRevision(
		ctx, parent.Owner.EnvironmentID, parent.ID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found || projection.Record.EnvironmentID != parent.Owner.EnvironmentID ||
		projection.Record.RevisionID != parent.ID ||
		projection.Record.RenderGeneration != input.desired.RenderGeneration {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child runtime projection is unavailable",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact, artifact,
	); err != nil || ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child artifact is invalid",
		)
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: parent.Owner.EnvironmentID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam:      parent.ID,
			taskjournal.TaskBlueprintParentParam:            parent.ID,
			taskjournal.TaskMaterializationEnvironmentParam: parent.Owner.EnvironmentID,
			taskcontract.EnvironmentBlueprintArtifactParam:  artifact.GetArtifactId(),
			taskcontract.EnvironmentBlueprintProcedureParam: string(taskcontract.BlueprintComposeProcedureNone),
		},
		TimeoutSeconds: desiredrevision.TaskTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	prepared, err := service.blueprintReleases.PrepareServiceUnit(
		ctx,
		blueprintrelease.PrepareServiceUnitInput{
			ServiceID: unit.Target.ID, Projection: projection.Record,
			Task: child, CreatedAt: createdAt,
			AllocateNamed: func(kind ids.Kind, _ string) string { return ids.New(kind) },
		},
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{
		Task: prepared.Task, ReleasePublication: prepared.Publication,
	}, nil
}

var _ blueprintcoordinator.ChildPreparer = (*Service)(nil)
