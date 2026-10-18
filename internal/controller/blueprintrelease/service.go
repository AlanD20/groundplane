package blueprintrelease

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/rollingpredecessor"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	agentregistration "github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

type Service struct {
	agents      *agentregistration.Repository
	images      workloadseal.Resolver
	observer    rollingpredecessor.Observer
	ledger      *etcd.ReleaseLedger
	scripts     *etcd.ScriptRepository
	plans       *taskplanning.TaskPlanResolver
	preparation *taskplanning.ScriptRunnerPreparationService
	artifacts   *taskplanning.ScriptArtifactService
	sources     *scriptsourcepublication.Authority
}

func NewService(
	ledger *etcd.ReleaseLedger,
	scripts *etcd.ScriptRepository,
	plans *taskplanning.TaskPlanResolver,
	artifacts *taskplanning.ScriptArtifactService,
	sources *scriptsourcepublication.Authority,
	agents *agentregistration.Repository,
	images workloadseal.Resolver,
	observer rollingpredecessor.Observer,
) (*Service, error) {
	if ledger == nil || scripts == nil || plans == nil || artifacts == nil || sources == nil || agents == nil ||
		images == nil || observer == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Release dependencies are not configured")
	}
	preparation, err := taskplanning.NewScriptRunnerPreparationService(artifacts, agents, images)
	if err != nil {
		return nil, err
	}
	return &Service{
		ledger:      ledger,
		scripts:     scripts,
		plans:       plans,
		preparation: preparation,
		artifacts:   artifacts,
		sources:     sources,
		agents:      agents,
		images:      images,
		observer:    observer,
	}, nil
}

type prepareInput struct {
	DesiredRevisionID string
	IntendedAttaches  []etcdstore.Versioned[attachrecord.Record]
	Workloads         workloadPreparation
	Tenant            etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Project           etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment       etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Projection        projectionrecord.EnvironmentComposeProjection
	ServiceChanges    []blueprints.EnvironmentBlueprintServiceChange
	Scripts           []scriptrecord.Record
	Files             []taskmaterialization.Record
	Work              workqueue.View
	OperationID       string
	PlanID            string
	Artifact          *agentpb.ComposeArtifact
	AllocateNamed     func(ids.Kind, string) string
	CreatedAt         time.Time
}

type Prepared struct {
	Item             workexecution.Item
	Input            workexecution.Input
	Publication      etcd.BlueprintReleasePublication
	ledger           *etcd.ReleaseLedger
	work             workqueue.View
	secrets          tasksecretpins.Prepared
	sourceConditions []etcdstore.Condition
}

func (prepared Prepared) Abandon(ctx context.Context) error {
	sourceErr := prepared.Publication.Abandon(ctx)
	if prepared.secrets.IsZero() {
		return sourceErr
	}
	return errors.Join(sourceErr, prepared.ledger.AbandonReleaseWorkSecretPins(ctx, prepared.work, prepared.secrets))
}

// Change binds the prerequisite snapshots used by the coordinator to the one
// publication. No hidden Task or generic plan is returned to the caller.
func (prepared Prepared) Change(dependencies []workqueue.ItemSnapshot) (workqueue.Change, error) {
	change, err := prepared.Publication.Change(prepared.work, dependencies)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, prepared.sourceConditions...)
	return change, nil
}

func (prepared *Prepared) Clear() {
	if prepared == nil {
		return
	}
	prepared.Publication.Clear()
	*prepared = Prepared{}
}

func (service *Service) validatePrepareInput(ctx context.Context, input prepareInput) error {
	if ctx == nil || service == nil || service.ledger == nil || service.plans == nil || input.AllocateNamed == nil ||
		input.Work.Task.Task.Validate() != nil || input.Work.Item.Item.Validate() != nil ||
		input.Work.Task.Task.Action != workexecution.ActionApply ||
		input.Work.Item.Item.TaskID != input.Work.Task.Task.ID ||
		input.Work.Item.Item.Operation != workexecution.Release ||
		input.Work.Item.Item.Placement.Executor != workexecution.Agent ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil || ids.Validate(ids.KindPlan, input.PlanID) != nil ||
		ids.Validate(ids.KindTask, input.DesiredRevisionID) != nil ||
		input.Projection.RevisionID != input.DesiredRevisionID ||
		input.Work.Task.Task.ID != input.DesiredRevisionID ||
		!input.CreatedAt.Equal(input.CreatedAt.UTC()) {
		return errs.New(errs.KindValidationFailed, "Blueprint Release preparation is invalid")
	}
	return nil
}

func (service *Service) prepareSelected(
	ctx context.Context,
	input prepareInput,
	candidates []blueprints.EnvironmentBlueprintServiceChange,
) (Prepared, error) {
	if len(candidates) != 1 || candidates[0].Record.Desired.ID != input.Work.Item.Item.TargetID {
		return Prepared{}, errs.New(errs.KindInternal, "Blueprint Release requires one owned Service unit")
	}
	proxyServices := make([]core.Service, len(candidates))
	for i, candidate := range candidates {
		proxyServices[i] = candidate.Record.Desired
		proxyServices[i].Expose = proxyServices[i].ProxyExposures()
	}
	addresses, err := taskplanning.PrepareServiceProxyAddresses(
		ctx,
		service.ledger.Planner,
		input.Projection,
		proxyServices,
	)
	if err != nil {
		return Prepared{}, err
	}
	defer addresses.Clear()
	captured, err := service.stageSelectedRuntime(ctx, input, candidates, addresses)
	if err != nil {
		return Prepared{}, err
	}
	return service.finishSelectedRuntime(ctx, input, captured, addresses)
}
