package blueprintrelease

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/rollingpredecessor"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
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
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	Task              etcd.TaskRecord
	PrefixSteps       []*agentpb.ExecutionStep
	Artifact          *agentpb.ComposeArtifact
	AllocateNamed     func(ids.Kind, string) string
	CreatedAt         time.Time
}

type Prepared struct {
	Task        etcd.TaskRecord
	Plan        *agentpb.ExecutionPlan
	Publication etcd.BlueprintReleasePublication
}

func (prepared Prepared) Abandon(ctx context.Context) error { return prepared.Publication.Abandon(ctx) }

func (service *Service) validatePrepareInput(ctx context.Context, input prepareInput) error {
	if ctx == nil || service == nil || service.ledger == nil || service.plans == nil || input.AllocateNamed == nil ||
		input.Task.ID == "" || input.Task.OperationID == "" ||
		ids.Validate(ids.KindTask, input.DesiredRevisionID) != nil ||
		input.Projection.RevisionID != input.DesiredRevisionID ||
		input.Task.Params[blueprints.EnvironmentDesiredRevisionParam] != input.DesiredRevisionID ||
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
	if len(candidates) != 1 || !taskjournal.IsBlueprintChild(input.Task.Params) {
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
	task := input.Task
	if task.Params == nil {
		task.Params = make(map[string]string)
	}
	procedure, validProcedure := taskcontract.ParseBlueprintComposeProcedure(
		task.Params[taskcontract.EnvironmentBlueprintProcedureParam],
	)
	if !validProcedure || procedure != taskcontract.BlueprintComposeProcedureNone {
		return Prepared{}, errs.New(errs.KindValidationFailed, "Blueprint Release Task procedure is invalid")
	}
	captured, err := service.stageSelectedRuntime(ctx, input, candidates, task, addresses)
	if err != nil {
		return Prepared{}, err
	}
	return service.finishSelectedRuntime(ctx, input, captured, addresses)
}
