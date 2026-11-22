// Package softwareactivation owns a captured activation graph in one visible
// Task. Preparation never activates and no hidden child Tasks are created.
package softwareactivation

import (
	"context"
	"math"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Route = "/software/activations"

type Accepted struct {
	TaskID string `json:"task_id"`
}

type PreparationStore interface {
	Get(context.Context, string) (etcdstore.Versioned[preparationrecord.Record], error)
}

type TaskStore interface {
	PrepareSoftwareActivationAuthority(
		context.Context,
		activationrecord.Record,
		etcdstore.Versioned[preparationrecord.Record],
	) (workqueue.Change, error)
}

type CurrentHealth interface {
	ListHealth(context.Context) ([]localagent.Health, error)
}

type Stager interface {
	// Stage imports the frozen preparation without changing running software.
	// Interrupted starts use Cleanup, never repeat the import under that attempt.
	Stage(context.Context, preparationrecord.Record, string) (upgrade.Release, error)
	StageAgent(context.Context, preparation.Artifact, int, string) error
	Cleanup(context.Context, preparationrecord.Record) error
}

type ControllerSource interface {
	DesiredAgentImage(context.Context) (string, error)
	ActivationPredecessor(context.Context) (upgrade.Digest, error)
}

type ServiceDependencies struct {
	Submitter         *acceptedaction.Submitter
	Preparations      PreparationStore
	Records           *activationrecord.Repository
	Tasks             TaskStore
	Health            CurrentHealth
	Stager            Stager
	ControllerSource  ControllerSource
	Queue             *workqueue.Repository
	Diagnostics       *executiondiagnostics.Repository
	ControllerRecords *controllersoftware.Repository
	AgentRecords      *agentoperations.Repository
	Console           *consolerelease.Store
	Evidence          requestidempotency.EvidenceRepository
	Intents           *requestidempotency.Coordinator
}

type Service struct {
	submitter         *acceptedaction.Submitter
	preparations      PreparationStore
	records           *activationrecord.Repository
	tasks             TaskStore
	health            CurrentHealth
	stager            Stager
	controllerSource  ControllerSource
	queue             *workqueue.Repository
	diagnostics       *executiondiagnostics.Repository
	controllerRecords *controllersoftware.Repository
	agentRecords      *agentoperations.Repository
	console           *consolerelease.Store
	evidence          requestidempotency.EvidenceRepository
	intents           *requestidempotency.Coordinator
	now               func() time.Time
}

func NewService(dependencies ServiceDependencies) (*Service, error) {
	if dependencies.Submitter == nil ||
		dependencies.Preparations == nil ||
		dependencies.Records == nil ||
		dependencies.Tasks == nil ||
		dependencies.Health == nil ||
		dependencies.Stager == nil ||
		dependencies.ControllerSource == nil ||
		dependencies.Queue == nil ||
		dependencies.Diagnostics == nil ||
		dependencies.ControllerRecords == nil ||
		dependencies.AgentRecords == nil ||
		dependencies.Evidence == nil ||
		dependencies.Intents == nil {
		return nil, errs.New(errs.KindInternal, "software activation service dependencies are incomplete")
	}
	return &Service{
		submitter: dependencies.Submitter, preparations: dependencies.Preparations, records: dependencies.Records, tasks: dependencies.Tasks,
		health: dependencies.Health, stager: dependencies.Stager,
		controllerSource: dependencies.ControllerSource, queue: dependencies.Queue, diagnostics: dependencies.Diagnostics,
		controllerRecords: dependencies.ControllerRecords, agentRecords: dependencies.AgentRecords,
		console:  dependencies.Console,
		evidence: dependencies.Evidence, intents: dependencies.Intents, now: time.Now,
	}, nil
}

func (service *Service) Start(
	ctx context.Context,
	preparationTaskID string,
	after []string,
	key string,
) (Accepted, error) {
	evidence, locator, err := service.protect(ctx, preparationTaskID, after, key)
	if err != nil {
		return Accepted{}, err
	}
	defer evidence.Destroy()
	existing, found, err := service.intents.ResolveOperationRootExisting(ctx, service.evidence, locator, evidence)
	if err != nil {
		return Accepted{}, err
	}
	if found {
		return decodeReplay(existing)
	}
	if ids.Validate(ids.KindTask, preparationTaskID) != nil {
		return Accepted{}, errs.New(errs.KindValidationFailed, "software preparation Task id is invalid")
	}
	at := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionActivate, TargetID: activationrecord.TargetID(), TargetName: "Groundplane software",
		After: append(
			[]string(nil),
			after...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return Accepted{}, err
	}
	defer clear(intent.Ciphertext)
	response, err := acceptedaction.Submit(ctx, service.submitter, acceptedaction.Submission{
		Task: task, Kind: "software-activate", Locator: locator, Intent: intent, Evidence: evidence},
		activationRequest{PreparationTaskID: preparationTaskID})
	if err != nil {
		return Accepted{}, err
	}
	return decodeAccepted(response.Body)

}

func (service *Service) Get(ctx context.Context, taskID string) (etcdstore.Versioned[activationrecord.Record], error) {
	return service.records.Get(ctx, taskID)
}

func (service *Service) freezeAgent(
	ctx context.Context,
	selection preparation.Selection,
) (*upgrade.AgentPredecessor, error) {
	agents, err := service.health.ListHealth(ctx)
	if err != nil {
		return nil, err
	}
	if len(agents) > 1 {
		return nil, errs.New(errs.KindStateConflict, "software activation requires at most one enrolled Agent")
	}
	if len(agents) == 0 {
		if selection.IncludesAgent() {
			return nil, errs.New(errs.KindStateConflict, "Agent activation requires an enrolled Agent")
		}
		return nil, nil
	}
	health := agents[0]
	if health.Agent.Phase != localagent.PhaseReady || !health.Healthy || health.Agent.Generation == 0 ||
		health.Agent.Generation > math.MaxUint64-4 || !imageref.IsDigestPinned(health.Agent.Image) {
		return nil, errs.New(errs.KindResourceInUse, "enrolled Agent is not ready for software activation")
	}
	return &upgrade.AgentPredecessor{
		ID: health.Agent.ID, Image: health.Agent.Image, Generation: health.Agent.Generation,
	}, nil
}
