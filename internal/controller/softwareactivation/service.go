// Package softwareactivation coordinates durable Controller and Agent update
// Tasks from one verified software preparation. Preparation never activates.
package softwareactivation

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Route = "/software/activations"

type Accepted struct {
	TaskID string `json:"task_id"`
}

type ChildAcceptance struct {
	TaskID string
}

type TickResult struct {
	TaskID     string
	Progressed bool
}

type PreparationStore interface {
	Get(context.Context, string) (etcdstore.Versioned[preparationrecord.Record], error)
}

type RecordStore interface {
	Get(context.Context, string) (etcdstore.Versioned[activationrecord.Record], error)
}

type TaskStore interface {
	CreateSoftwareActivationTask(
		context.Context,
		etcd.TaskRecord,
		activationrecord.Record,
		etcdstore.Versioned[preparationrecord.Record],
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	GetTask(context.Context, string) (etcdstore.Versioned[etcd.TaskRecord], error)
	ClaimNextSoftwareActivation(context.Context, time.Time) (etcdstore.Versioned[etcd.TaskRecord], bool, error)
	ListSoftwareActivationClaims(context.Context) ([]etcdstore.Versioned[etcd.TaskRecord], error)
	CheckpointSoftwareActivation(
		context.Context, string, string, string, activationrecord.Progress, time.Time,
	) (etcdstore.Versioned[activationrecord.Record], error)
	TerminalizeSoftwareActivation(
		context.Context, string, activationrecord.Progress, taskjournal.TaskStatus, time.Time,
	) (etcdstore.Versioned[etcd.TaskRecord], error)
}

type CurrentHealth interface {
	ListHealth(context.Context) ([]localagent.Health, error)
}

type Stager interface {
	// Stage must be idempotent for the frozen preparation and Agent image. A
	// restart can replay it after host publication but before the release
	// checkpoint commits.
	Stage(context.Context, preparationrecord.Record, string) (upgrade.Release, error)
}

// Update ports must replay the same accepted TaskID for a stable key even
// while its idempotency marker is pending or the original publication result
// was unknown. That closes the crash window before the parent checkpoint.
type ControllerUpdates interface {
	DesiredAgentImage(context.Context) (string, error)
	// PublishControllerUpdate returns only a child that pins predecessor. A nil
	// predecessor means the child must freeze the absence of an enrolled Agent.
	PublishControllerUpdate(context.Context, string, string, *upgrade.AgentPredecessor) (ChildAcceptance, error)
}

type AgentUpdates interface {
	// PublishAgentUpdate returns only a child that pins predecessor as its
	// previous image and starting generation.
	PublishAgentUpdate(context.Context, upgrade.AgentPredecessor, string, string) (ChildAcceptance, error)
}

type ServiceDependencies struct {
	Preparations      PreparationStore
	Records           RecordStore
	Tasks             TaskStore
	Health            CurrentHealth
	Stager            Stager
	ControllerUpdates ControllerUpdates
	AgentUpdates      AgentUpdates
	Evidence          requestidempotency.EvidenceRepository
	Intents           *requestidempotency.Coordinator
}

type Service struct {
	preparations      PreparationStore
	records           RecordStore
	tasks             TaskStore
	health            CurrentHealth
	stager            Stager
	controllerUpdates ControllerUpdates
	agentUpdates      AgentUpdates
	evidence          requestidempotency.EvidenceRepository
	intents           *requestidempotency.Coordinator
	now               func() time.Time
}

func NewService(dependencies ServiceDependencies) (*Service, error) {
	if dependencies.Preparations == nil || dependencies.Records == nil || dependencies.Tasks == nil ||
		dependencies.Health == nil || dependencies.Stager == nil || dependencies.ControllerUpdates == nil ||
		dependencies.AgentUpdates == nil || dependencies.Evidence == nil || dependencies.Intents == nil {
		return nil, errs.New(errs.KindInternal, "software activation service dependencies are incomplete")
	}
	return &Service{
		preparations: dependencies.Preparations, records: dependencies.Records, tasks: dependencies.Tasks,
		health: dependencies.Health, stager: dependencies.Stager,
		controllerUpdates: dependencies.ControllerUpdates, agentUpdates: dependencies.AgentUpdates,
		evidence: dependencies.Evidence, intents: dependencies.Intents, now: time.Now,
	}, nil
}

func (service *Service) Start(ctx context.Context, preparationTaskID, key string) (Accepted, error) {
	evidence, locator, err := service.protect(ctx, preparationTaskID, key)
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
	prepared, err := service.preparations.Get(ctx, preparationTaskID)
	if err != nil {
		return Accepted{}, err
	}
	if prepared.Record.Progress.Phase != preparation.PhaseVerified {
		return Accepted{}, errs.New(errs.KindStateConflict, "software preparation is not verified")
	}
	agent, err := service.freezeAgent(ctx, prepared.Record.Source.Selection)
	if err != nil {
		return Accepted{}, err
	}
	stagingImage := ""
	if prepared.Record.Source.Selection.IncludesController() {
		if agent != nil {
			stagingImage = agent.Image
		} else {
			stagingImage, err = service.controllerUpdates.DesiredAgentImage(ctx)
			if err != nil {
				return Accepted{}, err
			}
			if !imageref.IsDigestPinned(stagingImage) {
				return Accepted{}, errs.New(errs.KindStateConflict, "Controller activation has no valid default Agent image")
			}
		}
	}
	now := service.now().UTC()
	input := activationrecord.Input{
		OperationID: ids.New(ids.KindOperation), Preparation: prepared.Record,
		Agent: agent, StagingAgentImage: stagingImage,
	}
	task, inputHash, err := newTask(now, key, input)
	if err != nil {
		return Accepted{}, err
	}
	record, err := activationrecord.NewRecord(task.ID, inputHash, input, now)
	if err != nil {
		return Accepted{}, err
	}
	accepted := Accepted{TaskID: task.ID}
	body, err := json.Marshal(accepted)
	if err != nil {
		return Accepted{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	intent, err := evidence.DurableRecord()
	if err != nil {
		return Accepted{}, err
	}
	defer clear(intent.Ciphertext)
	result, publicationErr := service.tasks.CreateSoftwareActivationTask(
		ctx, task, record, prepared, idempotencyrecord.IdempotencyMarker{
			Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
			Locator: locator, Intent: intent,
			Response: idempotencyrecord.IdempotencyResponse{
				Status:      http.StatusAccepted,
				ContentKind: "application/json",
				Body:        body,
			},
			TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
		},
	)
	return service.resolvePublication(ctx, locator, evidence, result, publicationErr, accepted)
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
