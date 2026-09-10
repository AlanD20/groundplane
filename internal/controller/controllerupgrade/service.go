package controllerupgrade

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const UpdateRoute = "/controller/update"

type ReleaseCatalog interface {
	Current(context.Context) (upgrade.Journal, bool, error)
	Inspect(context.Context, upgrade.Digest) (upgrade.Manifest, error)
	Installed(context.Context) (upgrade.Digest, error)
	Candidate(context.Context) (upgrade.Release, bool, error)
	Selected(context.Context) (upgrade.Release, bool, error)
}

type BootstrapUnit interface{ VerifyBootstrap(context.Context) error }
type AgentInventory interface {
	ListHealth(context.Context) ([]localagent.Health, error)
}
type UpdateTaskStore interface {
	CreateTask(context.Context, etcd.TaskRecord, etcd.IdempotencyMarker) (etcd.IdempotencyTransactionResult, error)
	LatestControllerUpdate(context.Context) (etcd.Versioned[etcd.TaskRecord], bool, error)
}

type ServiceDependencies struct {
	// Nil Catalog explicitly means bootstrap is absent, not that a damaged
	// installation was accepted. Composition must reject other open failures.
	Catalog             ReleaseCatalog
	Unit                BootstrapUnit
	Agents              AgentInventory
	Tasks               UpdateTaskStore
	Evidence            idempotentintent.EvidenceRepository
	Intents             *idempotentintent.Coordinator
	ProcessDigest       upgrade.Digest
	BootstrapAgentImage string
}

// Service publishes frozen native Tasks and owns the current release selection.
// It never mutates the host; only the claimed Task coordinator can activate.
type Service struct {
	catalog        ReleaseCatalog
	unit           BootstrapUnit
	agents         AgentInventory
	tasks          UpdateTaskStore
	evidence       idempotentintent.EvidenceRepository
	intents        *idempotentintent.Coordinator
	process        upgrade.Digest
	bootstrapImage string
	now            func() time.Time
}

func NewService(dependencies ServiceDependencies) (*Service, error) {
	if dependencies.Unit == nil || dependencies.Agents == nil || dependencies.Tasks == nil ||
		dependencies.Evidence == nil || dependencies.Intents == nil || !dependencies.ProcessDigest.Valid() ||
		(dependencies.BootstrapAgentImage != "" && !imageref.IsDigestPinned(dependencies.BootstrapAgentImage)) {
		return nil, errs.New(errs.KindInternal, "controller update service dependencies are invalid")
	}
	return &Service{catalog: dependencies.Catalog, unit: dependencies.Unit, agents: dependencies.Agents,
		tasks: dependencies.Tasks, evidence: dependencies.Evidence, intents: dependencies.Intents,
		process: dependencies.ProcessDigest, bootstrapImage: dependencies.BootstrapAgentImage, now: time.Now}, nil
}

func (service *Service) UpdateController(ctx context.Context, release, key string) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "controller update context is required")
	}
	id := upgrade.Digest(release)
	if !id.Valid() {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "controller release digest is invalid")
	}
	evidence, err := service.protect(ctx, release)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := etcd.IdempotencyLocator{ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: UpdateRoute, Key: key}
	existing, found, err := service.intents.ResolveOperationRootExisting(ctx, service.evidence, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if found {
		return acceptedReplay(existing)
	}
	input, err := service.freeze(ctx, id)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	task, err := NewTask(now, key, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	raw, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	response := etcd.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: raw}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	result, err := service.tasks.CreateTask(ctx, task, etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending, Locator: locator,
		Intent: intent, Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	})
	return service.resolvePublication(ctx, locator, evidence, result, err, response)
}

func (service *Service) freeze(ctx context.Context, release upgrade.Digest) (Input, error) {
	if service.catalog == nil {
		return Input{}, bootstrapUnavailable()
	}
	journal, found, err := service.catalog.Current(ctx)
	if err != nil {
		return Input{}, err
	}
	if found {
		if err := journal.Validate(); err != nil {
			return Input{}, err
		}
		if !journal.Phase.Settled() {
			return Input{}, errs.New(errs.KindResourceInUse, "controller update recovery is active")
		}
	}
	manifest, err := service.catalog.Inspect(ctx, release)
	if err != nil {
		return Input{}, err
	}
	installed, err := service.catalog.Installed(ctx)
	if err != nil {
		return Input{}, err
	}
	if installed != service.process {
		return Input{}, errs.New(errs.KindStateConflict, "installed and running Controller differ")
	}
	if err := service.unit.VerifyBootstrap(ctx); err != nil {
		return Input{}, err
	}
	agents, err := service.agents.ListHealth(ctx)
	if err != nil {
		return Input{}, err
	}
	if len(agents) > 1 {
		return Input{}, errs.New(errs.KindStateConflict, "native update requires one local Agent")
	}
	input := Input{Release: release, Manifest: manifest, PreviousController: installed}
	if len(agents) == 1 {
		agent := agents[0].Agent
		if agent.Phase != localagent.PhaseReady || !agents[0].Healthy || agent.Generation > math.MaxUint64-4 {
			return Input{}, errs.New(errs.KindResourceInUse, "local Agent is not ready for coordinated update")
		}
		input.Agent = &upgrade.AgentPredecessor{ID: agent.ID, Image: agent.Image, Generation: agent.Generation}
	}
	if err := input.Validate(); err != nil {
		return Input{}, err
	}
	return input, nil
}

// DesiredAgentImage overlays bootstrap only with a durably qualified release.
// The active release store refuses selection during native recovery.
func (service *Service) DesiredAgentImage(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", errs.New(errs.KindInternal, "Agent image selection context is required")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if service.catalog != nil {
		selected, found, err := service.catalog.Selected(ctx)
		if err != nil {
			return "", err
		}
		if found {
			if err := selected.Validate(); err != nil {
				return "", err
			}
			return selected.Manifest.AgentImage, nil
		}
	}
	return service.bootstrapImage, nil
}

func bootstrapUnavailable() error {
	return errs.New(errs.KindStateConflict, "native Controller update bootstrap is not installed")
}
