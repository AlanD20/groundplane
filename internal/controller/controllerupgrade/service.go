package controllerupgrade

import (
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"math"
	"net/http"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
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
	CreateControllerSoftwareWork(
		context.Context,
		controllersoftware.Capture,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type ServiceDependencies struct {
	// Nil Catalog explicitly means bootstrap is absent, not that a damaged
	// installation was accepted. Composition must reject other open failures.
	Catalog             ReleaseCatalog
	Unit                BootstrapUnit
	Agents              AgentInventory
	Tasks               UpdateTaskStore
	Records             *controllersoftware.Repository
	Evidence            requestidempotency.EvidenceRepository
	Intents             *requestidempotency.Coordinator
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
	records        *controllersoftware.Repository
	evidence       requestidempotency.EvidenceRepository
	intents        *requestidempotency.Coordinator
	process        upgrade.Digest
	bootstrapImage string
	now            func() time.Time
}

func NewService(dependencies ServiceDependencies) (*Service, error) {
	if dependencies.Unit == nil || dependencies.Agents == nil || dependencies.Tasks == nil ||
		dependencies.Evidence == nil || dependencies.Intents == nil || dependencies.Records == nil || !dependencies.ProcessDigest.Valid() ||
		(dependencies.BootstrapAgentImage != "" && !imageref.IsDigestPinned(dependencies.BootstrapAgentImage)) {
		return nil, errs.New(errs.KindInternal, "controller update service dependencies are invalid")
	}
	return &Service{catalog: dependencies.Catalog, unit: dependencies.Unit, agents: dependencies.Agents,
		tasks: dependencies.Tasks, records: dependencies.Records, evidence: dependencies.Evidence, intents: dependencies.Intents,
		process: dependencies.ProcessDigest, bootstrapImage: dependencies.BootstrapAgentImage, now: time.Now}, nil
}

func (service *Service) UpdateController(
	ctx context.Context,
	release, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"controller update context is required",
		)
	}
	id := upgrade.Digest(release)
	if !id.Valid() {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"controller release digest is invalid",
		)
	}
	evidence, err := service.protect(ctx, release)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: UpdateRoute, Key: key}
	existing, found, err := service.intents.ResolveOperationRootExisting(ctx, service.evidence, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if found {
		return acceptedReplay(existing)
	}
	input, err := service.freeze(ctx, id)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	capture := controllersoftware.Capture{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem), PlanID: ids.New(ids.KindPlan), Input: input, CreatedAt: now}
	raw, err := json.Marshal(apiTypes.TaskAccepted{TaskID: capture.TaskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	response := idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        raw,
	}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	result, err := service.tasks.CreateControllerSoftwareWork(ctx, capture, idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending, Locator: locator,
		Intent: intent, Response: response, TaskID: capture.TaskID, CreatedAt: now, UpdatedAt: now,
	})
	return service.resolvePublication(ctx, locator, evidence, result, err, response)
}

func (service *Service) freeze(ctx context.Context, release upgrade.Digest) (upgrade.Input, error) {
	if service.catalog == nil {
		return upgrade.Input{}, bootstrapUnavailable()
	}
	journal, found, err := service.catalog.Current(ctx)
	if err != nil {
		return upgrade.Input{}, err
	}
	if found {
		if err := journal.Validate(); err != nil {
			return upgrade.Input{}, err
		}
		if !journal.Phase.Settled() {
			return upgrade.Input{}, errs.New(errs.KindResourceInUse, "controller update recovery is active")
		}
	}
	manifest, err := service.catalog.Inspect(ctx, release)
	if err != nil {
		return upgrade.Input{}, err
	}
	installed, err := service.catalog.Installed(ctx)
	if err != nil {
		return upgrade.Input{}, err
	}
	if installed != service.process {
		return upgrade.Input{}, errs.New(errs.KindStateConflict, "installed and running Controller differ")
	}
	if err := service.unit.VerifyBootstrap(ctx); err != nil {
		return upgrade.Input{}, err
	}
	agents, err := service.agents.ListHealth(ctx)
	if err != nil {
		return upgrade.Input{}, err
	}
	if len(agents) > 1 {
		return upgrade.Input{}, errs.New(errs.KindStateConflict, "native update requires one local Agent")
	}
	input := upgrade.Input{Release: release, Manifest: manifest, PreviousController: installed}
	if len(agents) == 1 {
		agent := agents[0].Agent
		if agent.Phase != localagent.PhaseReady || !agents[0].Healthy || agent.Generation > math.MaxUint64-4 {
			return upgrade.Input{}, errs.New(errs.KindResourceInUse, "local Agent is not ready for coordinated update")
		}
		input.Agent = &upgrade.AgentPredecessor{ID: agent.ID, Image: agent.Image, Generation: agent.Generation}
	}
	if err := input.Validate(); err != nil {
		return upgrade.Input{}, err
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
