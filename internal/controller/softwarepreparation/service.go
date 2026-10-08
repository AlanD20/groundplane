// Package softwarepreparation admits, queries, and executes durable native
// Tasks that publish inactive Controller and Agent software artifacts.
package softwarepreparation

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Route = "/software/preparations"

type Request struct {
	Selection  preparation.Selection
	SourceKind preparation.SourceKind
	Ref        string
	Platform   preparation.Platform
}

type Accepted struct {
	TaskID string `json:"task_id"`
}

type TaskPublisher interface {
	CreateSoftwarePreparationTask(
		context.Context,
		etcd.TaskRecord,
		preparationrecord.Record,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type RecordStore interface {
	Get(context.Context, string) (etcdstore.Versioned[preparationrecord.Record], error)
	List(context.Context, etcdstore.PageRequest) (etcdstore.Page[preparationrecord.Record], error)
	Checkpoint(
		context.Context,
		string,
		string,
		string,
		preparation.Progress,
		time.Time,
	) (etcdstore.Versioned[preparationrecord.Record], error)
}

type ServiceDependencies struct {
	Preparer *preparation.Preparer
	Tasks    TaskPublisher
	Records  RecordStore
	Evidence requestidempotency.EvidenceRepository
	Intents  *requestidempotency.Coordinator
}

type Service struct {
	preparer *preparation.Preparer
	tasks    TaskPublisher
	records  RecordStore
	evidence requestidempotency.EvidenceRepository
	intents  *requestidempotency.Coordinator
	now      func() time.Time
}

func NewService(dependencies ServiceDependencies) (*Service, error) {
	if dependencies.Preparer == nil || dependencies.Tasks == nil || dependencies.Records == nil ||
		dependencies.Evidence == nil || dependencies.Intents == nil {
		return nil, errs.New(errs.KindInternal, "software preparation service dependencies are incomplete")
	}
	return &Service{
		preparer: dependencies.Preparer, tasks: dependencies.Tasks, records: dependencies.Records,
		evidence: dependencies.Evidence, intents: dependencies.Intents, now: time.Now,
	}, nil
}

func (service *Service) Start(ctx context.Context, request Request, key string) (Accepted, error) {
	evidence, locator, err := service.protect(ctx, request, key)
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

	resolved, err := service.preparer.Resolve(ctx, preparation.ResolveInput{
		Selection: request.Selection, SourceKind: request.SourceKind,
		OriginalRef: request.Ref, Platform: request.Platform,
	})
	if err != nil {
		return Accepted{}, err
	}
	operationID := ids.New(ids.KindOperation)
	input := preparation.Input{OperationID: operationID, Source: resolved}
	if resolved.SourceKind == preparation.SourceRef && resolved.Selection.IncludesController() {
		catalog, err := postgres16protocol.CompiledManagedRelease()
		if err != nil {
			return Accepted{}, err
		}
		input.ControllerToolCatalog, err = json.Marshal(catalog)
		if err != nil {
			return Accepted{}, errs.Wrap(errs.KindInternal, err)
		}
		input.ControllerCatalogSHA256 = sha256Value(input.ControllerToolCatalog)
	}
	now := service.now().UTC()
	task, inputHash, err := newTask(now, key, input)
	if err != nil {
		return Accepted{}, err
	}
	record, err := preparationrecord.NewRecord(task.ID, operationID, inputHash, resolved, now)
	if err != nil {
		return Accepted{}, err
	}
	accepted := Accepted{TaskID: task.ID}
	body, err := json.Marshal(accepted)
	if err != nil {
		return Accepted{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return Accepted{}, err
	}
	defer clear(intent.Ciphertext)
	result, publicationErr := service.tasks.CreateSoftwarePreparationTask(
		ctx, task, record, idempotencyrecord.IdempotencyMarker{
			Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
			Locator: locator, Intent: intent, Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
		},
	)
	return service.resolvePublication(ctx, locator, evidence, result, publicationErr, accepted)
}

func (service *Service) Get(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[preparationrecord.Record], error) {
	return service.records.Get(ctx, taskID)
}

func (service *Service) List(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[preparationrecord.Record], error) {
	return service.records.List(ctx, request)
}
