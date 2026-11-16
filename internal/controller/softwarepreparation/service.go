// Package softwarepreparation admits, queries, and executes durable native
// Tasks that publish inactive Controller and Agent software artifacts.
package softwarepreparation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Route = "/software/preparations"

type Request struct {
	After      []string
	Selection  preparation.Selection
	SourceKind preparation.SourceKind
	Ref        string
	Platform   preparation.Platform
}

type Accepted struct {
	TaskID string `json:"task_id"`
}

type TaskPublisher interface {
	PrepareSoftwarePreparationAuthority(
		context.Context,
		preparationrecord.Capture,
		preparationrecord.Record,
	) (workqueue.Change, error)
}

type ServiceDependencies struct {
	Submitter   *acceptedaction.Submitter
	Preparer    *preparation.Preparer
	Tasks       TaskPublisher
	Records     *preparationrecord.Repository
	Queue       *workqueue.Repository
	Diagnostics *executiondiagnostics.Repository
	Evidence    requestidempotency.EvidenceRepository
	Intents     *requestidempotency.Coordinator
}

type Service struct {
	submitter   *acceptedaction.Submitter
	preparer    *preparation.Preparer
	tasks       TaskPublisher
	records     *preparationrecord.Repository
	queue       *workqueue.Repository
	diagnostics *executiondiagnostics.Repository
	evidence    requestidempotency.EvidenceRepository
	intents     *requestidempotency.Coordinator
	now         func() time.Time
}

func NewService(dependencies ServiceDependencies) (*Service, error) {
	if dependencies.Submitter == nil ||
		dependencies.Preparer == nil ||
		dependencies.Tasks == nil ||
		dependencies.Records == nil ||
		dependencies.Evidence == nil ||
		dependencies.Intents == nil ||
		dependencies.Queue == nil ||
		dependencies.Diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "software preparation service dependencies are incomplete")
	}
	return &Service{
		submitter: dependencies.Submitter, preparer: dependencies.Preparer, tasks: dependencies.Tasks, records: dependencies.Records,
		evidence: dependencies.Evidence, intents: dependencies.Intents, queue: dependencies.Queue, diagnostics: dependencies.Diagnostics, now: time.Now,
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

	at := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionPrepare, TargetID: preparationrecord.TargetID(), TargetName: "Groundplane software",
		After: append(
			[]string(nil),
			request.After...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return Accepted{}, err
	}
	defer clear(intent.Ciphertext)
	response, err := acceptedaction.Submit(ctx, service.submitter, acceptedaction.Submission{
		Task: task, Kind: "software-prepare", Locator: locator, Intent: intent, Evidence: evidence}, request)
	if err != nil {
		return Accepted{}, err
	}
	return decodeAccepted(response.Body)

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
