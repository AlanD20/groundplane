package imageoperations

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Prefix = "/v1/operations/images/"
const operationTimeout = time.Duration(imagefetch.TimeoutSeconds) * time.Second

func Key(taskID string) string { return Prefix + taskID }

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store Store }

func NewRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "image operation storage is required")
	}
	return &Repository{store: store}, nil
}

func (repository *Repository) Load(ctx context.Context, taskID string) (keyvalue.Versioned[Record], error) {
	return repository.LoadAt(ctx, taskID, 0)
}

func (repository *Repository) LoadAt(
	ctx context.Context,
	taskID string,
	revision int64,
) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindTask, taskID) != nil || revision < 0 {
		return keyvalue.Versioned[Record]{}, invalid()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{Key(taskID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "image operation read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "image operation read is incomplete")
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindRequestNotFound, "image operation is unavailable")
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if record.Input.TaskID != taskID {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "image operation ownership is corrupt")
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

// NewTask captures the resolved content exactly once. The caller joins this
// record with tag-selection authority, resource fences and request replay.
func NewTask(input Input) (Record, workexecution.Task, workexecution.Item, error) {
	ref, err := input.Reference()
	if err != nil {
		return Record{}, workexecution.Task{}, workexecution.Item{}, err
	}
	if input.Fetch != nil {
		plan := *input.Fetch
		input.Fetch = &plan
	}
	record := Record{Input: input, Phase: Ready, UpdatedAt: input.CreatedAt}
	task := workexecution.Task{ID: input.TaskID, Actor: workexecution.OperatorActor,
		Action: input.Action, TargetID: input.Target(), TargetName: input.Target(), Intent: ref,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt,
		Deadline: input.CreatedAt.Add(operationTimeout), ExpectedItems: 1}
	if input.Fetch != nil {
		task.TargetName = input.Fetch.Requested
	}
	item := workexecution.Item{ID: input.WorkItemID, TaskID: task.ID, Operation: workexecution.Image,
		TargetID: task.TargetID, Placement: input.Placement, Input: &task.Intent,
		Claims: []workexecution.Claim{{HostID: input.Placement.HostID, ScopeID: task.TargetID}},
		Phase:  workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline}
	if err := record.Validate(); err != nil {
		return Record{}, workexecution.Task{}, workexecution.Item{}, err
	}
	if err := task.Validate(); err != nil {
		return Record{}, workexecution.Task{}, workexecution.Item{}, err
	}
	return record, task, item, item.Validate()
}
