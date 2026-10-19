package workqueue

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	MeasureTransaction(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionBudget, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store Store }

func NewRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "execution store is required")
	}
	return &Repository{store: store}, nil
}

// View fixes all related reads to one revision. It is not an execution grant;
// prepared writes still compare the individual records used for their decision.
type View struct {
	Task         TaskSnapshot
	Item         ItemSnapshot
	ReadRevision int64
}

func (repository *Repository) ReadTask(
	ctx context.Context,
	taskID string,
	revision int64,
) (TaskSnapshot, int64, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || revision < 0 {
		return TaskSnapshot{}, 0, invalid("task read identity or revision is invalid")
	}
	read, err := repository.read(ctx, []string{TaskKey(taskID)}, revision)
	if err != nil {
		return TaskSnapshot{}, 0, err
	}
	if read.Values[0] == nil {
		return TaskSnapshot{}, read.ReadRevision, errs.New(errs.KindTaskNotFound, "task was not found")
	}
	task, err := DecodeTask(read.Values[0].Value)
	if err != nil {
		return TaskSnapshot{}, 0, err
	}
	if task.ID != taskID {
		return TaskSnapshot{}, 0, corrupt()
	}
	return TaskSnapshot{Task: task, Revision: read.Values[0].ModRevision}, read.ReadRevision, nil
}

func (repository *Repository) ReadWork(ctx context.Context, taskID, workID string, revision int64) (View, error) {
	return repository.readWork(ctx, taskID, workID, revision, false)
}

func (repository *Repository) readWork(
	ctx context.Context,
	taskID, workID string,
	revision int64,
	collecting bool,
) (View, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindWorkItem, workID) != nil || revision < 0 {
		return View{}, invalid("execution read identity or revision is invalid")
	}
	read, err := repository.read(ctx, []string{TaskKey(taskID), ItemKey(workID), TaskItemKey(taskID, workID)}, revision)
	if err != nil {
		return View{}, err
	}
	if read.Values[0] == nil {
		return View{}, errs.New(errs.KindTaskNotFound, "task was not found")
	}
	task, err := DecodeTask(read.Values[0].Value)
	if err != nil {
		return View{}, err
	}
	if task.ID != taskID {
		return View{}, corrupt()
	}
	if task.CollectionStartedAt != nil && !collecting {
		return View{}, errs.New(errs.KindTaskNotFound, "Task execution history has expired")
	}
	if collecting && task.CollectionStartedAt == nil {
		return View{}, invalid("Task history collection has not started")
	}
	if read.Values[1] == nil || read.Values[2] == nil {
		return View{}, errs.New(errs.KindStateConflict, "task execution has not been published")
	}
	item, err := DecodeItem(read.Values[1].Value)
	if err != nil {
		return View{}, err
	}
	owner, err := recordcodec.Decode[ClaimOwner](read.Values[2].Value, "execution_owner")
	if err != nil || validateOwner(owner) != nil || task.ID != taskID || item.ID != workID ||
		item.TaskID != taskID || owner.TaskID != taskID || owner.WorkItemID != workID {
		return View{}, corrupt()
	}
	return View{
		Task:         TaskSnapshot{Task: task, Revision: read.Values[0].ModRevision},
		Item:         ItemSnapshot{Item: item, Revision: read.Values[1].ModRevision},
		ReadRevision: read.ReadRevision,
	}, nil
}

type Page struct {
	Items        []ItemSnapshot
	ReadRevision int64
	More         bool
	LastID       string
}

// ListWork pages stable membership, without needing dispatch eligibility or
// diagnostics. The caller carries ReadRevision into subsequent pages.
func (repository *Repository) ListWork(
	ctx context.Context,
	taskID, after string,
	limit int64,
	revision int64,
) (Page, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Page{}, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil || after != "" && ids.Validate(ids.KindWorkItem, after) != nil ||
		limit < 1 || limit > 100 || revision < 0 {
		return Page{}, invalid("execution page request is invalid")
	}
	request := keyvalue.RangeRequest{Prefix: TaskItemsPrefix(taskID), Limit: limit, Revision: revision}
	if after != "" {
		request.StartExclusive = TaskItemKey(taskID, after)
	}
	members, err := repository.store.Range(ctx, request)
	if err != nil {
		return Page{}, err
	}
	if members == nil || members.ReadRevision <= 0 || revision > 0 && members.ReadRevision != revision ||
		int64(len(members.Values)) > limit || members.More && len(members.Values) == 0 {
		return Page{}, corrupt()
	}
	page := Page{
		ReadRevision: members.ReadRevision,
		More:         members.More,
		Items:        make([]ItemSnapshot, 0, len(members.Values)),
	}
	keys := make([]string, 0, len(members.Values))
	owners := make([]ClaimOwner, 0, len(members.Values))
	for _, member := range members.Values {
		owner, err := recordcodec.Decode[ClaimOwner](member.Value, "execution_owner")
		if err != nil || validateOwner(owner) != nil || owner.TaskID != taskID || member.ModRevision <= 0 ||
			member.Key != TaskItemKey(taskID, owner.WorkItemID) ||
			page.LastID != "" && owner.WorkItemID <= page.LastID {
			return Page{}, corrupt()
		}
		owners = append(owners, owner)
		keys = append(keys, ItemKey(owner.WorkItemID))
		page.LastID = owner.WorkItemID
	}
	if len(keys) == 0 {
		return page, nil
	}
	read, err := repository.read(ctx, keys, page.ReadRevision)
	if err != nil {
		return Page{}, err
	}
	for index, value := range read.Values {
		if value == nil {
			return Page{}, corrupt()
		}
		item, err := DecodeItem(value.Value)
		if err != nil || item.ID != owners[index].WorkItemID || item.TaskID != taskID {
			return Page{}, corrupt()
		}
		page.Items = append(page.Items, ItemSnapshot{Item: item, Revision: value.ModRevision})
	}
	return page, nil
}

func (repository *Repository) read(
	ctx context.Context,
	keys []string,
	revision int64,
) (*keyvalue.GetManyResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if len(keys) > 64 {
		result := &keyvalue.GetManyResult{ReadRevision: revision, Values: make([]*keyvalue.KeyValue, 0, len(keys))}
		for offset := 0; offset < len(keys); offset += 64 {
			end := min(offset+64, len(keys))
			page, err := repository.read(ctx, keys[offset:end], result.ReadRevision)
			if err != nil {
				return nil, err
			}
			result.ReadRevision, result.ResponseRevision = page.ReadRevision, page.ResponseRevision
			result.Values = append(result.Values, page.Values...)
		}
		return result, nil
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision ||
		len(read.Values) != len(keys) {
		return nil, corrupt()
	}
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision) {
			return nil, corrupt()
		}
	}
	return read, nil
}

func queueItemID(target workexecution.Placement, key string) (string, error) {
	prefix := QueuePrefix(target)
	if !strings.HasPrefix(key, prefix) {
		return "", corrupt()
	}
	id := strings.TrimPrefix(key, prefix)
	if ids.Validate(ids.KindWorkItem, id) != nil {
		return "", corrupt()
	}
	return id, nil
}
