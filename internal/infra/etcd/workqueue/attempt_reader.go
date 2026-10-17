package workqueue

import (
	"context"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadAttempt resolves a numbered attempt through its durable work membership.
// It does not require a live slot or current executor: finished attempts remain
// inspectable after restart and supply the exact result for explicit Retry.
func (repository *Repository) ReadAttempt(ctx context.Context, taskID, workID string,
	number uint64, revision int64,
) (View, AttemptSnapshot, error) {
	view, err := repository.ReadWork(ctx, taskID, workID, revision)
	if err != nil {
		return View{}, AttemptSnapshot{}, err
	}
	if number == 0 || number > view.Item.Item.AttemptNumber {
		return View{}, AttemptSnapshot{}, errs.New(errs.KindStateConflict, "requested execution attempt does not exist")
	}
	index, err := repository.read(ctx, []string{AttemptIndexKey(workID, number)}, view.ReadRevision)
	if err != nil {
		return View{}, AttemptSnapshot{}, err
	}
	if index.Values[0] == nil {
		return View{}, AttemptSnapshot{}, corrupt()
	}
	owner, err := decodeAttemptOwner(index.Values[0].Value, taskID, workID)
	if err != nil {
		return View{}, AttemptSnapshot{}, err
	}
	read, err := repository.read(ctx, []string{AttemptKey(owner.AttemptID)}, view.ReadRevision)
	if err != nil {
		return View{}, AttemptSnapshot{}, err
	}
	attempt, err := indexedAttempt(read.Values[0], owner, number)
	return view, attempt, err
}

type AttemptPage struct {
	Attempts     []AttemptSnapshot
	ReadRevision int64
	LastNumber   uint64
	More         bool
}

// ListAttempts keeps subsequent pages at the first page's read revision. Events
// and worker slots are deliberately not involved in reconstructing attempts.
func (repository *Repository) ListAttempts(ctx context.Context, taskID, workID string,
	after uint64, limit int64, revision int64,
) (AttemptPage, error) {
	_, page, err := repository.listAttempts(ctx, taskID, workID, after, limit, revision, false)
	return page, err
}

// CollectionAttempts reads the oldest remaining batch. Unlike a public history
// page, earlier batches may already be gone; the Task collection fence prevents
// this view from ever authorizing Retry or execution.
func (repository *Repository) CollectionAttempts(ctx context.Context, taskID, workID string) (View, AttemptPage, error) {
	// Leave room for operation-owned results and independently bounded stage,
	// event and loss records in the same retention transaction.
	return repository.listAttempts(ctx, taskID, workID, 0, 8, 0, true)
}

func (repository *Repository) listAttempts(ctx context.Context, taskID, workID string,
	after uint64, limit int64, revision int64, collecting bool,
) (View, AttemptPage, error) {
	if limit < 1 || limit > 100 {
		return View{}, AttemptPage{}, invalid("attempt page size must be between 1 and 100")
	}
	view, err := repository.readWork(ctx, taskID, workID, revision, collecting)
	if err != nil {
		return View{}, AttemptPage{}, err
	}
	if after > view.Item.Item.AttemptNumber {
		return View{}, AttemptPage{}, invalid("attempt cursor exceeds the captured work history")
	}
	request := keyvalue.RangeRequest{Prefix: AttemptIndexPrefix(workID), Limit: limit, Revision: view.ReadRevision}
	if after != 0 {
		request.StartExclusive = AttemptIndexKey(workID, after)
	}
	index, err := repository.store.Range(ctx, request)
	if err != nil {
		return View{}, AttemptPage{}, err
	}
	if index == nil || index.ReadRevision != view.ReadRevision || int64(len(index.Values)) > limit ||
		index.More && len(index.Values) == 0 {
		return View{}, AttemptPage{}, corrupt()
	}
	page := AttemptPage{ReadRevision: view.ReadRevision, LastNumber: after, More: index.More,
		Attempts: make([]AttemptSnapshot, 0, len(index.Values))}
	keys := make([]string, 0, len(index.Values))
	owners := make([]Slot, 0, len(index.Values))
	numbers := make([]uint64, 0, len(index.Values))
	for _, entry := range index.Values {
		number, err := strconv.ParseUint(strings.TrimPrefix(entry.Key, request.Prefix), 10, 64)
		if err != nil || number == 0 || number <= page.LastNumber || !collecting && number-page.LastNumber != 1 ||
			number > view.Item.Item.AttemptNumber || entry.Key != AttemptIndexKey(workID, number) ||
			entry.ModRevision <= 0 || entry.ModRevision > view.ReadRevision {
			return View{}, AttemptPage{}, corrupt()
		}
		owner, err := decodeAttemptOwner(entry.Value, taskID, workID)
		if err != nil {
			return View{}, AttemptPage{}, err
		}
		owners = append(owners, owner)
		numbers = append(numbers, number)
		keys = append(keys, AttemptKey(owner.AttemptID))
		page.LastNumber = number
	}
	if !collecting && page.More != (page.LastNumber < view.Item.Item.AttemptNumber) {
		return View{}, AttemptPage{}, corrupt()
	}
	if len(keys) == 0 {
		return view, page, nil
	}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return View{}, AttemptPage{}, err
	}
	for index, value := range read.Values {
		attempt, err := indexedAttempt(value, owners[index], numbers[index])
		if err != nil {
			return View{}, AttemptPage{}, err
		}
		page.Attempts = append(page.Attempts, attempt)
	}
	return view, page, nil
}

func decodeAttemptOwner(value []byte, taskID, workID string) (Slot, error) {
	owner, err := recordcodec.Decode[Slot](value, "execution_slot")
	if err != nil || owner.TaskID != taskID || owner.WorkItemID != workID || ids.Validate(ids.KindAttempt, owner.AttemptID) != nil {
		return Slot{}, corrupt()
	}
	return owner, nil
}

func indexedAttempt(value *keyvalue.KeyValue, owner Slot, number uint64) (AttemptSnapshot, error) {
	if value == nil {
		return AttemptSnapshot{}, corrupt()
	}
	attempt, err := DecodeAttempt(value.Value)
	if err != nil {
		return AttemptSnapshot{}, err
	}
	if attempt.ID != owner.AttemptID || attempt.TaskID != owner.TaskID || attempt.WorkItemID != owner.WorkItemID || attempt.Number != number {
		return AttemptSnapshot{}, corrupt()
	}
	return AttemptSnapshot{Attempt: attempt, Revision: value.ModRevision}, nil
}
