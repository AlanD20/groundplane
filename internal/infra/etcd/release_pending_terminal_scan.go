package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A crash after a no-effect Task terminal commit must not strand its release
// fence. Rotate a bounded durable scan instead of depending on an operator replay.
func (repository *TaskRepository) resumeUnassignedReleaseTerminals(ctx context.Context) error {
	cursor, err := repository.store.Get(ctx, releases.PendingTerminalScanCursorKey)
	if err != nil {
		return err
	}
	if cursor == nil || cursor.ReadRevision <= 0 {
		return errs.New(errs.KindInternal, "release terminal scan cursor read is incomplete")
	}
	start, cursorRevision := "", int64(0)
	if cursor.Entry != nil {
		start, cursorRevision = string(cursor.Entry.Value), cursor.Entry.ModRevision
		defer clear(cursor.Entry.Value)
		if !strings.HasPrefix(start, releases.ReleaseFenceSetScopePrefix()) ||
			ids.Validate(ids.KindEnvironment, strings.TrimPrefix(start, releases.ReleaseFenceSetScopePrefix())) != nil {
			return errs.New(errs.KindInternal, "release terminal scan cursor is invalid")
		}
	}
	page, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: releases.ReleaseFenceSetScopePrefix(), StartExclusive: start, Limit: 24},
	)
	if err != nil {
		return err
	}
	if page == nil || page.ReadRevision <= 0 || page.More && len(page.Values) == 0 {
		return errs.New(errs.KindInternal, "release terminal scan page is incomplete")
	}
	defer keyvalue.ClearRangeValues(page.Values)
	for _, value := range page.Values {
		fence, err := releases.DecodeReleaseRecord[releases.ReleaseFenceSet](value.Value, "release-fence-set")
		if err != nil || releases.ReleaseFenceSetKey(fence.EnvironmentID) != value.Key {
			return releases.CorruptReleaseRecord()
		}
		task, err := repository.GetTask(ctx, fence.AttemptTaskID)
		if err != nil {
			return err
		}
		if task.Record.OperationID != fence.OperationID || task.Record.Owner.EnvironmentID != fence.EnvironmentID {
			return releases.CorruptReleaseRecord()
		}
		if !unassignedReleaseTerminal(task.Record, task.Record.Status) {
			continue
		}
		if _, err := repository.finishUnassignedReleaseTerminal(ctx, task); err != nil {
			if kind, _ := errs.KindOf(err); kind == errs.KindStateConflict {
				continue
			}
			return err
		}
	}
	if !page.More && cursor.Entry == nil {
		return nil
	}
	mutation := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: releases.PendingTerminalScanCursorKey}
	if page.More {
		mutation.Type, mutation.Value = keyvalue.MutationPut, []byte(page.Values[len(page.Values)-1].Key)
	}
	transaction, err := repository.store.Transact(
		ctx,
		[]keyvalue.Condition{
			{Key: releases.PendingTerminalScanCursorKey, ModRevision: cursorRevision},
		},
		[]keyvalue.Mutation{mutation},
	)
	keyvalue.ClearValues(transaction.FailureReads)
	return err // A cursor CAS race is harmless: either scanner made bounded progress.
}
