package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	taskRetentionScanKey   = "/v1/runtime/task-retention-scan-cursor"
	taskPruneResumeScanKey = "/v1/runtime/task-prune-resume-cursor"
)

type taskCleanupCursor struct {
	After string `json:"after"`
}
type taskCleanupPage struct {
	page           *keyvalue.RangeResult
	cursorKey      string
	cursorRevision int64
	after          string
}

func (repository *TaskRepository) readTaskCleanupPage(
	ctx context.Context,
	cursorKey, prefix string,
	limit int64,
) (taskCleanupPage, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{cursorKey}})
	if err != nil {
		return taskCleanupPage{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return taskCleanupPage{}, taskjournal.CorruptPruneIntent()
	}
	defer keyvalue.ClearValues(read.Values)
	cursor := taskCleanupCursor{}
	if read.Values[0] != nil {
		if len(read.Values[0].Value) > 1024 {
			return taskCleanupPage{}, taskjournal.CorruptPruneIntent()
		}
		cursor, err = recordcodec.Decode[taskCleanupCursor](read.Values[0].Value, "task-cleanup-scan")
		if err != nil || cursor.After != "" && !strings.HasPrefix(cursor.After, prefix) {
			return taskCleanupPage{}, taskjournal.CorruptPruneIntent()
		}
	}
	page, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: prefix, StartExclusive: cursor.After, Revision: read.ReadRevision, Limit: limit},
	)
	if err != nil {
		return taskCleanupPage{}, err
	}
	if page == nil || page.ReadRevision != read.ReadRevision || len(page.Values) > int(limit) ||
		page.More && len(page.Values) == 0 {
		return taskCleanupPage{}, taskjournal.CorruptPruneIntent()
	}
	return taskCleanupPage{
		page:           page,
		cursorKey:      cursorKey,
		cursorRevision: keyvalue.RevisionOf(read.Values[0]),
		after:          cursor.After,
	}, nil
}

func (repository *TaskRepository) advanceTaskCleanupPage(
	ctx context.Context,
	page taskCleanupPage,
	after string,
) error {
	if after == page.after {
		return nil
	}
	value, err := recordcodec.Encode("task-cleanup-scan", taskCleanupCursor{After: after})
	if err != nil {
		return err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: page.cursorKey, ModRevision: page.cursorRevision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: page.cursorKey, Value: value}},
	)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Task cleanup scan advanced concurrently")
	}
	return nil
}
