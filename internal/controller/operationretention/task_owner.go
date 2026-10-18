package operationretention

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TaskOwner collects the operation's graph-wide capture only after every work
// item and attempt has retired. Per-work collection must not remove this root:
// another member can still need it to authenticate its retained input.
type TaskOwner interface {
	PrepareTaskRetirement(context.Context, workqueue.TaskSnapshot, int64, time.Time) (workqueue.Change, error)
}

type TaskRegistration struct {
	Action workexecution.Action
	Owner  TaskOwner
	Drafts DraftOwner
}

// DraftOwner sweeps inert admission captures that never became accepted Tasks.
// The cursor bounds each pass independently of ordinary completed Task history.
type DraftOwner interface {
	CollectDrafts(context.Context, string, time.Time) (string, error)
}
