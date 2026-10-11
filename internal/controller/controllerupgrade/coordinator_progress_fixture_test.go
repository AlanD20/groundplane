package controllerupgrade

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type coordinatorProgressJournal struct{}

func (coordinatorProgressJournal) AppendControllerTaskEvent(ctx context.Context, _ taskjournal.TaskEventInput,
	_ time.Time,
) (etcd.TaskEventAppend, error) {
	return etcd.TaskEventAppend{}, ctx.Err()
}

func (h *coordinatorHarness) progress(ctx context.Context) *controllertask.Progress {
	return controllertask.NewProgress(ctx, coordinatorProgressJournal{}, h.claim, func() time.Time { return h.now })
}
