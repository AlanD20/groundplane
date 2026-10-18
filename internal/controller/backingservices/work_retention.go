package backingservices

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (owner *Work) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	return owner.records.PrepareRetention(ctx, view, at, final)
}

func (owner *Work) PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return backingprovisionwork.PrepareAttemptRetirement(view, attempt)
}
