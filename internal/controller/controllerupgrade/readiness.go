package controllerupgrade

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type StorageHealth interface{ Health(context.Context) error }

// Readiness joins the two native listener startup signals with a fresh etcd
// health read. It is constructed before the listeners and coordinator so no
// process can qualify merely because its Task goroutine started first.
type Readiness struct {
	storage               StorageHealth
	http                  chan struct{}
	channel               chan struct{}
	httpOnce, channelOnce sync.Once
}

func NewReadiness(storage StorageHealth) (*Readiness, error) {
	if storage == nil {
		return nil, errs.New(errs.KindInternal, "controller readiness storage is required")
	}
	return &Readiness{storage: storage, http: make(chan struct{}), channel: make(chan struct{})}, nil
}

func (ready *Readiness) MarkHTTPReady() {
	ready.httpOnce.Do(func() { close(ready.http) })
}

func (ready *Readiness) MarkChannelReady() {
	ready.channelOnce.Do(func() { close(ready.channel) })
}

func (ready *Readiness) Ready(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "controller readiness context is required")
	}
	for _, signal := range []<-chan struct{}{ready.http, ready.channel} {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-signal:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return ready.storage.Health(ctx)
}
