// Package imagefence carries an image-selection revision through one operation.
// Publication compares this revision atomically with its ordinary write fences.
package imagefence

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const Key = "/v1/runtime/host-image-removal-fence"

type contextKey int

const (
	readKey contextKey = iota
	captureKey
)

type selection struct {
	mu       sync.Mutex
	revision int64
	captured bool
}

// WithScope belongs at the producer boundary, before any image observation.
// Nested producers share the fence instead of silently refreshing stale input.
func WithScope(ctx context.Context) context.Context {
	if _, ok := ctx.Value(readKey).(func() (int64, bool)); ok {
		return ctx
	}
	scope := &selection{}
	ctx = context.WithValue(ctx, readKey, scope.read)
	return context.WithValue(ctx, captureKey, scope.capture)
}

func Capture(ctx context.Context, revision int64) error {
	capture, ok := ctx.Value(captureKey).(func(int64) error)
	if !ok {
		return errs.New(errs.KindInternal, "image selection requires a publication scope")
	}
	return capture(revision)
}

func (scope *selection) capture(revision int64) error {
	if revision < 0 {
		return errs.New(errs.KindInternal, "image selection revision is invalid")
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.captured && scope.revision != revision {
		return errs.New(errs.KindResourceInUse, "host images changed during image selection; retry the operation")
	}
	scope.revision, scope.captured = revision, true
	return nil
}

func Revision(ctx context.Context) (int64, bool) {
	read, ok := ctx.Value(readKey).(func() (int64, bool))
	if !ok {
		return 0, false
	}
	return read()
}

func (scope *selection) read() (int64, bool) {
	scope.mu.Lock()
	defer scope.mu.Unlock()
	return scope.revision, scope.captured
}
