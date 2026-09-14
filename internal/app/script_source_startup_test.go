package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SEC-07: initialization must not hand source authority to live handlers
// before preparation recovery succeeds; unreadable recovery state fails startup.
func TestInitializeScriptSourceReferencesRequiresRecovery(t *testing.T) {
	for failAt := 0; failAt <= 3; failAt++ {
		store := &scriptSourceStartupStore{failAt: failAt}
		authority, err := initializeExecutionSourceReferences(context.Background(), store)
		fail, calls := failAt != 0, failAt
		if !fail {
			calls = 3
		}
		if (err != nil) != fail || (authority == nil) != fail || store.calls != calls {
			t.Fatalf(
				"source startup failAt=%d: authority=%t calls=%d error=%v",
				failAt,
				authority != nil,
				store.calls,
				err,
			)
		}
	}
}

// Unused Store operations intentionally have no implementation: startup with
// an empty preparation range must not issue unrelated reads or writes.
type scriptSourceStartupStore struct {
	etcd.Store
	failAt int
	calls  int
}

func (store *scriptSourceStartupStore) Range(_ context.Context, request etcd.RangeRequest) (*etcd.RangeResult, error) {
	store.calls++
	expected := []string{"/v1/staging/task-secret-pin-sets/", "/v1/indexes/task-secret-pin-sets/releasing/",
		scriptsourcereference.PreparationPrefix}
	if store.calls > len(expected) || request.Prefix != expected[store.calls-1] ||
		request.Limit <= 0 || request.Limit > 16 {
		return nil, errs.New(errs.KindInternal, "unexpected source startup range")
	}
	if store.calls == store.failAt {
		return nil, errs.New(errs.KindInternal, "unreadable preparation state")
	}
	return &etcd.RangeResult{ReadRevision: 1}, nil
}
