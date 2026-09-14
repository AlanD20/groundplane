package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: initialization must not hand source authority to live handlers
// before preparation recovery succeeds; unreadable recovery state fails startup.
func TestInitializeScriptSourceReferencesRequiresRecovery(t *testing.T) {
	for _, fail := range []bool{false, true} {
		store := &scriptSourceStartupStore{fail: fail}
		authority, err := initializeExecutionSourceReferences(context.Background(), store)
		if (err != nil) != fail || (authority == nil) != fail || store.calls != 1 {
			t.Fatalf("source startup fail=%t: authority=%t calls=%d error=%v", fail, authority != nil, store.calls, err)
		}
	}
}

// Unused Store operations intentionally have no implementation: startup with
// an empty preparation range must not issue unrelated reads or writes.
type scriptSourceStartupStore struct {
	etcd.Store
	fail  bool
	calls int
}

func (store *scriptSourceStartupStore) Range(_ context.Context, request etcd.RangeRequest) (*etcd.RangeResult, error) {
	store.calls++
	if request.Prefix != scriptsourcereference.PreparationPrefix || request.Limit <= 0 || request.Limit > 16 {
		return nil, errs.New(errs.KindInternal, "unexpected source startup range")
	}
	if store.fail {
		return nil, errs.New(errs.KindInternal, "unreadable preparation state")
	}
	return &etcd.RangeResult{ReadRevision: 1}, nil
}
