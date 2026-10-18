package blueprintattachinputs

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Reader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

// ReadAtRevision keeps captured input and execution ownership in one MVCC view.
func ReadAtRevision(ctx context.Context, store Reader, revisionID, attachID string, revision int64) (etcdstore.Versioned[Generation], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Generation]{}, false, err
	}
	if store == nil || revision <= 0 || ids.Validate(ids.KindTask, revisionID) != nil || ids.Validate(ids.KindAttach, attachID) != nil {
		return etcdstore.Versioned[Generation]{}, false, errs.New(
			errs.KindValidationFailed, "Blueprint Attach input generation identity is invalid",
		)
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{Key(revisionID, attachID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[Generation]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[Generation]{}, false, errs.New(errs.KindInternal, "Blueprint Attach input generation read is missing")
	}
	defer etcdstore.ClearValues(result.Values)
	if len(result.Values) != 1 || result.ReadRevision != revision {
		return etcdstore.Versioned[Generation]{}, false, errs.New(
			errs.KindInternal, "Blueprint Attach input generation read is incomplete",
		)
	}
	if result.Values[0] == nil {
		return etcdstore.Versioned[Generation]{ReadRevision: result.ReadRevision}, false, nil
	}
	value := result.Values[0]
	if value.Key != Key(revisionID, attachID) || value.ModRevision <= 0 || value.ModRevision > result.ReadRevision {
		return etcdstore.Versioned[Generation]{}, false, errs.New(errs.KindInternal, "Blueprint Attach input generation storage identity is invalid")
	}
	generation, err := Decode(value.Value)
	if err != nil || generation.RevisionID != revisionID || generation.AttachID != attachID {
		Clear(&generation)
		if err != nil {
			return etcdstore.Versioned[Generation]{}, false, err
		}
		return etcdstore.Versioned[Generation]{}, false, errs.New(
			errs.KindInternal, "Blueprint Attach input generation key is corrupt",
		)
	}
	return etcdstore.Versioned[Generation]{
		Record: generation, Revision: value.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
