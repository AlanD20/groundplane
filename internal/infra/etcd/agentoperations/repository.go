package agentoperations

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store store }

func New(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "Agent lifecycle execution storage is required")
	}
	return &Repository{store: backend}, nil
}

func (repository *Repository) Load(ctx context.Context, item workexecution.Item, revision int64) (keyvalue.Versioned[Capture], error) {
	if item.Validate() != nil || item.Operation != workexecution.AgentLifecycle && item.Operation != workexecution.AgentSoftware || revision < 0 {
		return keyvalue.Versioned[Capture]{}, errs.New(errs.KindValidationFailed, "Agent lifecycle work is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{InputKey(item.ID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Capture]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[Capture]{}, errs.New(errs.KindStorageUnavailable, "Agent lifecycle input read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return keyvalue.Versioned[Capture]{}, errs.New(errs.KindStateConflict, "Agent lifecycle captured input is missing")
	}
	capture, err := recordcodec.Decode[Capture](read.Values[0].Value, "agent_lifecycle_input")
	if err != nil {
		return keyvalue.Versioned[Capture]{}, err
	}
	if err := capture.Match(item); err != nil {
		return keyvalue.Versioned[Capture]{}, err
	}
	return keyvalue.Versioned[Capture]{Record: capture, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, nil
}
