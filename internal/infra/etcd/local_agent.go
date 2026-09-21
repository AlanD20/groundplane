package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type localAgentRepositoryStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type LocalAgentRepository struct {
	store localAgentRepositoryStore
}

type localAgentEvidence struct {
	record          localagentrecord.LocalAgentRecord
	primaryRevision int64
	readRevision    int64
	singleton       *etcdstore.KeyValue
	primary         *etcdstore.KeyValue
	owner           *etcdstore.KeyValue
	config          *etcdstore.KeyValue
	token           *etcdstore.KeyValue
	digest          *etcdstore.KeyValue
}

func NewLocalAgentRepository(store etcdstore.Store) (*LocalAgentRepository, error) {
	return newLocalAgentRepository(store)
}

func newLocalAgentRepository(store localAgentRepositoryStore) (*LocalAgentRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "local Agent store is required")
	}
	return &LocalAgentRepository{store: store}, nil
}

func agentCredentialNotFound() error {
	return errs.New(errs.KindAgentNotFound, "Agent credential was not found")
}

func errorsIsAgentNotFound(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindAgentNotFound
}
