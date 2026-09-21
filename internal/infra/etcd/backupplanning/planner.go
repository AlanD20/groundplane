package backupplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type readStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

// Planner captures Backup source identities and configuration at one revision.
// Task publication and operation locking remain with the publishing repository.
type Planner struct {
	store  readStore
	reader *backupruntime.Reader
}

func NewPlanner(store readStore) *Planner {
	return &Planner{store: store, reader: backupruntime.NewReader(store)}
}

type ManualRunSources struct {
	Run          backupruntime.BackupRunRecord
	Owner        taskjournal.TaskOwner
	Lock         backupruntime.BackupOperationLockRecord
	ReadRevision int64
}
