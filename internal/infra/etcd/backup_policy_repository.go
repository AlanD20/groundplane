package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackupPolicyRepository owns protected replacement of the Environment singleton,
// including Connector references, age-key creation, and exact replay bytes.
type BackupPolicyRepository struct {
	*blueprintplanning.BackupPolicyPlanner
	*backuppolicymutations.Repository
	store hierarchyStore
	now   func() time.Time
}

func NewBackupPolicyRepository(store etcdstore.Store) (*BackupPolicyRepository, error) {
	return newBackupPolicyRepository(store)
}

func newBackupPolicyRepository(store hierarchyStore) (*BackupPolicyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup policy store is required")
	}
	repository := &BackupPolicyRepository{store: store, now: time.Now}
	repository.Repository = backuppolicymutations.NewRepository(store, func() time.Time { return repository.now() })
	repository.BackupPolicyPlanner = blueprintplanning.NewBackupPolicyPlanner(store, repository.Repository)
	return repository, nil
}
