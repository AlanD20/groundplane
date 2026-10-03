package hierarchydeletion

import (
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/hierarchyattachplan"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// NewRuntime connects the durable hierarchy journal to its captured Attach plans
// and the existing Backup cleanup executor.
func NewRuntime(
	store etcdstore.Store, idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator, backupCleanup BackupCleanupExecutor,
	volumeRoot string, facts *attachments.FactService, plans *taskplanning.TaskPlanResolver,
) (*Service, error) {
	if plans == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy execution plan resolver is required")
	}
	journal, err := etcd.NewHierarchyDeletionRepository(store)
	if err != nil {
		return nil, err
	}
	attachPlans, err := hierarchyattachplan.New(volumeRoot, journal, facts)
	if err != nil {
		return nil, err
	}
	if err := journal.EnableAttachPlanBuilder(attachPlans); err != nil {
		return nil, err
	}
	if err := plans.EnableHierarchyAttachPlans(attachPlans); err != nil {
		return nil, err
	}
	clock := SystemClock{}
	repository, err := NewEtcdRepository(journal, idempotency, coordinator, clock, backupCleanup)
	if err != nil {
		return nil, err
	}
	return NewService(repository, repository, StableIDGenerator{}, clock), nil
}
