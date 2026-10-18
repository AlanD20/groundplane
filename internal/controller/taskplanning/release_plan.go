package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (resolver *TaskPlanResolver) EnableReleasePlans(ledger *etcd.ReleaseLedger) error {
	if resolver == nil || ledger == nil {
		return errs.New(errs.KindInternal, "release plan dependencies are required")
	}
	resolver.releases = ledger
	return nil
}
