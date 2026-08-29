package runner

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestNewRunnerRemovalTaskUsesCanonicalHostSlot(t *testing.T) {
	at := time.Date(2026, 8, 29, 2, 25, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	record := etcd.RunnerRecord{
		Desired: etcd.RunnerDesiredRecord{
			ID: ids.NewAt(ids.KindRunner, at, 2), OwnerKind: etcd.RunnerOwnerTenant,
			OwnerID: tenantID, TenantID: tenantID,
		},
		RunnerLifecycleRecord: etcd.RunnerLifecycleRecord{
			Allocation: runnerallocation.RunnerHostAllocationRecord{Slot: 0, NetworkCIDR: "10.240.0.0/29"},
		},
	}
	task := newRunnerRemovalTask(record, "runner-remove-test-key", at)
	if got := task.Params[etcd.RunnerHostSlotParam]; got != "0000000000" {
		t.Fatalf("runner host slot task parameter = %q, want canonical fixed width", got)
	}
}
