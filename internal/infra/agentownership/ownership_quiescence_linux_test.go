//go:build linux

package agentownership

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func TestLeaseQuiescenceRequiresLiveLeaseWithoutExecutions(t *testing.T) {
	lease := &Lease{opened: true, held: true, active: make(map[string]workexecution.Attempt)}
	if !lease.Quiescent() {
		t.Fatal("empty live lease was not quiescent")
	}
	lease.active["work"] = workexecution.Attempt{}
	if lease.Quiescent() {
		t.Fatal("active execution ownership was reported quiescent")
	}
	delete(lease.active, "work")
	lease.closed = true
	if lease.Quiescent() {
		t.Fatal("closed ownership lease was reported quiescent")
	}
}
