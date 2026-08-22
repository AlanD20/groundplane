package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: the MVP assigns one independent database/role identity to one consuming Service, so accepting
// multiple consumers would make the locked service-name-derived identity ambiguous.
func TestNewPendingAttachRecordRejectsMultipleConsumers(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 22, 22, 0, 0, 0, time.UTC)
	_, err := NewPendingAttachRecord(
		ids.NewAt(ids.KindAttach, now, 1),
		ids.NewAt(ids.KindEnvironment, now, 2),
		"api-db",
		ids.NewAt(ids.KindProject, now, 3),
		ids.NewAt(ids.KindEnvironment, now, 4),
		ids.NewAt(ids.KindService, now, 5),
		ids.NewAt(ids.KindNetwork, now, 6),
		[]string{ids.NewAt(ids.KindService, now, 7), ids.NewAt(ids.KindService, now, 8)},
		nil,
		nil,
		ids.NewAt(ids.KindTask, now, 9),
		now,
	)
	if err == nil {
		t.Fatal("NewPendingAttachRecord() accepted multiple consuming Services")
	}
}
