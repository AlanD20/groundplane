package etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: the credential owner is always an Attach, including self-ownership
// for a newly provisioned credential.
func TestNewPendingAttachRecordRejectsInvalidCredentialOwner(t *testing.T) {
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
		ids.NewAt(ids.KindService, now, 7),
		ids.NewAt(ids.KindService, now, 8),
		nil,
		nil,
		ids.NewAt(ids.KindTask, now, 9),
		now,
	)
	if err == nil {
		t.Fatal("NewPendingAttachRecord() accepted a non-Attach credential owner")
	}
}

// Rationale: Attach names are mutable API labels and etcd index segments, so every write path must
// enforce the same bounded canonical hyphen grammar used by generated and operator-supplied names.
func TestNewPendingAttachRecordRejectsNonCanonicalName(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 22, 22, 0, 0, 0, time.UTC)
	for _, name := range []string{"API DB", "api--db", "-api", strings.Repeat("a", MaximumAttachNameBytes+1)} {
		_, err := NewPendingAttachRecord(
			ids.NewAt(ids.KindAttach, now, 1),
			ids.NewAt(ids.KindEnvironment, now, 2),
			name,
			ids.NewAt(ids.KindProject, now, 3),
			ids.NewAt(ids.KindEnvironment, now, 4),
			ids.NewAt(ids.KindService, now, 5),
			ids.NewAt(ids.KindNetwork, now, 6),
			ids.NewAt(ids.KindService, now, 7),
			ids.NewAt(ids.KindAttach, now, 1),
			nil,
			nil,
			ids.NewAt(ids.KindTask, now, 8),
			now,
		)
		if err == nil {
			t.Fatalf("NewPendingAttachRecord() accepted name %q", name)
		}
	}
}
