package apiclient

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
)

func TestBackupPolicyFromGeneratedProjectsNullableNextRunAt(t *testing.T) {
	at := time.Date(2026, 8, 27, 14, 30, 0, 0, time.UTC)
	projected := backupPolicyFromGenerated(generated.BackupPolicy{
		Enabled: true, NextRunAt: &at, Sources: []generated.BackupSource{},
	})
	if projected.NextRunAt == nil || *projected.NextRunAt != "2026-08-27T14:30:00Z" {
		t.Fatalf("next_run_at = %#v", projected.NextRunAt)
	}
	withoutNext := backupPolicyFromGenerated(generated.BackupPolicy{
		Enabled: false, Sources: []generated.BackupSource{},
	})
	if withoutNext.NextRunAt != nil {
		t.Fatalf("disabled next_run_at = %#v", withoutNext.NextRunAt)
	}
}
