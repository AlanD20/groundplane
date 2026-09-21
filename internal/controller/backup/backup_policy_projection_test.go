package backup

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	testbackupqueries "github.com/AlanD20/groundplane/internal/infra/etcd/backupqueries"
)

// Rationale: the app boundary must preserve source identities, order, and key
// timestamps without exposing repository evidence.
func TestBackupPolicyAPIProjectsExactStableFields(t *testing.T) {
	created := time.Date(2026, 8, 24, 9, 10, 11, 0, time.FixedZone("test", 3600))
	rotated := created.Add(time.Hour)
	got := backupPolicyAPI(testbackupqueries.BackupPolicyProjection{
		Enabled: true, Frequency: "*-*-* 03:15:00", Keep: 7, Encryption: "age",
		ConnectorID: "con_01AAAAAAAAAAAAAAAAAAAAAAAA", AgeRecipient: "age1recipient", KeyEra: 2,
		KeyCreatedAt: created, KeyRotatedAt: rotated,
		Sources: []testbackupqueries.BackupPolicySourceProjection{
			{
				ID:       "spt_01AAAAAAAAAAAAAAAAAAAAAAAA",
				Kind:     core.BackupSourceVolume,
				TargetID: "vol_01AAAAAAAAAAAAAAAAAAAAAAAA",
			},
			{
				ID:       "spt_01BBBBBBBBBBBBBBBBBBBBBBBB",
				Kind:     core.BackupSourceConfig,
				TargetID: "env_01AAAAAAAAAAAAAAAAAAAAAAAA",
			},
		},
	})
	if len(got.Sources) != 2 || got.Sources[0].TargetID != "vol_01AAAAAAAAAAAAAAAAAAAAAAAA" ||
		got.Sources[1].ID != "spt_01BBBBBBBBBBBBBBBBBBBBBBBB" {
		t.Fatalf("sources = %#v", got.Sources)
	}
	if got.KeyCreatedAt != created.UTC().Format(time.RFC3339) ||
		got.KeyRotatedAt != rotated.UTC().Format(time.RFC3339) {
		t.Fatalf("timestamps = %q, %q", got.KeyCreatedAt, got.KeyRotatedAt)
	}
}

// Rationale: an absent singleton is disabled with non-null sources and no
// invalid empty enum or numeric configuration fields.
func TestBackupPolicyAPIProjectsEffectiveDisabledAbsence(t *testing.T) {
	encoded, err := json.Marshal(backupPolicyAPI(testbackupqueries.BackupPolicyProjection{
		Sources: []testbackupqueries.BackupPolicySourceProjection{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"enabled":false,"sources":[],"next_run_at":null}` {
		t.Fatalf("response = %s", encoded)
	}
}
