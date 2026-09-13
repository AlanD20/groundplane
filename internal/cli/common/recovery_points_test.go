package common

import (
	"slices"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// QA: BAK-07, UI-01; pure table projection only, not fixed-revision listing or verified publication.
// Rationale: CLI table formatting must preserve the exact public field order
// while omitting key era only for an unencrypted Recovery Point.
func TestRecoveryPointTableFormatsExactProjection(t *testing.T) {
	t.Parallel()
	headers, rows := RecoveryPointTable([]apiTypes.RecoveryPoint{
		{
			ID: "rp_1", SourceID: "spt_1", SourceKind: apiTypes.BackupSourceVolume,
			TargetID: "vol_1", CreatedAt: "2026-08-27T12:00:00Z", SizeBytes: 123,
			Encrypted: true, KeyEra: 3, Status: apiTypes.RecoveryPointVerified,
		},
		{
			ID: "rp_2", SourceID: "spt_2", SourceKind: apiTypes.BackupSourceConfig,
			TargetID: "env_1", CreatedAt: "2026-08-27T11:00:00Z", SizeBytes: 45,
			Status: apiTypes.RecoveryPointVerified,
		},
	})
	wantHeaders := []string{
		"ID", "SOURCE_ID", "SOURCE_KIND", "TARGET_ID", "CREATED_AT",
		"SIZE_BYTES", "ENCRYPTED", "KEY_ERA", "STATUS",
	}
	wantRows := [][]string{
		{"rp_1", "spt_1", "volume", "vol_1", "2026-08-27T12:00:00Z", "123", "true", "3", "verified"},
		{"rp_2", "spt_2", "config", "env_1", "2026-08-27T11:00:00Z", "45", "false", "", "verified"},
	}
	if !slices.Equal(headers, wantHeaders) || len(rows) != len(wantRows) ||
		!slices.Equal(rows[0], wantRows[0]) || !slices.Equal(rows[1], wantRows[1]) {
		t.Fatalf("Recovery Point table = %#v / %#v", headers, rows)
	}
}
