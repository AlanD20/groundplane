package common

import (
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

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
	if len(headers) != 9 || len(rows) != 2 || rows[0][7] != "3" || rows[1][7] != "" ||
		rows[0][5] != "123" || rows[0][6] != "true" || rows[1][6] != "false" {
		t.Fatalf("Recovery Point table = %#v / %#v", headers, rows)
	}
}
