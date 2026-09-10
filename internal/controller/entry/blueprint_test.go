package entry

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: candidate Script references and Blueprint export use immutable
// Entry keys, so normalization must never discard them.
func TestEnvironmentEntryProjectionPreservesBlueprintKey(t *testing.T) {
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 11)
	record, err := etcd.NewBlueprintEntryRecord(environmentID, "APP_MODE", core.EnvEntry{
		ID: ids.NewAt(ids.KindEnvEntry, now, 12), Kind: core.EntryKindEnv, Key: "APP_MODE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "acceptance"}, Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 13))
	if err != nil {
		t.Fatal(err)
	}
	projected, err := BlueprintProjection(
		[]etcd.Versioned[etcd.EntryRecord]{{Record: record, Revision: 1, ReadRevision: 1}},
	)
	if err != nil || len(projected) != 1 || projected[0].BlueprintKey != record.BlueprintKey {
		t.Fatalf("BlueprintProjection() = %#v, %v", projected, err)
	}
}
