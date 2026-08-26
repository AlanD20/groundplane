package etcd

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: an Entry removal retry must retain the exact immutable value
// generation and current/candidate applied projections without value bytes.
func TestEntryRemovalIntentCodecPinsGenerationCandidate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 10, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 2)
	record, err := NewEntryRecord(environmentID, core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "production"},
		Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 3))
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	projection := Versioned[EnvironmentComposeProjection]{
		Record: EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 4),
			RenderGeneration: 4, Entries: []EntryRecord{record},
		},
		Revision: 9, ReadRevision: 10,
	}
	intent, err := NewEntryRemovalIntent(
		ids.NewAt(ids.KindTask, now, 5), environmentID, entryID, 8, &projection, now,
	)
	if err != nil {
		t.Fatalf("NewEntryRemovalIntent() error = %v", err)
	}
	encoded, err := encodeEntryRemovalIntent(intent)
	if err != nil {
		t.Fatalf("encodeEntryRemovalIntent() error = %v", err)
	}
	decoded, err := decodeEntryRemovalIntent(encoded)
	if err != nil || decoded.CurrentProjectionRevision != 9 || decoded.CurrentProjection == nil ||
		decoded.CandidateProjection == nil || decoded.CandidateProjection.RenderGeneration != 5 ||
		len(decoded.CurrentProjection.Entries) != 1 || len(decoded.CandidateProjection.Entries) != 0 {
		t.Fatalf("decodeEntryRemovalIntent() = %#v, %v", decoded, err)
	}
	terminalAt := now.Add(time.Minute)
	terminal, err := terminalEntryRemovalIntent(decoded, TaskStatusCompleted, terminalAt)
	if err != nil || terminal.TerminalAt == nil || !terminal.TerminalAt.Equal(terminalAt) {
		t.Fatalf("terminalEntryRemovalIntent() = %#v, %v", terminal, err)
	}
	corrupt := bytes.Replace(encoded, []byte(`"render_generation":5`), []byte(`"render_generation":6`), 1)
	if _, err := decodeEntryRemovalIntent(corrupt); err == nil {
		t.Fatal("decodeEntryRemovalIntent(corrupt candidate) error = nil")
	}
}

// Rationale: deleting an Entry that was never applied has no host cleanup and
// must not manufacture a candidate projection or render generation.
func TestEntryRemovalIntentWithoutAppliedEntryHasNoProjectionCandidate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 11, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := Versioned[EnvironmentComposeProjection]{
		Record: EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2),
			RenderGeneration: 3,
		},
		Revision: 7, ReadRevision: 7,
	}
	intent, err := NewEntryRemovalIntent(
		ids.NewAt(ids.KindTask, now, 3), environmentID, ids.NewAt(ids.KindEnvEntry, now, 4), 6, &projection, now,
	)
	if err != nil {
		t.Fatalf("NewEntryRemovalIntent() error = %v", err)
	}
	if intent.CurrentProjection != nil || intent.CandidateProjection != nil ||
		intent.CurrentProjectionRevision != 0 {
		t.Fatalf("NewEntryRemovalIntent() manufactured runtime state = %#v", intent)
	}
}
