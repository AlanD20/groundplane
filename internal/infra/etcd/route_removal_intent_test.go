package etcd

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: a Route removal retry must retain the exact current/candidate
// applied projection without persisting rendered Caddyfile bytes.
func TestRouteRemovalIntentCodecPinsSuppressionCandidate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 2, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	routeID := ids.NewAt(ids.KindRoute, now, 2)
	projection := Versioned[EnvironmentComposeProjection]{
		Record: EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 3),
			RenderGeneration: 4,
			Routes:           []EnvironmentRouteIdentity{{ID: routeID, Host: "app.example.com", Path: "/app/*"}},
		},
		Revision: 9, ReadRevision: 10,
	}
	projection.Record = withTestEnvironmentComposeArtifact(projection.Record)
	intent, err := NewRouteRemovalIntent(
		ids.NewAt(ids.KindTask, now, 4), environmentID, routeID, 8, &projection, now,
	)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	encoded, err := encodeRouteRemovalIntent(intent)
	if err != nil {
		t.Fatalf("encodeRouteRemovalIntent() error = %v", err)
	}
	decoded, err := decodeRouteRemovalIntent(encoded)
	if err != nil || decoded.CurrentProjectionRevision != 9 || decoded.CandidateProjection == nil ||
		decoded.CandidateProjection.RenderGeneration != 5 || len(decoded.CandidateProjection.SuppressedRoutes) != 1 {
		t.Fatalf("decodeRouteRemovalIntent() = %#v, %v", decoded, err)
	}
	terminalAt := now.Add(time.Minute)
	terminal, err := terminalRouteRemovalIntent(decoded, TaskStatusCompleted, terminalAt)
	if err != nil || terminal.TerminalAt == nil || !terminal.TerminalAt.Equal(terminalAt) {
		t.Fatalf("terminalRouteRemovalIntent() = %#v, %v", terminal, err)
	}
	corrupt := bytes.Replace(encoded, []byte(`"render_generation":5`), []byte(`"render_generation":6`), 1)
	if _, err := decodeRouteRemovalIntent(corrupt); err == nil {
		t.Fatal("decodeRouteRemovalIntent(corrupt candidate) error = nil")
	}
}

// Rationale: removing a Route that was never applied has no runtime candidate
// and must not manufacture a Caddy requirement or render generation.
func TestRouteRemovalIntentWithoutAppliedRouteHasNoProjectionCandidate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 2, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := Versioned[EnvironmentComposeProjection]{
		Record: EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2),
			RenderGeneration: 3,
		},
		Revision: 7, ReadRevision: 7,
	}
	projection.Record = withTestEnvironmentComposeArtifact(projection.Record)
	intent, err := NewRouteRemovalIntent(
		ids.NewAt(ids.KindTask, now, 3), environmentID, ids.NewAt(ids.KindRoute, now, 4), 6, &projection, now,
	)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	if intent.CurrentProjection != nil || intent.CandidateProjection != nil ||
		intent.CurrentProjectionRevision != 0 || intent.RequiresCaddy {
		t.Fatalf("NewRouteRemovalIntent() manufactured runtime state = %#v", intent)
	}
}
