package etcd

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a Route removal retry must retain the exact current/candidate
// desired projection without persisting rendered managed configuration bytes.
func TestRouteRemovalIntentCodecPinsDesiredCandidate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 2, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	routeID := ids.NewAt(ids.KindRoute, now, 2)
	route, err := testroutes.NewRecord(environmentID, core.Route{
		ID: routeID, Host: "app.example.com", Path: "/app/*",
		TargetServiceID: ids.NewAt(ids.KindService, now, 5), TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	applied, err := testenvironmentprojection.ApplyEnvironmentRoute(
		withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 3), RenderGeneration: 3,
		}),
		route,
	)
	if err != nil {
		t.Fatalf("ApplyEnvironmentRoute() error = %v", err)
	}
	projection := testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record:   applied,
		Revision: 9, ReadRevision: 10,
	}
	intent, err := testenvironmentchanges.NewRouteRemovalIntent(
		ids.NewAt(ids.KindTask, now, 4), environmentID, routeID, 8, &projection, now,
	)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	encoded, err := testenvironmentchanges.EncodeRouteRemovalIntent(intent)
	if err != nil {
		t.Fatalf("encodeRouteRemovalIntent() error = %v", err)
	}
	decoded, err := testenvironmentchanges.DecodeRouteRemovalIntent(encoded)
	if err != nil || decoded.CurrentProjectionRevision != 9 || decoded.CandidateProjection == nil ||
		decoded.CandidateProjection.RenderGeneration != 5 || len(decoded.CandidateProjection.DesiredRoutes) != 0 ||
		decoded.CurrentProjection == nil || len(decoded.CurrentProjection.DesiredRoutes) != 1 {
		t.Fatalf("decodeRouteRemovalIntent() = %#v, %v", decoded, err)
	}
	terminalAt := now.Add(time.Minute)
	terminal, err := testenvironmentchanges.TerminalRouteRemovalIntent(
		decoded,
		testtaskjournal.TaskStatusCompleted,
		terminalAt,
	)
	if err != nil || terminal.TerminalAt == nil || !terminal.TerminalAt.Equal(terminalAt) {
		t.Fatalf("terminalRouteRemovalIntent() = %#v, %v", terminal, err)
	}
	corrupt := bytes.Replace(encoded, []byte(`"render_generation":5`), []byte(`"render_generation":6`), 1)
	if _, err := testenvironmentchanges.DecodeRouteRemovalIntent(corrupt); err == nil {
		t.Fatal("decodeRouteRemovalIntent(corrupt candidate) error = nil")
	}
}

// Rationale: removing a Route that was never applied has no runtime candidate
// and must not manufacture a provider requirement or render generation.
func TestRouteRemovalIntentWithoutAppliedRouteHasNoProjectionCandidate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 2, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2),
			RenderGeneration: 3,
		},
		Revision: 7, ReadRevision: 7,
	}
	projection.Record = withTestEnvironmentComposeArtifact(projection.Record)
	intent, err := testenvironmentchanges.NewRouteRemovalIntent(
		ids.NewAt(ids.KindTask, now, 3), environmentID, ids.NewAt(ids.KindRoute, now, 4), 6, &projection, now,
	)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	if intent.CurrentProjection != nil || intent.CandidateProjection != nil ||
		intent.CurrentProjectionRevision != 0 || intent.Provider != nil {
		t.Fatalf("NewRouteRemovalIntent() manufactured runtime state = %#v", intent)
	}
}
