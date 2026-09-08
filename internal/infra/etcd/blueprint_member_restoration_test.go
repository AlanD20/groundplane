package etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: configured-only Apply creates an applied Environment record, but
// it does not create a serving Release that first-candidate recovery can restore.
func TestBlueprintConfiguredOnlyProjectionDoesNotInventServingPredecessor(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 14)
	task := TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 10), OperationID: ids.NewAt(ids.KindOperation, at, 11),
		Owner: TaskOwner{EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 12)}, Type: TaskUpdate,
		PlanHash: strings.Repeat("4", 64), RenderGeneration: 2,
		Params: map[string]string{
			TaskReleasePublicationParam: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			TaskComposeArtifactParam:    ids.NewAt(ids.KindConfig, at, 13),
		},
	}
	manifest := ReleaseStagedManifest{
		PublicationID: task.Params[TaskReleasePublicationParam], OperationID: task.OperationID,
		Members: []ReleaseStagedMemberRef{{ServiceID: serviceID, ReleaseID: ids.NewAt(ids.KindDeployment, at, 15)}},
	}
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: task.Owner.EnvironmentID,
		DesiredServices: []EnvironmentServiceProjection{{
			EnvironmentID: task.Owner.EnvironmentID,
			Desired:       core.Service{ID: serviceID, Name: "api", Image: "example/api:1"},
		}},
	})
	predecessor := taskMaterializationAppliedPredecessor{
		Present: true, KeyRevision: 21, RevisionID: ids.NewAt(ids.KindTask, at, 16), RenderGeneration: 1,
	}
	authority, _, err := buildBlueprintRestorationAuthority(task, predecessor, manifest, projection.ComposeArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(authority.Candidates) != 1 || authority.Candidates[0].Target != ReleaseRestorationCandidateAbsence {
		t.Fatalf("configured-only predecessor selected %v; want exact first-candidate absence", authority.Candidates)
	}
	// Rationale: replay validation must bind the member map to its sealed
	// witness, not merely accept a fresh hash over a different valid enum.
	authority.Candidates[0].Target = ReleaseRestorationServingPredecessor
	if err := validateReleaseRestorationAuthority(authority); err == nil {
		t.Fatal("configured-only witness accepted a serving restoration target")
	}
}
