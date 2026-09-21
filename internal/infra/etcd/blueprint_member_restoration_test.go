package etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: configured-only Apply creates an applied Environment record, but
// it does not create a serving Release that first-candidate recovery can restore.
func TestBlueprintConfiguredOnlyProjectionDoesNotInventServingPredecessor(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 14)
	task := TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 10), OperationID: ids.NewAt(ids.KindOperation, at, 11),
		Owner: testtaskjournal.TaskOwner{
			EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 12),
		}, Type: testtaskjournal.TaskUpdate,
		PlanHash: strings.Repeat("4", 64), RenderGeneration: 2,
		Params: map[string]string{
			testreleaserender.TaskReleasePublicationParam: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			testtaskjournal.TaskComposeArtifactParam:      ids.NewAt(ids.KindConfig, at, 13),
		},
	}
	manifest := testreleases.ReleaseStagedManifest{
		PublicationID: task.Params[testreleaserender.TaskReleasePublicationParam], OperationID: task.OperationID,
		Members: []testreleases.ReleaseStagedMemberRef{
			{ServiceID: serviceID, ReleaseID: ids.NewAt(ids.KindDeployment, at, 15)},
		},
	}
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: task.Owner.EnvironmentID,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: task.Owner.EnvironmentID,
			Desired:       core.Service{ID: serviceID, Name: "api", Image: "example/api:1"},
		}},
	})
	predecessor := taskMaterializationAppliedPredecessor{
		Present: true, KeyRevision: 21, RevisionID: ids.NewAt(ids.KindTask, at, 16), RenderGeneration: 1,
	}
	authority, _, err := buildBlueprintAbsenceAuthorityForTest(task, predecessor, manifest, projection.ComposeArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(authority.Candidates) != 1 ||
		authority.Candidates[0].Target != testtaskassignments.ReleaseRestorationCandidateAbsence {
		t.Fatalf("configured-only predecessor selected %v; want exact first-candidate absence", authority.Candidates)
	}
	// Rationale: replay validation must bind the member map to its sealed
	// witness, not merely accept a fresh hash over a different valid enum.
	authority.Candidates[0].Target = testtaskassignments.ReleaseRestorationServingPredecessor
	if err := testtaskassignments.ValidateReleaseRestorationAuthority(authority); err == nil {
		t.Fatal("configured-only witness accepted a serving restoration target")
	}
	authority.Candidates[0].Target = testtaskassignments.ReleaseRestorationCandidateAbsence
	authority.NativePredecessors = nil
	if err := testtaskassignments.ValidateReleaseRestorationAuthority(authority); err == nil {
		t.Fatal("applied metadata substituted for missing native absence authority")
	}
}
