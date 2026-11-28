package releasequeries

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
)

// A Release ID must not import another Project's image authority, fall back
// to a failed candidate, or survive expiration of the captured source.
func TestSelectPromotionChecksScopeAndRetainedImage(t *testing.T) {
	for _, scenario := range []string{"sibling", "same environment", "other project", "failed", "expired", "wrong image", "deleting"} {
		t.Run(scenario, func(t *testing.T) {
			intent, marker := nativePublicationFixture(t)
			intent.OperationKind = domain.OperationDeploy
			scope := ReleasePlanningScope{ReadRevision: 42}
			scope.Environment.Record.ID = ids.New(ids.KindEnvironment)
			scope.Project.Record.ID = intent.Workspace.ProjectID
			scope.Tenant.Record.ID = intent.Workspace.TenantID
			terminal := domain.TerminalSummary{ReleaseID: intent.ID, Outcome: domain.StateCompleted,
				FinalServingReleaseID: intent.ID, CompletedAt: intent.CreatedAt}
			retention := domain.RollbackMaterial{ReleaseID: intent.ID, Status: domain.RetentionAvailable,
				References: []string{intent.CandidateWorkload.LocalImageID}, Revision: 1}
			switch scenario {
			case "same environment":
				scope.Environment.Record.ID = intent.EnvironmentID
			case "other project":
				scope.Project.Record.ID = ids.New(ids.KindProject)
			case "failed":
				terminal.Outcome, terminal.FinalServingReleaseID = domain.StateFailed, ""
			case "expired":
				retention.Status = domain.RetentionExpired
			case "wrong image":
				retention.References = []string{intent.RenderInputID}
			}
			encode := func(kind string, value any) []byte {
				raw, err := releases.EncodeReleaseRecord(kind, value)
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			publication, err := releases.EncodeWorkPublication(marker)
			if err != nil {
				t.Fatal(err)
			}
			index, err := json.Marshal(
				releases.ReleaseServiceIndexValue{Schema: 1, PublicationID: marker.PublicationID},
			)
			if err != nil {
				t.Fatal(err)
			}
			environment, err := hierarchy.EncodeEnvironment(hierarchy.EnvironmentRecord{
				ID: intent.EnvironmentID, ProjectID: intent.Workspace.ProjectID, Name: "staging",
				NetworkPool: "10.40.0.0/16", CreatedAt: intent.CreatedAt,
				VolumeDir:         "/var/lib/groundplane/vol/" + intent.Workspace.TenantID + "/" + intent.Workspace.ProjectID + "/" + intent.EnvironmentID,
				ProvisioningState: hierarchy.EnvironmentProvisioningReady, CreateTaskID: ids.New(ids.KindTask),
			})
			if err != nil {
				t.Fatal(err)
			}
			environmentKey := hierarchy.EnvironmentKey(intent.EnvironmentID)
			deletionKey := deletions.TombstoneKey("environment", intent.EnvironmentID)
			retentionKey := releases.ReleaseRetentionKey(intent.ID)
			store := &nativePublicationReadStore{values: map[string][]byte{
				releases.ReleaseIntentStagingKey(marker.PublicationID, intent.ID): encode("release-intent", intent),
				releases.ReleaseCheckpointStagingKey(marker.PublicationID, intent.ID): encode(
					"release-checkpoint",
					domain.Checkpoint{
						ReleaseID: intent.ID, State: domain.StateCompleted, UpdatedAt: intent.CreatedAt,
					},
				),
				releases.ReleasePublicationKey(marker.PublicationID):                               publication,
				releases.ReleaseServiceIndexKey(intent.EnvironmentID, intent.ServiceID, intent.ID): index,
				releases.ReleaseTerminalKey(intent.ID): encode(
					"release-terminal-summary",
					terminal,
				),
				environmentKey: environment, retentionKey: encode("release-retention", retention),
			}}
			if scenario == "deleting" {
				store.values[deletionKey] = []byte("deletion in progress")
			}
			selected, guards, err := NewReader(store).SelectPromotion(t.Context(), scope, intent.ID)
			if scenario != "sibling" {
				if err == nil || selected.ID != "" || len(guards) != 0 {
					t.Fatalf("ineligible source produced authority: %+v %v %v", selected, guards, err)
				}
				return
			}
			want := []keyvalue.Condition{{Key: environmentKey, ModRevision: 40}, {Key: deletionKey},
				{Key: retentionKey, ModRevision: 40}}
			if err != nil || selected.ID != intent.ID || selected.CandidateWorkload != intent.CandidateWorkload ||
				!slices.EqualFunc(guards, want, func(a, b keyvalue.Condition) bool {
					return a.Key == b.Key && a.ModRevision == b.ModRevision
				}) {
				t.Fatalf("successful source lost immutable image authority: %+v %v %v", selected, guards, err)
			}
			for _, revision := range store.revisions {
				if revision != scope.ReadRevision {
					t.Fatalf("promotion mixed snapshots: %v", store.revisions)
				}
			}
		})
	}
}
