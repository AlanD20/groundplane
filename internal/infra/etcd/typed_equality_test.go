package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

// Rationale: a candidate comparison must include lifecycle and fact metadata,
// because omitting either lets publication proceed from changed evidence.
func TestSameBlueprintAttachCandidateRecordChecksAllTypedIdentity(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	left := testattachments.Record{
		ID: "attach", EnvironmentID: "environment", Name: "name", BackingProjectID: "project",
		BackingEnvironmentID: "backing-environment", BackingServiceID: "backing-service", BackingNetworkID: "network",
		ServiceID: "service", CredentialAttachID: "credential", GrantAttachIDs: []string{"grant"},
		FactSets: []testattachments.FactSetMetadata{
			{GrantAttachID: "grant", Facts: []testattachments.FactDefinition{{Key: "URL"}}},
		},
		Status: core.AttachPending, Operation: testattachments.AttachOperationProvision, TaskID: "task", CreatedAt: now,
	}
	if !testattachments.SameBlueprintAttachCandidateRecord(left, left) {
		t.Fatal("equal Attach records were rejected")
	}

	changedStatus := left
	changedStatus.Status = core.AttachReady
	if testattachments.SameBlueprintAttachCandidateRecord(left, changedStatus) {
		t.Fatal("changed Attach status was accepted")
	}
	changedFacts := left
	changedFacts.FactSets = []testattachments.FactSetMetadata{
		{GrantAttachID: "grant", Facts: []testattachments.FactDefinition{{Key: "PASSWORD"}}},
	}
	if testattachments.SameBlueprintAttachCandidateRecord(left, changedFacts) {
		t.Fatal("changed Attach facts were accepted")
	}

	changedPresence := left
	changedPresence.GrantAttachIDs = []string{}
	if testattachments.SameBlueprintAttachCandidateRecord(left, changedPresence) {
		t.Fatal("nil and empty Attach grants were treated as equal")
	}
	changedTimestamp := left
	changedTimestamp.CreatedAt = time.Unix(1, 0).In(time.FixedZone("UTC", 0))
	if testattachments.SameBlueprintAttachCandidateRecord(left, changedTimestamp) {
		t.Fatal("different time representations were treated as equal")
	}
}

// Rationale: service-removal publication must reject changes to nested
// Component configuration even when the projection's top-level fields match.
func TestSameServiceRemovalProjectionChecksTypedComponentConfig(t *testing.T) {
	left := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: "environment", RevisionID: "revision", RenderGeneration: 1,
		NormalizedCompose: []byte("services:\n  api: {image: api}\n"),
		RuntimeFiles:      []core.BlueprintFile{{Path: "api.env", Content: []byte("MODE=live\n")}},
		ServiceExtensions: map[string]core.ServiceExtensionSpec{
			"api": {DependsOn: map[string]core.ServiceDependency{
				"db": {Phases: []core.ServiceDependencyPhase{"deploy"}},
			}},
		},
		Components: []testcomponents.Record{{Desired: testcomponents.DesiredRecord{
			ID: "component", Owner: core.ComponentOwnerEnvironment, OwnerID: "environment",
			Kind: core.ComponentKindIngressCaddy, Config: core.ComponentConfig{
				Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{"zone"}, CaddyfileTemplate: "site"},
			},
		}}},
	}
	if !testenvironmentchanges.SameServiceRemovalProjection(left, left) {
		t.Fatal("equal projections were rejected")
	}
	changed := left
	changed.Components = append([]testcomponents.Record(nil), left.Components...)
	changed.Components[0].Desired.Config.Caddy = &core.CaddyComponentConfig{
		ZoneIDs:           []string{"zone"},
		CaddyfileTemplate: "changed",
	}
	if testenvironmentchanges.SameServiceRemovalProjection(left, changed) {
		t.Fatal("changed Component configuration was accepted")
	}
	changedPresence := left
	changedPresence.ComposeArtifact = []byte{}
	if testenvironmentchanges.SameServiceRemovalProjection(left, changedPresence) {
		t.Fatal("nil and empty projection artifacts were treated as equal")
	}
	changedPresence = left
	changedPresence.Components = []testcomponents.Record{}
	if testenvironmentchanges.SameServiceRemovalProjection(left, changedPresence) {
		t.Fatal("nil and empty Component collections were treated as equal")
	}
	changed = testenvironmentprojection.CloneEnvironmentComposeProjection(left)
	changed.NormalizedCompose[0] = 'x'
	if testenvironmentchanges.SameServiceRemovalProjection(left, changed) {
		t.Fatal("changed normalized Compose was accepted")
	}
	changed = testenvironmentprojection.CloneEnvironmentComposeProjection(left)
	changed.RuntimeFiles[0].Content[0] = 'X'
	if testenvironmentchanges.SameServiceRemovalProjection(left, changed) {
		t.Fatal("changed runtime companion was accepted")
	}
	changed = testenvironmentprojection.CloneEnvironmentComposeProjection(left)
	extension := changed.ServiceExtensions["api"]
	dependency := extension.DependsOn["db"]
	dependency.Phases[0] = "rollback"
	extension.DependsOn["db"] = dependency
	changed.ServiceExtensions["api"] = extension
	if testenvironmentchanges.SameServiceRemovalProjection(left, changed) {
		t.Fatal("changed Service extension was accepted")
	}
}

// Rationale: omitempty restores absent collections as nil after durable
// decode, so cloning must not manufacture non-nil empties that replay rejects.
func TestCloneEnvironmentComposeProjectionPreservesAbsentCollections(t *testing.T) {
	t.Parallel()
	clone := testenvironmentprojection.CloneEnvironmentComposeProjection(
		testenvironmentprojection.EnvironmentComposeProjection{},
	)
	if clone.RuntimeFiles != nil || clone.Components != nil || clone.Entries != nil {
		t.Fatalf("clone manufactured optional collections: %#v", clone)
	}
}
