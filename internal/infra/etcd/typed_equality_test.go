package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: a candidate comparison must include lifecycle and fact metadata,
// because omitting either lets publication proceed from changed evidence.
func TestSameBlueprintAttachCandidateRecordChecksAllTypedIdentity(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	left := AttachRecord{
		ID: "attach", EnvironmentID: "environment", Name: "name", BackingProjectID: "project",
		BackingEnvironmentID: "backing-environment", BackingServiceID: "backing-service", BackingNetworkID: "network",
		ServiceID: "service", CredentialAttachID: "credential", GrantAttachIDs: []string{"grant"},
		FactSets: []AttachFactSetMetadata{{GrantAttachID: "grant", Facts: []AttachFactDefinition{{Key: "URL"}}}},
		Status:   core.AttachPending, Operation: AttachOperationProvision, TaskID: "task", CreatedAt: now,
	}
	if !sameBlueprintAttachCandidateRecord(left, left) {
		t.Fatal("equal Attach records were rejected")
	}

	changedStatus := left
	changedStatus.Status = core.AttachReady
	if sameBlueprintAttachCandidateRecord(left, changedStatus) {
		t.Fatal("changed Attach status was accepted")
	}
	changedFacts := left
	changedFacts.FactSets = []AttachFactSetMetadata{
		{GrantAttachID: "grant", Facts: []AttachFactDefinition{{Key: "PASSWORD"}}},
	}
	if sameBlueprintAttachCandidateRecord(left, changedFacts) {
		t.Fatal("changed Attach facts were accepted")
	}

	changedPresence := left
	changedPresence.GrantAttachIDs = []string{}
	if sameBlueprintAttachCandidateRecord(left, changedPresence) {
		t.Fatal("nil and empty Attach grants were treated as equal")
	}
	changedTimestamp := left
	changedTimestamp.CreatedAt = time.Unix(1, 0).In(time.FixedZone("UTC", 0))
	if sameBlueprintAttachCandidateRecord(left, changedTimestamp) {
		t.Fatal("different time representations were treated as equal")
	}
}

// Rationale: service-removal publication must reject changes to nested
// Component configuration even when the projection's top-level fields match.
func TestSameServiceRemovalProjectionChecksTypedComponentConfig(t *testing.T) {
	left := EnvironmentComposeProjection{
		EnvironmentID: "environment", RevisionID: "revision", RenderGeneration: 1,
		NormalizedCompose: []byte("services:\n  api: {image: api}\n"),
		RuntimeFiles:      []core.BlueprintFile{{Path: "api.env", Content: []byte("MODE=live\n")}},
		ServiceExtensions: map[string]core.ServiceExtensionSpec{
			"api": {DependsOn: map[string]core.ServiceDependency{
				"db": {Phases: []core.ServiceDependencyPhase{"deploy"}},
			}},
		},
		Components: []ComponentRecord{{Desired: ComponentDesiredRecord{
			ID: "component", Owner: core.ComponentOwnerEnvironment, OwnerID: "environment",
			Kind: core.ComponentKindIngressCaddy, Config: core.ComponentConfig{
				Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{"zone"}, CaddyfileTemplate: "site"},
			},
		}}},
	}
	if !sameServiceRemovalProjection(left, left) {
		t.Fatal("equal projections were rejected")
	}
	changed := left
	changed.Components = append([]ComponentRecord(nil), left.Components...)
	changed.Components[0].Desired.Config.Caddy = &core.CaddyComponentConfig{
		ZoneIDs:           []string{"zone"},
		CaddyfileTemplate: "changed",
	}
	if sameServiceRemovalProjection(left, changed) {
		t.Fatal("changed Component configuration was accepted")
	}
	changedPresence := left
	changedPresence.ComposeArtifact = []byte{}
	if sameServiceRemovalProjection(left, changedPresence) {
		t.Fatal("nil and empty projection artifacts were treated as equal")
	}
	changedPresence = left
	changedPresence.Components = []ComponentRecord{}
	if sameServiceRemovalProjection(left, changedPresence) {
		t.Fatal("nil and empty Component collections were treated as equal")
	}
	changed = cloneEnvironmentComposeProjection(left)
	changed.NormalizedCompose[0] = 'x'
	if sameServiceRemovalProjection(left, changed) {
		t.Fatal("changed normalized Compose was accepted")
	}
	changed = cloneEnvironmentComposeProjection(left)
	changed.RuntimeFiles[0].Content[0] = 'X'
	if sameServiceRemovalProjection(left, changed) {
		t.Fatal("changed runtime companion was accepted")
	}
	changed = cloneEnvironmentComposeProjection(left)
	extension := changed.ServiceExtensions["api"]
	dependency := extension.DependsOn["db"]
	dependency.Phases[0] = "rollback"
	extension.DependsOn["db"] = dependency
	changed.ServiceExtensions["api"] = extension
	if sameServiceRemovalProjection(left, changed) {
		t.Fatal("changed Service extension was accepted")
	}
}

// Rationale: omitempty restores absent collections as nil after durable
// decode, so cloning must not manufacture non-nil empties that replay rejects.
func TestCloneEnvironmentComposeProjectionPreservesAbsentCollections(t *testing.T) {
	t.Parallel()
	clone := cloneEnvironmentComposeProjection(EnvironmentComposeProjection{})
	if clone.RuntimeFiles != nil || clone.Components != nil || clone.Entries != nil {
		t.Fatalf("clone manufactured optional collections: %#v", clone)
	}
}
