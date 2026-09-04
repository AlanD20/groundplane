package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: changing an operator-owned Service from one to three replicas
// must remain a normal desired-state mutation even while a Script targets the
// logical Service; the Script still executes only once per logical release.
func TestServiceMutationAllowsScalingScriptTargetFromOneToThree(t *testing.T) {
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	scripts, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository() error = %v", err)
	}
	script, err := NewScriptRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Slug: "release", ServiceName: target.Record.Desired.Name,
		Body: "printf release", When: core.ScriptPostDeploy,
	})
	if err != nil {
		t.Fatalf("NewScriptRecord() error = %v", err)
	}
	if _, err := scripts.CreateScript(ctx, environment, project, target, script); err != nil {
		t.Fatalf("CreateScript() error = %v", err)
	}

	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	currentProjection, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetEnvironmentComposeProjection() = %#v/%v/%v", currentProjection, found, err)
	}
	desired := target.Record.Desired
	desired.Replicas = 3
	replacement, err := ReplaceServiceDesired(target.Record, desired)
	if err != nil {
		t.Fatalf("ReplaceServiceDesired() error = %v", err)
	}
	fixture := seedDesiredServiceFixture(
		t, ctx, store, environment.Record.ID, desired, "", 970, false, false,
	)
	fixture.Projection.Record.RenderGeneration = currentProjection.Record.RenderGeneration + 1
	fixture.Claim.RenderGeneration = fixture.Projection.Record.RenderGeneration
	marker := directServiceMutationMarker(environment.Record.ID, desired.ID, "script-target-replicas")
	claim := stageDirectServicePublicationForTest(t, ctx, store, fixture, marker, currentProjection.Revision)
	result, err := hierarchy.PublishEnvironmentServiceDesiredRevisionDirect(ctx, EnvironmentServiceDesiredPublication{
		Project: project, Environment: environment, ExpectedHeadRevision: currentProjection.Revision,
		Claim: claim, Revision: EnvironmentDesiredRevisionIdentity{
			EnvironmentID: environment.Record.ID, RevisionID: fixture.Projection.Record.RevisionID,
		}, Projection: fixture.Projection.Record,
		Change: EnvironmentBlueprintServiceChange{Current: &target, Record: replacement}, Marker: marker,
	})
	if err != nil {
		t.Fatalf("PublishEnvironmentServiceDesiredRevisionDirect() error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil ||
		conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("scale Script target publication = %v/%v/%v", outcome, conflict, classifyErr)
	}
	updated, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	service, err := updated.GetService(ctx, desired.ID)
	if err != nil || service.Record.Desired.Replicas != 3 {
		t.Fatalf("scaled Service = %#v/%v", service, err)
	}
}
