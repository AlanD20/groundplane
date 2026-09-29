package serviceobservation

import (
	"context"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	testprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Provisioned backing containers have no Release. Status must use their
// acknowledged artifact, reject foreign ownership, and retain the revision fence.
func TestCaptureBackingUsesAcknowledgedOwnership(t *testing.T) {
	fixture := newSourceFixture(t, 80, domain.StrategyRecreate, "", nil, 1)
	fixture.service.Record.BackingNetworkID = "backing-zone"
	input := fixture.render.Record
	artifact := &agentpb.ComposeArtifact{OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: fixture.service.Record.EnvironmentID, Services: []*agentpb.ComposeService{{
			ServiceId: fixture.service.Record.Desired.ID, ComposeName: "database", ExpectedReplicas: 1,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.managed", Value: "true"}, {Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.environment-id", Value: fixture.service.Record.EnvironmentID},
				{Key: "com.groundplane.service-id", Value: fixture.service.Record.Desired.ID},
				{Key: "com.groundplane.plan-id", Value: input.PlanID},
				{Key: "com.groundplane.render-generation", Value: "1"},
			},
		}}}
	encoded, err := proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	applied := testkeyvalue.Versioned[testprojection.EnvironmentComposeProjection]{Revision: 5,
		ReadRevision: fixture.service.ReadRevision, Record: testprojection.EnvironmentComposeProjection{
			EnvironmentID: fixture.service.Record.EnvironmentID, RenderGeneration: 1, ComposeArtifact: encoded,
		}}
	releases := &fakeReleases{applied: &applied}
	captured, ok := capture(context.Background(), releases, fixture.service)
	if !ok || captured.target.RuntimeRole != "backing" || captured.target.ReleaseId != "" || captured.expected != 1 ||
		len(releases.resolveCalls) != 0 {
		t.Fatal("backing observation did not select acknowledged backing runtime")
	}
	applied.Revision++
	changed, ok := capture(context.Background(), releases, fixture.service)
	if !ok || captured.unchanged(changed) {
		t.Fatal("applied runtime revision change was not fenced")
	}
	artifact.Services[0].ExpectedLabels[3].Value = "foreign-service"
	applied.Record.ComposeArtifact, err = proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := capture(context.Background(), releases, fixture.service); ok {
		t.Fatal("accepted foreign backing ownership")
	}
	releases.applied = nil
	if _, ok := capture(context.Background(), releases, fixture.service); ok {
		t.Fatal("invented runtime from desired backing state")
	}
}
