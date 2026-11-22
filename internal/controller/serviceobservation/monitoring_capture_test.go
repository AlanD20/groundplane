package serviceobservation

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/monitoring"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: collectors retain one opaque authority across an Agent exchange.
// An ABA write must fail the recheck, and caller mutation must not retarget it.
func TestMonitoringCaptureClonesAuthorityAndRejectsChangedBackSource(t *testing.T) {
	fixture := newSourceFixture(t, 200, domain.StrategyRecreate, "", nil, 2)
	volumeID := bindMonitoringVolume(t, &fixture, 220)
	releases := &fakeReleases{fixtures: fixtureMap(fixture), followRevision: true}
	captured, ok := CaptureMonitoring(t.Context(), releases, fixture.service)
	if !ok || captured.ExpectedReplicas() != 2 || captured.Fingerprint() == (monitoring.SourceFingerprint{}) {
		t.Fatal("valid acknowledged monitoring authority was rejected")
	}
	target, volumes := captured.Target(), captured.Volumes()
	if target.DatabaseProbe != nil || len(volumes) != 1 || volumes[0].VolumeId != volumeID ||
		volumes[0].EnvironmentId != fixture.service.Record.EnvironmentID ||
		volumes[0].ComposeProject == "" || volumes[0].VolumeKey != "data" {
		t.Fatalf("monitoring capture leaked or omitted authority: target=%+v volumes=%+v", target, volumes)
	}
	target.Workloads[0].ComposeName = "retargeted"
	volumes[0].DockerName = "foreign"
	if captured.Target().Workloads[0].ComposeName == "retargeted" || captured.Volumes()[0].DockerName == "foreign" {
		t.Fatal("monitoring accessors exposed mutable capture pointers")
	}
	current := fixture.service
	current.ReadRevision++
	if !RecheckMonitoring(t.Context(), releases, current, captured) {
		t.Fatal("unchanged acknowledged authority did not pass a fresh fixed-revision recheck")
	}
	// The exact record value is restored, but its MVCC revision proves an
	// intervening write. It must start a new metrics segment and reject this exchange.
	fixture.runtime.Revision++
	if RecheckMonitoring(t.Context(), releases, current, captured) {
		t.Fatal("changed-and-changed-back acknowledged source passed monitoring recheck")
	}
	changed, ok := CaptureMonitoring(t.Context(), releases, current)
	if !ok || changed.Fingerprint() == captured.Fingerprint() {
		t.Fatal("changed source did not receive a distinct monitoring fingerprint")
	}
}

// Rationale: a Docker volume name is not ownership. Monitoring storage usage
// requires the acknowledged artifact and its stable persisted Volume binding.
func TestMonitoringCaptureRejectsAbsentOrForeignVolumeEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ComposeArtifact)
	}{
		{name: "absent artifact volume", mutate: func(artifact *agentpb.ComposeArtifact) { artifact.Volumes = nil }},
		{name: "foreign volume identity", mutate: func(artifact *agentpb.ComposeArtifact) {
			foreign := ids.NewAt(ids.KindVolume, time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC), 999)
			artifact.Volumes[0].VolumeId = foreign
			artifact.Volumes[0].DockerName = "gp_vol_" + strings.ToLower(foreign)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSourceFixture(t, 230, domain.StrategyRecreate, "", nil, 1)
			bindMonitoringVolume(t, &fixture, 250)
			artifact := new(agentpb.ComposeArtifact)
			if proto.Unmarshal(fixture.runtime.Record.Runtime.CurrentArtifact, artifact) != nil {
				t.Fatal("decode fixture artifact")
			}
			test.mutate(artifact)
			encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
			if err != nil {
				t.Fatal(err)
			}
			fixture.runtime.Record.Runtime.CurrentArtifact = encoded
			if serviceruntimerecord.Validate(fixture.runtime.Record) != nil {
				t.Fatal("fixture mutation unexpectedly invalidated unrelated runtime authority")
			}
			releases := &fakeReleases{fixtures: fixtureMap(fixture)}
			if _, ok := CaptureMonitoring(t.Context(), releases, fixture.service); ok {
				t.Fatal("unbound volume became monitoring authority")
			}
		})
	}
}

func TestMonitoringCaptureSelectsBoundVolumeFromLargerAcknowledgedEnvelope(t *testing.T) {
	fixture := newSourceFixture(t, 255, domain.StrategyRecreate, "", nil, 1)
	selectedID := bindMonitoringVolume(t, &fixture, 256)
	artifact := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(fixture.runtime.Record.Runtime.CurrentArtifact, artifact) != nil {
		t.Fatal("decode fixture artifact")
	}
	extraID := ids.NewAt(ids.KindVolume, time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC), 257)
	artifact.Volumes = append(artifact.Volumes, &agentpb.ComposeVolume{
		VolumeId: extraID, ComposeName: "unrelated", DockerName: "gp_vol_" + strings.ToLower(extraID),
	})
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	fixture.runtime.Record.Runtime.CurrentArtifact = encoded
	if err := serviceruntimerecord.Validate(fixture.runtime.Record); err != nil {
		t.Fatal(err)
	}
	captured, ok := CaptureMonitoring(t.Context(), &fakeReleases{fixtures: fixtureMap(fixture)}, fixture.service)
	if !ok || len(captured.Volumes()) != 1 || captured.Volumes()[0].VolumeId != selectedID {
		t.Fatalf("bound volume was hidden by unrelated acknowledged volume: ok=%v volumes=%+v", ok, captured.Volumes())
	}
}

func TestMonitoringCaptureUsesAcknowledgedBackingVolume(t *testing.T) {
	fixture := newSourceFixture(t, 260, domain.StrategyRecreate, "", nil, 1)
	stamp := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	fixture.service.Record.BackingNetworkID = ids.NewAt(ids.KindNetwork, stamp, 261)
	fixture.service.Record.Desired.Adapter = "custom"
	volumeID := ids.NewAt(ids.KindVolume, stamp, 262)
	artifact := &agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   fixture.service.Record.EnvironmentID, ProjectName: "gp-" + strings.ToLower(fixture.service.Record.EnvironmentID),
		Services: []*agentpb.ComposeService{{
			ServiceId: fixture.service.Record.Desired.ID, ComposeName: "database", ExpectedReplicas: 1,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.managed", Value: "true"}, {Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.environment-id", Value: fixture.service.Record.EnvironmentID},
				{Key: "com.groundplane.service-id", Value: fixture.service.Record.Desired.ID},
				{Key: "com.groundplane.plan-id", Value: fixture.render.Record.PlanID},
				{Key: "com.groundplane.render-generation", Value: "1"},
			},
		}},
		Volumes: []*agentpb.ComposeVolume{{
			VolumeId: volumeID, ComposeName: "data", DockerName: "gp_vol_" + strings.ToLower(volumeID),
		}},
	}
	encoded, err := proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	applied := testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Revision: 5, ReadRevision: fixture.service.ReadRevision,
		Record: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: fixture.service.Record.EnvironmentID, RenderGeneration: 1, ComposeArtifact: encoded,
			Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "data", Key: "data"}},
			VolumeMounts: []testenvironmentprojection.EnvironmentServiceVolumeMount{{
				ServiceID: fixture.service.Record.Desired.ID, VolumeID: volumeID, Target: "/data",
			}},
		},
	}
	captured, ok := CaptureMonitoring(t.Context(), &fakeReleases{applied: &applied}, fixture.service)
	if !ok || captured.Target().RuntimeRole != "backing" || len(captured.Volumes()) != 1 ||
		captured.Volumes()[0].VolumeId != volumeID {
		t.Fatalf("acknowledged backing volume was not captured: ok=%v target=%+v volumes=%+v",
			ok, captured.Target(), captured.Volumes())
	}
}

func bindMonitoringVolume(t *testing.T, fixture *sourceFixture, seed int64) string {
	t.Helper()
	volumeID := ids.NewAt(ids.KindVolume, time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC), seed)
	artifact := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(fixture.runtime.Record.Runtime.CurrentArtifact, artifact) != nil {
		t.Fatal("decode fixture artifact")
	}
	artifact.Volumes = []*agentpb.ComposeVolume{{
		VolumeId: volumeID, ComposeName: "data", DockerName: "gp_vol_" + strings.ToLower(volumeID),
		ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.managed", Value: "true"}},
	}}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	fixture.runtime.Record.Runtime.CurrentArtifact = encoded
	if err := serviceruntimerecord.Validate(fixture.runtime.Record); err != nil {
		t.Fatal(err)
	}
	fixture.render.Record.Projection.Volumes = []testenvironmentprojection.EnvironmentVolumeIdentity{{
		ID: volumeID, Slug: "data", Key: "data",
	}}
	fixture.render.Record.Projection.VolumeMounts = []testenvironmentprojection.EnvironmentServiceVolumeMount{{
		ServiceID: fixture.service.Record.Desired.ID, VolumeID: volumeID, Target: "/data",
	}}
	digest, err := domain.Digest(fixture.render.Record)
	if err != nil {
		t.Fatal(err)
	}
	fixture.serving.Intent.RenderInputDigest = digest
	return volumeID
}
