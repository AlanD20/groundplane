package etcd

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestEnvironmentComposeProjectionRoundTripsLosslessDesiredTopology(t *testing.T) {
	// Rationale: a queued qa-workload Blueprint Task must own the exact six Zones,
	// thirteen Services, and six Routes it was published to reconcile.
	t.Parallel()
	projection := desiredTopologyProjectionFixture(t)
	encoded, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(encoded)
	if err != nil {
		t.Fatalf("decodeEnvironmentComposeProjection() error = %v", err)
	}
	if len(decoded.DesiredZones) != 6 || len(decoded.DesiredServices) != 13 || len(decoded.DesiredRoutes) != 6 ||
		!reflect.DeepEqual(decoded.DesiredZones, projection.DesiredZones) ||
		!reflect.DeepEqual(decoded.DesiredServices, projection.DesiredServices) ||
		!reflect.DeepEqual(decoded.DesiredRoutes, projection.DesiredRoutes) {
		t.Fatalf("decoded desired topology = %d/%d/%d %#v", len(decoded.DesiredZones),
			len(decoded.DesiredServices), len(decoded.DesiredRoutes), decoded)
	}
	if bytes.Contains(encoded, []byte("runtime_intent")) || bytes.Contains(encoded, []byte(`"observed"`)) {
		t.Fatalf("sealed desired projection contains runtime or observed state: %q", encoded)
	}

	baseDigest, _, err := testblueprints.EnvironmentBlueprintProjectionEvidence(projection)
	if err != nil {
		t.Fatalf("EnvironmentBlueprintProjectionEvidence() error = %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(*testenvironmentprojection.EnvironmentComposeProjection)
	}{
		{name: "Zone desired field", mutate: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
			value.DesiredZones[0].Desired.Internal = !value.DesiredZones[0].Desired.Internal
		}},
		{name: "Service desired field", mutate: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
			value.DesiredServices[0].Desired.Image = "postgres:16.9"
		}},
		{name: "Service backing network", mutate: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
			value.DesiredServices[0].BackingNetworkID = value.DesiredZones[1].Desired.ID
		}},
		{name: "Route desired field", mutate: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
			value.DesiredRoutes[0].Desired.Exposure = "internal"
		}},
		{name: "Route desired generation", mutate: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
			value.DesiredRoutes[0].DesiredGeneration++
		}},
	}
	for _, test := range mutations {
		test := test
		t.Run(test.name, func(t *testing.T) {
			changed := testenvironmentprojection.CloneEnvironmentComposeProjection(projection)
			test.mutate(&changed)
			changedBytes, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(changed)
			if err != nil {
				t.Fatalf("encode changed projection: %v", err)
			}
			changedDigest, _, err := testblueprints.EnvironmentBlueprintProjectionEvidence(changed)
			if err != nil {
				t.Fatalf("changed projection evidence: %v", err)
			}
			if bytes.Equal(changedBytes, encoded) || changedDigest == baseDigest {
				t.Fatal("desired topology change did not change sealed bytes and digest")
			}
		})
	}
}

func desiredTopologyProjectionFixture(t *testing.T) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2), RenderGeneration: 1,
	}
	for index := 0; index < 6; index++ {
		zone := core.Zone{
			ID: ids.NewAt(ids.KindNetwork, now, int64(10+index)), Name: fmt.Sprintf("zone-%02d", index),
			Subnet: fmt.Sprintf("10.200.%d.0/24", index), OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		}
		projection.DesiredZones = append(projection.DesiredZones, testenvironmentprojection.EnvironmentZoneProjection{
			EnvironmentID: environmentID, Desired: zone,
		})
	}
	for index := 0; index < 13; index++ {
		service := core.Service{
			ID: ids.NewAt(ids.KindService, now, int64(30+index)), Name: fmt.Sprintf("service-%02d", index),
			Image: fmt.Sprintf("example/service:%d", index),
		}
		backingNetworkID := ""
		if index == 0 {
			service.Adapter = "postgres:16"
			backingNetworkID = projection.DesiredZones[0].Desired.ID
		}
		projection.DesiredServices = append(projection.DesiredServices, testservices.EnvironmentServiceProjection{
			EnvironmentID: environmentID, BackingNetworkID: backingNetworkID, Desired: service,
		})
	}
	for index := 0; index < 6; index++ {
		route := core.Route{
			ID: ids.NewAt(ids.KindRoute, now, int64(50+index)), Host: fmt.Sprintf("route-%02d.example.test", index),
			Path: "/", TargetServiceID: projection.DesiredServices[index].Desired.ID,
			TargetPort: uint16(8000 + index), Exposure: "public",
		}
		projection.DesiredRoutes = append(
			projection.DesiredRoutes,
			testenvironmentprojection.EnvironmentRouteProjection{
				EnvironmentID: environmentID, Desired: route, DesiredGeneration: uint64(index + 1),
			},
		)
	}
	return withTestEnvironmentComposeArtifact(projection)
}

func TestApplyEnvironmentRouteMutatesDesiredRoutesOnly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 22, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2), RenderGeneration: 1,
	})
	route, err := testroutes.NewRecord(environmentID, core.Route{
		ID: ids.NewAt(ids.KindRoute, now, 3), Host: "app.example.com", Path: "/app/*",
		TargetServiceID: ids.NewAt(ids.KindService, now, 4), TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	next, err := testenvironmentprojection.ApplyEnvironmentRoute(projection, route)
	if err != nil {
		t.Fatalf("ApplyEnvironmentRoute() error = %v", err)
	}
	if next.RenderGeneration != 2 || len(next.DesiredRoutes) != 1 ||
		next.DesiredRoutes[0].Desired.ID != route.Desired.ID || len(projection.DesiredRoutes) != 0 {
		t.Fatalf("ApplyEnvironmentRoute() = %#v; input = %#v", next, projection)
	}
}

// Rationale: a queued Blueprint task must retain the exact effective
// Component graph instead of re-reading mutable active Component records.
func TestEnvironmentComposeProjectionPinsSortedComponentSnapshots(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 22, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 10)
	componentRecord := func(kind core.ComponentKind, offset int64) testcomponents.Record {
		record, err := testcomponents.NewRecord(core.Component{
			ID: ids.NewAt(ids.KindComponent, now, offset), Owner: core.ComponentOwnerEnvironment,
			OwnerID: environmentID, Kind: kind,
		})
		if err != nil {
			t.Fatalf("NewComponentRecord() error = %v", err)
		}
		return record
	}
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 11),
		RenderGeneration: 1,
		Components: []testcomponents.Record{
			componentRecord(core.ComponentKindIngressCaddy, 12),
			componentRecord(core.ComponentKindEdgeCloudflare, 13),
		},
	})
	encoded, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(encoded)
	if err != nil || len(decoded.Components) != 2 ||
		decoded.Components[1].Desired.ID != projection.Components[1].Desired.ID {
		t.Fatalf("decodeEnvironmentComposeProjection() = %#v, %v", decoded, err)
	}
	projection.Components[0], projection.Components[1] = projection.Components[1], projection.Components[0]
	if _, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("encodeEnvironmentComposeProjection(unsorted Components) error = %v", err)
	}
}

func TestEnvironmentComposeProjectionRejectsDuplicateGeneratedServiceOwnership(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 70)
	serviceID := ids.NewAt(ids.KindService, now, 71)
	caddy, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 72), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, now, 76)},
		}},
		GeneratedServices: []string{serviceID},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(Caddy) error = %v", err)
	}
	tunnel, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 73), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
			ZoneIDs:  []string{ids.NewAt(ids.KindNetwork, now, 77)},
			SecretID: ids.NewAt(ids.KindSecret, now, 74),
		}},
		GeneratedServices: []string{serviceID},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(Tunnel) error = %v", err)
	}
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 75), RenderGeneration: 1,
		Components: []testcomponents.Record{caddy, tunnel},
	})
	if err := testenvironmentprojection.ValidateEnvironmentComposeProjection(projection); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateEnvironmentComposeProjection(duplicate owner) error = %v", err)
	}
}

// Rationale: enabling Caddy adds a Component-owned runtime Service to the
// normalized artifact without turning it into an authored or ordinary Service.
func TestEnvironmentComposeProjectionValidatesGeneratedServiceArtifactCoverage(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 80)
	authoredServiceID := ids.NewAt(ids.KindService, now, 81)
	generatedServiceID := ids.NewAt(ids.KindService, now, 82)
	caddy, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 83), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, now, 84)},
		}},
		GeneratedServices: []string{generatedServiceID},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(Caddy) error = %v", err)
	}
	tunnel, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 85), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(Tunnel) error = %v", err)
	}
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 86), RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: authoredServiceID, Name: "app", Image: "example/app:1"},
		}},
		Components: []testcomponents.Record{caddy, tunnel},
	})
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		t.Fatalf("unmarshal Compose artifact: %v", err)
	}
	artifact.Services = append(artifact.Services, &agentpb.ComposeService{
		ServiceId: generatedServiceID, ComposeName: "caddy", OwnerComponentId: caddy.Desired.ID,
	})
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal Compose artifact: %v", err)
	}
	encoded, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(encoded)
	if err != nil || len(decoded.DesiredServices) != 1 ||
		decoded.DesiredServices[0].Desired.ID != authoredServiceID {
		t.Fatalf("decodeEnvironmentComposeProjection() = %#v, %v", decoded, err)
	}

	tests := []struct {
		name   string
		mutate func(*agentpb.ComposeArtifact)
	}{
		{name: "missing generated Service", mutate: func(value *agentpb.ComposeArtifact) {
			value.Services = value.Services[:1]
		}},
		{name: "extra Service", mutate: func(value *agentpb.ComposeArtifact) {
			value.Services = append(value.Services, &agentpb.ComposeService{
				ServiceId: ids.NewAt(ids.KindService, now, 87), ComposeName: "extra",
			})
		}},
		{name: "duplicate Service", mutate: func(value *agentpb.ComposeArtifact) {
			value.Services[1] = &agentpb.ComposeService{ServiceId: authoredServiceID, ComposeName: "app"}
		}},
		{name: "wrong generated owner", mutate: func(value *agentpb.ComposeArtifact) {
			value.Services[1].OwnerComponentId = tunnel.Desired.ID
		}},
		{name: "duplicate generated name", mutate: func(value *agentpb.ComposeArtifact) {
			value.Services[1].ComposeName = "app"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := testenvironmentprojection.CloneEnvironmentComposeProjection(projection)
			mutated := proto.Clone(artifact).(*agentpb.ComposeArtifact)
			test.mutate(mutated)
			candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
			if err != nil {
				t.Fatalf("marshal mutated Compose artifact: %v", err)
			}
			if err := testenvironmentprojection.ValidateEnvironmentComposeProjection(candidate); !errors.Is(
				err, errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("validateEnvironmentComposeProjection() error = %v", err)
			}
		})
	}
}

// Rationale: Component secret bindings must retain exact non-secret Entry
// metadata and immutable generation ids without storing their value bytes.
func TestEnvironmentComposeProjectionPinsSortedEntrySnapshots(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 22, 45, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 20)
	entryRecord := func(offset int64, key string) testentries.Record {
		record, err := testentries.NewRecord(environmentID, core.EnvEntry{
			ID: ids.NewAt(ids.KindEnvEntry, now, offset), Kind: core.EntryKindEnv, Key: key,
			Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"cloudflare-tunnel"}, Secret: true,
		}, ids.NewAt(ids.KindConfig, now, offset+10))
		if err != nil {
			t.Fatalf("NewEntryRecord() error = %v", err)
		}
		return record
	}
	entries := []testentries.Record{
		entryRecord(22, "CLOUDFLARE_TUNNEL_TOKEN"),
		entryRecord(23, "SECOND_TOKEN"),
	}
	sort.Slice(entries, func(left int, right int) bool { return entries[left].Entry.ID < entries[right].Entry.ID })
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 21),
		RenderGeneration: 1, Entries: entries,
	})
	encoded, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(encoded)
	if err != nil || len(decoded.Entries) != 2 ||
		decoded.Entries[1].CurrentValueGenerationID != projection.Entries[1].CurrentValueGenerationID {
		t.Fatalf("decodeEnvironmentComposeProjection() = %#v, %v", decoded, err)
	}
	projection.Entries[0], projection.Entries[1] = projection.Entries[1], projection.Entries[0]
	if _, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("encodeEnvironmentComposeProjection(unsorted Entries) error = %v", err)
	}
}

// Rationale: Entry removal must advance the applied render exactly once while
// dropping only the immutable Entry generation selected by stable id.
func TestRemoveEnvironmentEntryDropsPinnedGeneration(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 3)
	record, err := testentries.NewRecord(environmentID, core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "production"},
		Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 4))
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2),
		RenderGeneration: 7, Entries: []testentries.Record{record},
	})
	next, changed, err := testenvironmentprojection.RemoveEnvironmentEntry(projection, entryID)
	if err != nil {
		t.Fatalf("RemoveEnvironmentEntry() error = %v", err)
	}
	if !changed || next.RenderGeneration != 8 || len(next.Entries) != 0 {
		t.Fatalf("RemoveEnvironmentEntry() = %#v, changed=%t", next, changed)
	}
	if len(projection.Entries) != 1 || projection.RenderGeneration != 7 {
		t.Fatalf("RemoveEnvironmentEntry() mutated input = %#v", projection)
	}
	replayed, changed, err := testenvironmentprojection.RemoveEnvironmentEntry(next, entryID)
	if err != nil || changed || replayed.RenderGeneration != 8 {
		t.Fatalf("RemoveEnvironmentEntry(replay) = %#v, %t, %v", replayed, changed, err)
	}
}

// Rationale: a Blueprint publication cannot turn omission into deletion. Only
// the resource-specific terminal Remove path may first publish a desired head
// without a Service, Zone, or Route; a pending Remove Task is not that proof.
func TestEnvironmentComposeProjectionPublicationRejectsNonEntryOmission(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 18, 0, 0, 0, time.UTC)
	previous := desiredTopologyProjectionFixture(t)
	previous.Volumes = []testenvironmentprojection.EnvironmentVolumeIdentity{{
		ID: ids.NewAt(ids.KindVolume, now, 1), Slug: "data", Key: "data",
	}}
	previous = withTestEnvironmentComposeArtifact(previous)

	tests := []struct {
		name         string
		resourceKind string
		target       string
		omit         func(*testenvironmentprojection.EnvironmentComposeProjection)
	}{
		{
			name: "Service", resourceKind: testtaskjournal.TaskResourceService,
			target: previous.DesiredServices[0].Desired.ID,
			omit: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
				value.DesiredServices = value.DesiredServices[1:]
			},
		},
		{
			name: "Zone", resourceKind: testtaskjournal.TaskResourceBackingZone,
			target: previous.DesiredZones[0].Desired.ID,
			omit: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
				value.DesiredZones = value.DesiredZones[1:]
			},
		},
		{
			name: "Route", resourceKind: testtaskjournal.TaskResourceRoute,
			target: previous.DesiredRoutes[0].Desired.ID,
			omit: func(value *testenvironmentprojection.EnvironmentComposeProjection) {
				value.DesiredRoutes = value.DesiredRoutes[1:]
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			next := testenvironmentprojection.CloneEnvironmentComposeProjection(previous)
			next.RevisionID = ids.NewAt(ids.KindTask, now, 10)
			next.RenderGeneration++
			test.omit(&next)
			next = withTestEnvironmentComposeArtifact(next)

			for _, task := range []TaskRecord{
				{Type: testtaskjournal.TaskUpdate},
				{
					Type: testtaskjournal.TaskRemove, Target: test.target, Status: testtaskjournal.TaskStatusPending,
					Params: map[string]string{testtaskjournal.TaskResourceKindParam: test.resourceKind},
				},
			} {
				if err := validateEnvironmentComposeProjectionPublicationAdvance(
					previous, true, next, task,
				); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
					t.Fatalf("validateEnvironmentComposeProjectionPublicationAdvance(%s) error = %v", task.Type, err)
				}
			}
		})
	}
}

// Rationale: ADR 0049 publishes Volume absence before runtime cleanup under
// one exact typed Remove Task; no other pending Task may authorize that loss.
func TestEnvironmentComposeProjectionPublicationAllowsExactVolumeRemoval(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 18, 30, 0, 0, time.UTC)
	volumeID := ids.NewAt(ids.KindVolume, now, 1)
	previous := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		RevisionID:    ids.NewAt(ids.KindTask, now, 3), RenderGeneration: 1,
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "data", Key: "data"}},
	})
	next := testenvironmentprojection.CloneEnvironmentComposeProjection(previous)
	next.RevisionID = ids.NewAt(ids.KindTask, now, 4)
	next.RenderGeneration++
	next.Volumes = nil
	next = withTestEnvironmentComposeArtifact(next)

	if err := validateEnvironmentComposeProjectionPublicationAdvance(
		previous, true, next, TaskRecord{Type: testtaskjournal.TaskUpdate},
	); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("validateEnvironmentComposeProjectionPublicationAdvance(update) error = %v", err)
	}
	if err := validateEnvironmentComposeProjectionPublicationAdvance(previous, true, next, TaskRecord{
		Type: testtaskjournal.TaskRemove, Target: volumeID, Status: testtaskjournal.TaskStatusPending,
		Params: map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceVolume},
	}); err != nil {
		t.Fatalf("validateEnvironmentComposeProjectionPublicationAdvance(Volume Remove) error = %v", err)
	}
}
