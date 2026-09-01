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
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEnvironmentComposeProjectionRoundTripsLosslessDesiredTopology(t *testing.T) {
	// Rationale: a queued qa-workload Blueprint Task must own the exact six Zones,
	// thirteen Services, and six Routes it was published to reconcile.
	t.Parallel()
	projection := desiredTopologyProjectionFixture(t)
	encoded, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := decodeEnvironmentComposeProjection(encoded)
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

	baseDigest, _, err := EnvironmentBlueprintProjectionEvidence(projection)
	if err != nil {
		t.Fatalf("EnvironmentBlueprintProjectionEvidence() error = %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(*EnvironmentComposeProjection)
	}{
		{name: "Zone desired field", mutate: func(value *EnvironmentComposeProjection) {
			value.DesiredZones[0].Desired.Internal = !value.DesiredZones[0].Desired.Internal
		}},
		{name: "Service desired field", mutate: func(value *EnvironmentComposeProjection) {
			value.DesiredServices[0].Desired.Image = "postgres:16.9"
		}},
		{name: "Service backing network", mutate: func(value *EnvironmentComposeProjection) {
			value.DesiredServices[0].BackingNetworkID = value.DesiredZones[1].Desired.ID
		}},
		{name: "Route desired field", mutate: func(value *EnvironmentComposeProjection) {
			value.DesiredRoutes[0].Desired.Exposure = "internal"
		}},
		{name: "Route desired generation", mutate: func(value *EnvironmentComposeProjection) {
			value.DesiredRoutes[0].DesiredGeneration++
		}},
	}
	for _, test := range mutations {
		test := test
		t.Run(test.name, func(t *testing.T) {
			changed := cloneEnvironmentComposeProjection(projection)
			test.mutate(&changed)
			changedBytes, err := encodeEnvironmentComposeProjection(changed)
			if err != nil {
				t.Fatalf("encode changed projection: %v", err)
			}
			changedDigest, _, err := EnvironmentBlueprintProjectionEvidence(changed)
			if err != nil {
				t.Fatalf("changed projection evidence: %v", err)
			}
			if bytes.Equal(changedBytes, encoded) || changedDigest == baseDigest {
				t.Fatal("desired topology change did not change sealed bytes and digest")
			}
		})
	}
}

func desiredTopologyProjectionFixture(t *testing.T) EnvironmentComposeProjection {
	t.Helper()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2), RenderGeneration: 1,
	}
	for index := 0; index < 6; index++ {
		zone := core.Zone{
			ID: ids.NewAt(ids.KindNetwork, now, int64(10+index)), Name: fmt.Sprintf("zone-%02d", index),
			Subnet: fmt.Sprintf("10.200.%d.0/24", index), OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		}
		projection.DesiredZones = append(projection.DesiredZones, EnvironmentZoneProjection{
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
		projection.DesiredServices = append(projection.DesiredServices, EnvironmentServiceProjection{
			EnvironmentID: environmentID, BackingNetworkID: backingNetworkID, Desired: service,
		})
	}
	for index := 0; index < 6; index++ {
		route := core.Route{
			ID: ids.NewAt(ids.KindRoute, now, int64(50+index)), Host: fmt.Sprintf("route-%02d.example.test", index),
			Path: "/", TargetServiceID: projection.DesiredServices[index].Desired.ID,
			TargetPort: uint16(8000 + index), Exposure: "public",
		}
		projection.DesiredRoutes = append(projection.DesiredRoutes, EnvironmentRouteProjection{
			EnvironmentID: environmentID, Desired: route, DesiredGeneration: uint64(index + 1),
		})
	}
	return withTestEnvironmentComposeArtifact(projection)
}

func TestApplyEnvironmentRouteMutatesDesiredRoutesOnly(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 22, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2), RenderGeneration: 1,
	})
	route, err := NewRouteRecord(environmentID, core.Route{
		ID: ids.NewAt(ids.KindRoute, now, 3), Host: "app.example.com", Path: "/app/*",
		TargetServiceID: ids.NewAt(ids.KindService, now, 4), TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	next, err := ApplyEnvironmentRoute(projection, route)
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
	componentRecord := func(kind core.ComponentKind, offset int64) ComponentRecord {
		record, err := NewComponentRecord(core.Component{
			ID: ids.NewAt(ids.KindComponent, now, offset), Owner: core.ComponentOwnerEnvironment,
			OwnerID: environmentID, Kind: kind,
		})
		if err != nil {
			t.Fatalf("NewComponentRecord() error = %v", err)
		}
		return record
	}
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 11),
		RenderGeneration: 1,
		Components: []ComponentRecord{
			componentRecord(core.ComponentKindIngressCaddy, 12),
			componentRecord(core.ComponentKindEdgeCloudflare, 13),
		},
	})
	encoded, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := decodeEnvironmentComposeProjection(encoded)
	if err != nil || len(decoded.Components) != 2 ||
		decoded.Components[1].Desired.ID != projection.Components[1].Desired.ID {
		t.Fatalf("decodeEnvironmentComposeProjection() = %#v, %v", decoded, err)
	}
	projection.Components[0], projection.Components[1] = projection.Components[1], projection.Components[0]
	if _, err := encodeEnvironmentComposeProjection(projection); !errors.Is(
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
	caddy, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 72), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneID: ids.NewAt(ids.KindNetwork, now, 76),
		}},
		GeneratedServices: []string{serviceID},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(Caddy) error = %v", err)
	}
	tunnel, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 73), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
			SecretID: ids.NewAt(ids.KindSecret, now, 74),
		}},
		GeneratedServices: []string{serviceID},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(Tunnel) error = %v", err)
	}
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 75), RenderGeneration: 1,
		Components: []ComponentRecord{caddy, tunnel},
	})
	if err := validateEnvironmentComposeProjection(projection); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateEnvironmentComposeProjection(duplicate owner) error = %v", err)
	}
}

// Rationale: Component secret bindings must retain exact non-secret Entry
// metadata and immutable generation ids without storing their value bytes.
func TestEnvironmentComposeProjectionPinsSortedEntrySnapshots(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 22, 45, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 20)
	entryRecord := func(offset int64, key string) EntryRecord {
		record, err := NewEntryRecord(environmentID, core.EnvEntry{
			ID: ids.NewAt(ids.KindEnvEntry, now, offset), Kind: core.EntryKindEnv, Key: key,
			Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"cloudflare-tunnel"}, Secret: true,
		}, ids.NewAt(ids.KindConfig, now, offset+10))
		if err != nil {
			t.Fatalf("NewEntryRecord() error = %v", err)
		}
		return record
	}
	entries := []EntryRecord{
		entryRecord(22, "CLOUDFLARE_TUNNEL_TOKEN"),
		entryRecord(23, "SECOND_TOKEN"),
	}
	sort.Slice(entries, func(left int, right int) bool { return entries[left].Entry.ID < entries[right].Entry.ID })
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 21),
		RenderGeneration: 1, Entries: entries,
	})
	encoded, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := decodeEnvironmentComposeProjection(encoded)
	if err != nil || len(decoded.Entries) != 2 ||
		decoded.Entries[1].CurrentValueGenerationID != projection.Entries[1].CurrentValueGenerationID {
		t.Fatalf("decodeEnvironmentComposeProjection() = %#v, %v", decoded, err)
	}
	projection.Entries[0], projection.Entries[1] = projection.Entries[1], projection.Entries[0]
	if _, err := encodeEnvironmentComposeProjection(projection); !errors.Is(
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
	record, err := NewEntryRecord(environmentID, core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: "production"},
		Exposure: []string{"all"},
	}, ids.NewAt(ids.KindConfig, now, 4))
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 2),
		RenderGeneration: 7, Entries: []EntryRecord{record},
	})
	next, changed, err := RemoveEnvironmentEntry(projection, entryID)
	if err != nil {
		t.Fatalf("RemoveEnvironmentEntry() error = %v", err)
	}
	if !changed || next.RenderGeneration != 8 || len(next.Entries) != 0 {
		t.Fatalf("RemoveEnvironmentEntry() = %#v, changed=%t", next, changed)
	}
	if len(projection.Entries) != 1 || projection.RenderGeneration != 7 {
		t.Fatalf("RemoveEnvironmentEntry() mutated input = %#v", projection)
	}
	replayed, changed, err := RemoveEnvironmentEntry(next, entryID)
	if err != nil || changed || replayed.RenderGeneration != 8 {
		t.Fatalf("RemoveEnvironmentEntry(replay) = %#v, %t, %v", replayed, changed, err)
	}
}
