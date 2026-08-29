package etcd

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEnvironmentComposeProjectionPinsSortedRouteIdentities(t *testing.T) {
	// Rationale: restart-safe Caddy rendering must recover the stable Route IDs
	// assigned at apply without consulting mutable Route records.
	t.Parallel()
	now := time.Date(2026, 8, 22, 22, 0, 0, 0, time.UTC)
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID:    ids.NewAt(ids.KindEnvironment, now, 1),
		RevisionID:       ids.NewAt(ids.KindTask, now, 2),
		RenderGeneration: 1,
		Routes: []EnvironmentRouteIdentity{
			{ID: ids.NewAt(ids.KindRoute, now, 3), Host: "api.example.com", Path: "/"},
			{ID: ids.NewAt(ids.KindRoute, now, 4), Host: "app.example.com", Path: "/app/*"},
		},
	})
	encoded, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := decodeEnvironmentComposeProjection(encoded)
	if err != nil || len(decoded.Routes) != 2 || decoded.Routes[1].ID != projection.Routes[1].ID {
		t.Fatalf("decodeEnvironmentComposeProjection() = %#v, %v", decoded, err)
	}
	projection.Routes[0], projection.Routes[1] = projection.Routes[1], projection.Routes[0]
	if _, err := encodeEnvironmentComposeProjection(projection); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("encodeEnvironmentComposeProjection(unsorted Routes) error = %v", err)
	}
}

// Rationale: Route removal must advance the applied render exactly once while
// retaining the old Blueprint match for deterministic Component regeneration.
func TestSuppressEnvironmentRouteMovesIdentityOutOfEffectiveSet(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 1, 0, 0, 0, time.UTC)
	routeID := ids.NewAt(ids.KindRoute, now, 3)
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 1),
		RevisionID:    ids.NewAt(ids.KindTask, now, 2), RenderGeneration: 7,
		Routes: []EnvironmentRouteIdentity{{ID: routeID, Host: "app.example.com", Path: "/app/*"}},
	})
	next, changed, err := SuppressEnvironmentRoute(projection, routeID)
	if err != nil {
		t.Fatalf("SuppressEnvironmentRoute() error = %v", err)
	}
	if !changed || next.RenderGeneration != 8 || len(next.Routes) != 0 ||
		len(next.SuppressedRoutes) != 1 || next.SuppressedRoutes[0].ID != routeID {
		t.Fatalf("SuppressEnvironmentRoute() = %#v, changed=%t", next, changed)
	}
	if len(projection.Routes) != 1 || len(projection.SuppressedRoutes) != 0 {
		t.Fatalf("SuppressEnvironmentRoute() mutated input = %#v", projection)
	}
	replayed, changed, err := SuppressEnvironmentRoute(next, routeID)
	if err != nil || changed || len(replayed.SuppressedRoutes) != 1 {
		t.Fatalf("SuppressEnvironmentRoute(replay) = %#v, %t, %v", replayed, changed, err)
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

// Rationale: explicit Component disable may remove only its generated Service;
// authored Service omission must remain blocked by the same projection CAS.
func TestEnvironmentComposeProjectionAdvanceAllowsComponentGeneratedServiceRemoval(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 23, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 40)
	generatedServiceID := ids.NewAt(ids.KindService, now, 41)
	componentRecord := func(kind core.ComponentKind, offset int64, enabled bool, services []string) ComponentRecord {
		record, err := NewComponentRecord(core.Component{
			ID: ids.NewAt(ids.KindComponent, now, offset), Owner: core.ComponentOwnerEnvironment,
			OwnerID: environmentID, Kind: kind, Enabled: enabled, GeneratedServices: services,
		})
		if err != nil {
			t.Fatalf("NewComponentRecord() error = %v", err)
		}
		return record
	}
	previous := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 44),
		RenderGeneration: 1,
		Services:         []EnvironmentComposeIdentity{{ID: generatedServiceID, Name: "cloudflare-tunnel"}},
		Components: []ComponentRecord{
			componentRecord(core.ComponentKindIngressCaddy, 42, false, nil),
			func() ComponentRecord {
				record, err := NewComponentRecord(core.Component{
					ID: ids.NewAt(ids.KindComponent, now, 43), Owner: core.ComponentOwnerEnvironment,
					OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
					Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
						SecretID: ids.NewAt(ids.KindSecret, now, 47),
					}},
					GeneratedServices: []string{generatedServiceID},
				})
				if err != nil {
					t.Fatalf("NewComponentRecord(Cloudflare) error = %v", err)
				}
				return record
			}(),
		},
	})
	next := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: ids.NewAt(ids.KindTask, now, 45),
		RenderGeneration: 2,
		Components: []ComponentRecord{
			componentRecord(core.ComponentKindIngressCaddy, 42, false, nil),
			componentRecord(core.ComponentKindEdgeCloudflare, 43, false, nil),
		},
	})
	if err := validateEnvironmentComposeProjectionAdvance(previous, true, next); err != nil {
		t.Fatalf("validateEnvironmentComposeProjectionAdvance() error = %v", err)
	}
	previous.Services = append(previous.Services, EnvironmentComposeIdentity{
		ID: ids.NewAt(ids.KindService, now, 46), Name: "worker",
	})
	sort.Slice(previous.Services, func(left int, right int) bool {
		return previous.Services[left].Name < previous.Services[right].Name
	})
	previous = withTestEnvironmentComposeArtifact(previous)
	if err := validateEnvironmentComposeProjectionAdvance(previous, true, next); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("validateEnvironmentComposeProjectionAdvance(authored omission) error = %v", err)
	}
}
