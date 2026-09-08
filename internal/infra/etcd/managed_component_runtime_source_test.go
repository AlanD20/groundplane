package etcd

import (
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a failed desired-head attempt must carry the prior exact source
// descriptors forward so a later disable can remove the unpromoted runtime.
func TestManagedComponentRuntimeSourcesCarryFailedHeadThenDisable(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	oldRevision := ids.NewAt(ids.KindTask, now, 2)
	failedRevision := ids.NewAt(ids.KindTask, now, 3)
	oldArtifact := ids.NewAt(ids.KindConfig, now, 4)
	newArtifact := ids.NewAt(ids.KindConfig, now, 5)
	caddyID := ids.NewAt(ids.KindComponent, now, 6)
	tunnelID := ids.NewAt(ids.KindComponent, now, 7)
	oldCaddyService := ids.NewAt(ids.KindService, now, 8)
	oldTunnelService := ids.NewAt(ids.KindService, now, 9)
	newTunnelService := ids.NewAt(ids.KindService, now, 10)
	oldDigest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oldSources := []ManagedComponentRuntimeSource{
		{ComponentKind: core.ComponentKindIngressCaddy, ComponentID: caddyID, ServiceID: oldCaddyService,
			ComposeName: "caddy-old", RevisionID: oldRevision, ArtifactID: oldArtifact, ArtifactSHA256: oldDigest},
		{ComponentKind: core.ComponentKindEdgeCloudflare, ComponentID: tunnelID, ServiceID: oldTunnelService,
			ComposeName: "tunnel-old", RevisionID: oldRevision, ArtifactID: oldArtifact, ArtifactSHA256: oldDigest},
	}
	artifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(
		&agentpb.ComposeArtifact{ArtifactId: newArtifact},
	)
	if err != nil {
		t.Fatal(err)
	}
	failedHead := EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: failedRevision, ComposeArtifact: artifactBytes,
		Components: []ComponentRecord{
			managedComponentSourceRecord(t, environmentID, caddyID, core.ComponentKindIngressCaddy, false, "", ""),
			managedComponentSourceRecord(t, environmentID, tunnelID, core.ComponentKindEdgeCloudflare, false, "", ""),
		},
		ManagedComponentRuntimeSources: oldSources,
	}
	carried, err := ProjectManagedComponentRuntimeSources(ComponentTaskPreparation{}, failedHead)
	if err != nil {
		t.Fatalf("failed desired head source carry = %v", err)
	}
	if !reflect.DeepEqual(carried, oldSources) {
		t.Fatalf("failed desired head changed source descriptors: %#v", carried)
	}

	currentTunnel := managedComponentSourceRecord(
		t,
		environmentID,
		tunnelID,
		core.ComponentKindEdgeCloudflare,
		true,
		oldTunnelService,
		"",
	)
	disabledTunnel := managedComponentSourceRecord(
		t,
		environmentID,
		tunnelID,
		core.ComponentKindEdgeCloudflare,
		false,
		"",
		"",
	)
	taskID := ids.NewAt(ids.KindTask, now, 11)
	intent, err := NewComponentTaskIntent(taskID, environmentID, []ComponentTaskCandidate{{
		CurrentRevision: 7, Current: currentTunnel, Candidate: disabledTunnel,
	}}, now)
	if err != nil {
		t.Fatal(err)
	}
	disabledProjection := failedHead
	disabledProjection.Components = []ComponentRecord{
		failedHead.Components[0], disabledTunnel,
	}
	preparation := ComponentTaskPreparation{
		Intent: intent, managedRuntimeSources: oldSources, desiredProjectionRevision: 12,
	}
	retained, err := ProjectManagedComponentRuntimeSources(preparation, disabledProjection)
	if err != nil {
		t.Fatalf("disable source retention = %v", err)
	}
	if !reflect.DeepEqual(retained, oldSources) {
		t.Fatalf("disable changed historical source descriptors: %#v", retained)
	}

	newArtifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: newArtifact,
		Services: []*agentpb.ComposeService{
			{ServiceId: newTunnelService, ComposeName: "tunnel-new", OwnerComponentId: tunnelID},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	enabledTunnel := managedComponentSourceRecord(
		t,
		environmentID,
		tunnelID,
		core.ComponentKindEdgeCloudflare,
		true,
		newTunnelService,
		"",
	)
	enableIntent, err := NewComponentTaskIntent(
		ids.NewAt(ids.KindTask, now, 13),
		environmentID,
		[]ComponentTaskCandidate{{
			CurrentRevision: 8, Current: disabledTunnel, Candidate: enabledTunnel,
		}},
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	enableProjection := disabledProjection
	enableProjection.ComposeArtifact = newArtifactBytes
	enableProjection.Components = []ComponentRecord{failedHead.Components[0], enabledTunnel}
	replaced, err := ProjectManagedComponentRuntimeSources(ComponentTaskPreparation{
		Intent: enableIntent, managedRuntimeSources: oldSources, desiredProjectionRevision: 14,
	}, enableProjection)
	if err != nil {
		t.Fatalf("enabled source replacement = %v", err)
	}
	if len(replaced) != 2 || replaced[0] != oldSources[0] || replaced[1].ServiceID != newTunnelService ||
		replaced[1].RevisionID != failedRevision || replaced[1].ArtifactID != newArtifact || replaced[1].ComposeName != "tunnel-new" {
		t.Fatalf("enabled candidate did not replace only its source: %#v", replaced)
	}
}

func managedComponentSourceRecord(
	t *testing.T,
	environmentID, componentID string,
	kind core.ComponentKind,
	enabled bool,
	serviceID, zoneID string,
) ComponentRecord {
	t.Helper()
	component := core.Component{
		ID:      componentID,
		Owner:   core.ComponentOwnerEnvironment,
		OwnerID: environmentID,
		Kind:    kind,
		Enabled: enabled,
	}
	if enabled {
		component.GeneratedServices = []string{serviceID}
		if kind == core.ComponentKindEdgeCloudflare {
			component.Config = core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
				ZoneIDs:  []string{ids.NewAt(ids.KindNetwork, time.Date(2026, 9, 6, 9, 30, 0, 0, time.UTC), 1)},
				SecretID: ids.NewAt(ids.KindSecret, time.Date(2026, 9, 6, 9, 30, 0, 0, time.UTC), 2),
			}}
		} else if zoneID != "" {
			component.Config = core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{zoneID}}}
		}
	}
	record, err := NewComponentRecord(component)
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	return record
}
