package dnsresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type renderPlannerBaselineRepository struct {
	record etcd.HostResolverBaselineRecord
}

func (repository renderPlannerBaselineRepository) GetHostResolverBaseline(
	context.Context,
) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error) {
	return etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: repository.record}, true, nil
}

func (repository renderPlannerBaselineRepository) EnsureHostResolverBaseline(
	context.Context,
	[]byte,
	time.Time,
) (etcd.Versioned[etcd.HostResolverBaselineRecord], error) {
	return etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: repository.record}, nil
}

type renderPlannerObservationRepository struct{}

func (renderPlannerObservationRepository) GetPlatformComponentObservation(
	context.Context,
	string,
) (etcd.Versioned[etcd.ComponentObservationRecord], bool, error) {
	return etcd.Versioned[etcd.ComponentObservationRecord]{}, false, nil
}

type renderPlannerCatalog struct {
	definition componentsdk.Definition
}

func (catalog renderPlannerCatalog) Digest() [32]byte {
	return catalog.definition.Digest()
}

func (catalog renderPlannerCatalog) FindAction(
	implementation componentsdk.ImplementationKey,
	action componentsdk.ActionID,
) (componentsdk.Definition, componentsdk.ActionDefinition, bool) {
	if implementation != catalog.definition.Implementation() {
		return componentsdk.Definition{}, componentsdk.ActionDefinition{}, false
	}
	definition, found := catalog.definition.FindAction(action)
	return catalog.definition, definition, found
}

func (catalog renderPlannerCatalog) FindActionByCapability(
	capability componentsdk.Capability,
	actionID componentsdk.ActionID,
) (componentsdk.Definition, componentsdk.ActionDefinition, bool) {
	for _, provided := range catalog.definition.Provides() {
		if provided != capability {
			continue
		}
		action, found := catalog.definition.FindAction(actionID)
		return catalog.definition, action, found
	}
	return componentsdk.Definition{}, componentsdk.ActionDefinition{}, false
}

type renderPlannerEnvironmentPlanner struct{}

func (renderPlannerEnvironmentPlanner) Plan(
	_ componentsdk.ImplementationKey,
	serviceID string,
	_ componentdns.RenderInput,
) (componentsdk.EnvironmentPlan, error) {
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{{
			ID: serviceID, Name: "resolver", Image: testPlatformImage("example/resolver"),
			NetworkMode: componentsdk.ManagedNetworkModeHost, Restart: "unless-stopped", Replicas: 1,
			Mounts: []componentsdk.ManagedMount{{
				Source: "config", Target: "/etc/resolver", Kind: componentsdk.ManagedMountKindDirectory,
				ReadOnly: true,
			}}, ObservationAction: "observe-serving",
		}},
		Files: []componentsdk.ManagedFile{{Path: "config/Corefile", Content: []byte("same bytes\n")}},
	}, nil
}

// Rationale: only repository-proven clean bootstrap may lack both generated
// Service and observation; every ambiguous or real predecessor stays closed.
func TestPrepareConfigTaskRequiresObservationOnlyForServingPredecessor(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                string
		currentEnabled      bool
		hasGeneratedService bool
		bootstrapProvenance bool
		wantConflict        bool
	}{
		{name: "clean enabled bootstrap", currentEnabled: true, bootstrapProvenance: true},
		{name: "enabled initial activation", currentEnabled: true},
		{
			name: "serving predecessor without observation", currentEnabled: true,
			hasGeneratedService: true, wantConflict: true,
		},
		{
			name:                "disabled serving predecessor without observation",
			hasGeneratedService: true, wantConflict: true,
		},
		{name: "disabled first enable", currentEnabled: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			now := time.Unix(1_700_000_000, 0).UTC()
			componentID := ids.NewAt(ids.KindComponent, now, 1)
			component := core.Component{
				ID: componentID, Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
				Enabled: test.currentEnabled,
				Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
					CorefileTemplate: testCorefileTemplate,
					UpstreamAuto:     true,
				}},
			}
			if test.hasGeneratedService {
				component.GeneratedServices = []string{ids.NewAt(ids.KindService, now, 8)}
			}
			record, err := etcd.NewComponentRecord(component)
			if err != nil {
				t.Fatalf("NewComponentRecord() error = %v", err)
			}
			desired, err := etcd.ProjectComponentRecord(record)
			if err != nil {
				t.Fatalf("ProjectComponentRecord() error = %v", err)
			}
			desired.Enabled = true
			projection, err := etcd.NewHostResolutionProjectionRecord(1, nil)
			if err != nil {
				t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
			}
			baselineContent := []byte("nameserver 1.1.1.1\n")
			baselineDigest := sha256.Sum256(baselineContent)
			baseline := etcd.HostResolverBaselineRecord{
				Generation: 1, Content: baselineContent, SHA256: hex.EncodeToString(baselineDigest[:]), CapturedAt: now,
			}
			definition := testResolverDefinition(t)
			planner, err := NewPlatformRenderPlanner(
				fixedProjectionReader{record: projection}, renderPlannerBaselineRepository{record: baseline},
				renderPlannerObservationRepository{}, func(context.Context) ([]byte, error) {
					return append([]byte(nil), baselineContent...), nil
				}, rendererPin{}, renderPlannerEnvironmentPlanner{}, renderPlannerCatalog{definition: definition},
				"activate-config",
			)
			if err != nil {
				t.Fatalf("NewPlatformRenderPlanner() error = %v", err)
			}
			task := etcd.TaskRecord{
				ID: ids.NewAt(ids.KindTask, now, 2), PlanID: ids.NewAt(ids.KindPlan, now, 3),
				RenderGeneration: 1, Steps: []etcd.TaskStepRecord{
					{
						Kind: etcd.TaskStepOperation,
						ID:   ids.NewAt(ids.KindStep, now, 4),
					}, {Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 5)},
					{
						Kind: etcd.TaskStepOperation,
						ID:   ids.NewAt(ids.KindStep, now, 6),
					}, {Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 7)},
				},
			}
			current := etcd.Versioned[etcd.ComponentRecord]{Record: record}
			var input etcd.PlatformComponentTaskRenderInput
			if test.bootstrapProvenance {
				input, err = planner.PrepareBootstrapConfigTaskAtProjection(
					context.Background(), current, desired, task, projection,
				)
			} else {
				input, err = planner.PrepareConfigTask(context.Background(), current, desired, task)
			}
			if test.wantConflict {
				if err == nil {
					t.Fatalf("PrepareConfigTask() error = %v, want missing-observation state conflict", err)
				}
				kind, found := errs.KindOf(err)
				if !found || kind != errs.KindStateConflict {
					t.Fatalf("PrepareConfigTask() error = %v, want missing-observation state conflict", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("PrepareConfigTask() error = %v", err)
			}
			if input.RollbackComposeArtifact != nil {
				t.Fatal("initial Component activation planned compensation to a missing predecessor")
			}
		})
	}
}
