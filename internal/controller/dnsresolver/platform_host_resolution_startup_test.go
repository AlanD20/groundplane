package dnsresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestStartupResolverTaskSealsAutomaticProcedure(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		ensureService bool
		wantSteps     int
	}{
		{name: "update", wantSteps: 2},
		{name: "ensure", ensureService: true, wantSteps: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			task := startupResolverTask(
				"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC),
				test.ensureService,
			)
			if len(task.Steps) != test.wantSteps {
				t.Fatalf("len(Steps) = %d, want %d", len(task.Steps), test.wantSteps)
			}
			if len(task.Params) != 2 || task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceComponent ||
				task.Params[etcd.TaskAutomaticReconcileParam] != "true" {
				t.Fatalf("Params = %#v, want sealed automatic Component shape", task.Params)
			}
			if !etcd.IsAutomaticReconcileTask(task) {
				t.Fatal("startup Task is not recognized as automatic reconciliation")
			}
		})
	}
}

// Rationale: a clean-start automatic resolver Task must persist the hash of
// the exact generic execution plan regenerated for its Agent assignment.
func TestStartupResolverTaskPersistsResolvedExecutionPlanHash(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0).UTC()
	componentID := ids.NewAt(ids.KindComponent, now, 1)
	component := core.Component{
		ID: componentID, Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: testCorefileTemplate,
			UpstreamAuto:     true,
		}},
	}
	record, err := etcd.NewComponentRecord(component)
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	desired, err := etcd.ProjectComponentRecord(record)
	if err != nil {
		t.Fatalf("ProjectComponentRecord() error = %v", err)
	}
	projection, err := etcd.NewHostResolutionProjectionRecord(1, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	baselineContent := []byte("nameserver 1.1.1.1\n")
	baselineDigest := sha256.Sum256(baselineContent)
	baseline := etcd.HostResolverBaselineRecord{
		Generation: 1, Content: baselineContent, SHA256: hex.EncodeToString(baselineDigest[:]), CapturedAt: now,
	}
	definition, err := registeredcoredns.Definition()
	if err != nil {
		t.Fatalf("registeredcoredns.Definition() error = %v", err)
	}
	environmentPlanner := renderPlannerEnvironmentPlanner{}
	renderPlanner, err := NewPlatformRenderPlanner(
		fixedProjectionReader{record: projection}, renderPlannerBaselineRepository{record: baseline},
		renderPlannerObservationRepository{}, func(context.Context) ([]byte, error) {
			return append([]byte(nil), baselineContent...), nil
		}, rendererPin{}, environmentPlanner, renderPlannerCatalog{definition: definition}, "activate-config",
	)
	if err != nil {
		t.Fatalf("NewPlatformRenderPlanner() error = %v", err)
	}
	task := startupResolverTask(componentID, now, true)
	current := etcd.Versioned[etcd.ComponentRecord]{Record: record}
	input, err := renderPlanner.PrepareBootstrapConfigTaskAtProjection(
		context.Background(), current, desired, task, projection,
	)
	if err != nil {
		t.Fatalf("PrepareBootstrapConfigTaskAtProjection() error = %v", err)
	}
	if input.PlanSHA256 == input.ExecutionPlanSHA256 {
		t.Fatal("registered EnvironmentPlan and generic ExecutionPlan digests were conflated")
	}
	task.PlanHash = input.ExecutionPlanSHA256
	task.Params[etcd.TaskPlatformComponentDesiredSHA256Param] = input.DesiredSHA256
	registeredPlan, err := environmentPlanner.Plan(
		definition.Implementation(), input.GeneratedServiceID, componentdns.RenderInput{
			CorefileTemplate: testCorefileTemplate,
		},
	)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	executionPlanner, err := NewPlatformComponentExecutionPlanner(
		environmentpath.DefaultVolumeRoot,
		executionRepositoryStub{
			input: input, current: current,
			baseline: etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: baseline},
		},
		&executionCatalogStub{plan: registeredPlan},
	)
	if err != nil {
		t.Fatalf("NewPlatformComponentExecutionPlanner() error = %v", err)
	}
	resolved, err := executionPlanner.ResolveComponentExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveComponentExecutionPlan() error = %v", err)
	}
	if resolvedHash := hex.EncodeToString(resolved.GetPlanHash()); task.PlanHash != resolvedHash {
		t.Fatalf("durable/resolved plan hash = %s / %s", task.PlanHash, resolvedHash)
	}
}
