package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type routeRemovalPlanReader struct {
	*blueprintPlanReader
	intent etcd.RouteRemovalIntent
}

type routeRemovalPlanCaddyRenderer struct{}

func (routeRemovalPlanCaddyRenderer) Render(
	environment core.Environment,
	component core.Component,
) (map[string]components.GeneratedService, map[string][]byte, error) {
	if !component.Enabled {
		return map[string]components.GeneratedService{}, map[string][]byte{}, nil
	}
	content := []byte("# no routes\n")
	if len(environment.Routes) != 0 {
		content = []byte(environment.Routes[0].Host + "\n")
	}
	return map[string]components.GeneratedService{
		"caddy": {
			Service: core.Service{
				ID: component.GeneratedServices[0], Name: "caddy", Image: "caddy:2",
				Zones: []string{"frontend"}, Restart: "unless-stopped", Replicas: 1,
			},
			StaticIPv4: map[string]string{"frontend": component.PinnedIPv4},
			Mounts: []components.GeneratedMount{{
				Source: RouteRemovalCaddyfilePath, Target: "/etc/caddy/Caddyfile", ReadOnly: true,
			}},
		},
	}, map[string][]byte{RouteRemovalCaddyfilePath: content}, nil
}

func (routeRemovalPlanCaddyRenderer) Healthy(core.Environment, core.Component) (bool, error) {
	return true, nil
}

func (reader *routeRemovalPlanReader) GetRouteRemovalIntent(
	context.Context,
	string,
) (etcd.Versioned[etcd.RouteRemovalIntent], bool, error) {
	return etcd.Versioned[etcd.RouteRemovalIntent]{Record: reader.intent}, true, nil
}

// Rationale: initial publication and restart-time reconstruction must seal the
// same candidate Caddyfile metadata, exact generated Service, and closed
// validate/reload procedure without persisting generated file bytes.
func TestTaskPlanResolverRebuildsRouteRemovalCaddyProcedure(t *testing.T) {
	t.Parallel()
	reader, intent, task := routeRemovalPlanTestState(t)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	_, _, _, _, catalog := componentPlanProjectionInput(t)
	catalog[0].Environment = routeRemovalPlanCaddyRenderer{}
	resolver.componentCatalog = catalog
	prepared, err := resolver.PrepareRouteRemovalTask(
		context.Background(),
		task,
		intent,
		RouteRemovalTaskProcedureIDs{
			ArtifactID:        "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			MaterializationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			MaterializeStepID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			ApplyStepID:       "step_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		},
	)
	if err != nil {
		t.Fatalf("PrepareRouteRemovalTask() error = %v", err)
	}
	reader.intent = intent
	first, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	second, err := resolver.ResolveExecutionPlan(context.Background(), prepared)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(replay) error = %v", err)
	}
	digest, err := hex.DecodeString(prepared.Materializations[0].SHA256)
	if err != nil {
		t.Fatalf("DecodeString(materialization digest) error = %v", err)
	}
	apply := first.Steps[1].GetCaddyConfigApply()
	if !bytes.Equal(first.PlanHash, second.PlanHash) || hex.EncodeToString(first.PlanHash) != prepared.PlanHash ||
		first.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || first.TargetId != task.Target ||
		len(first.Artifacts) != 1 || len(first.Steps) != 2 || first.Steps[0].GetMaterializeFile() == nil ||
		apply == nil || apply.ServiceId != intent.CandidateProjection.Components[0].Runtime.GeneratedServices[0] ||
		!bytes.Equal(apply.CaddyfileSha256, digest) {
		t.Fatalf("resolved Route removal plans = %#v / %#v", first, second)
	}
	content, err := resolver.ResolveComponentFile(
		context.Background(),
		intent.EnvironmentID,
		*prepared.Materializations[0].Source.ComponentFile,
	)
	if err != nil {
		t.Fatalf("ResolveComponentFile(candidate) error = %v", err)
	}
	if strings.Contains(string(content), "app.example.com") {
		t.Fatalf("candidate Caddyfile retained removed Route: %q", content)
	}
}

func routeRemovalPlanTestState(
	t *testing.T,
) (*routeRemovalPlanReader, etcd.RouteRemovalIntent, etcd.TaskRecord) {
	t.Helper()
	_, identity, projection, _, _ := componentPlanProjectionInput(t)
	identity.TenantID = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	identity.TenantSlug = "acme"
	identity.ProjectSlug = "shop"
	at := time.Date(2026, 8, 23, 4, 0, 0, 0, time.UTC)
	content := []byte(`kind: environment
schema: 1
metadata: {tenant: acme, project: shop, environment: production}
services:
  api:
    image: api:1
    networks: [frontend]
    expose: ["8080"]
networks:
  frontend:
    ipam:
      config:
        - subnet: 10.70.0.0/24
x-gp-routes:
  - hostname: app.example.com
    path: /app/*
    target: api
    target_port: 8080
    exposure: public
x-gp-components:
  caddy:
    kind: caddy
    enabled: true
`)
	base := &blueprintPlanReader{
		tenant: etcd.TenantRecord{ID: identity.TenantID, Slug: identity.TenantSlug, Name: "Acme"},
		project: etcd.ProjectRecord{
			ID: identity.ProjectID, TenantID: identity.TenantID, Slug: identity.ProjectSlug,
			Name: "Shop", Kind: etcd.ProjectKindTenant,
		},
		environment: etcd.EnvironmentRecord{
			ID: identity.EnvironmentID, ProjectID: identity.ProjectID, Name: identity.EnvironmentName,
			NetworkPool: "10.70.0.0/16", VolumeDir: identity.AuthorizedVolumeDir,
			ProvisioningState: etcd.EnvironmentProvisioningReady,
		},
		revision: etcd.EnvironmentBlueprintRevision{
			EnvironmentID: identity.EnvironmentID, RevisionID: projection.BlueprintRevisionID,
			RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
			Files: []etcd.EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: content}}, CreatedAt: at,
		},
		projection: projection,
	}
	taskID := ids.NewAt(ids.KindTask, at, 100)
	intent, err := etcd.NewRouteRemovalIntent(
		taskID,
		identity.EnvironmentID,
		projection.Routes[0].ID,
		17,
		&etcd.Versioned[etcd.EnvironmentComposeProjection]{
			Record: projection, Revision: 18, ReadRevision: 18,
		},
		at,
	)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	reader := &routeRemovalPlanReader{blueprintPlanReader: base, intent: intent}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, at, 101),
		Executor: etcd.TaskExecutorAgent, PlanID: ids.NewAt(ids.KindPlan, at, 102),
		Type: etcd.TaskRemove, Target: intent.RouteID, TimeoutSeconds: 120,
		Status: etcd.TaskStatusPending, CreatedAt: at,
	}
	return reader, intent, task
}
