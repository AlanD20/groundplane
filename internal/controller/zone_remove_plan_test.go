package controller

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestZoneRemovalPlanRebuildsAffectedServicesBeforeManagedNetworkRemoval(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 12)
	projectID := ids.NewAt(ids.KindProject, now, 13)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	zoneID := ids.NewAt(ids.KindNetwork, now, 2)
	firstServiceID := ids.NewAt(ids.KindService, now, 3)
	secondServiceID := ids.NewAt(ids.KindService, now, 4)
	taskID := ids.NewAt(ids.KindTask, now, 5)
	operationID := ids.NewAt(ids.KindOperation, now, 6)
	artifactID := ids.NewAt(ids.KindConfig, now, 7)
	planID := ids.NewAt(ids.KindPlan, now, 8)
	yaml := []byte("services:\n  api:\n    image: example.invalid/api:1\n  worker:\n    image: example.invalid/worker:1\nnetworks: {}\n")
	digest := sha256.Sum256(yaml)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProjectName:   "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: yaml, YamlSha256: digest[:],
		Services: []*agentpb.ComposeService{
			{ServiceId: firstServiceID, ComposeName: "api", ExpectedLabels: zoneRemovalPlanTestLabels(environmentID, firstServiceID, planID)},
			{ServiceId: secondServiceID, ComposeName: "worker", ExpectedLabels: zoneRemovalPlanTestLabels(environmentID, secondServiceID, planID)},
		},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, Executor: etcd.TaskExecutorAgent,
		PlanID: planID, RenderGeneration: 2, Type: etcd.TaskRemove, Target: zoneID,
		Params: map[string]string{
			etcd.TaskZoneEnvironmentParam: environmentID, etcd.TaskZoneRemovalOperationParam: operationID,
			etcd.EnvironmentDesiredRevisionParam: taskID, etcd.TaskComposeArtifactParam: artifactID,
		},
		Steps: []etcd.TaskStepRecord{
			{ID: ids.NewAt(ids.KindStep, now, 9)}, {ID: ids.NewAt(ids.KindStep, now, 10)},
			{ID: ids.NewAt(ids.KindStep, now, 11)},
		},
		TimeoutSeconds: 120,
	}
	intent := etcd.ZoneRemovalIntent{
		OperationID: operationID, ActiveTaskID: taskID, EnvironmentID: environmentID, ZoneID: zoneID,
		Status: etcd.TaskStatusPending, AffectedServiceIDs: []string{firstServiceID, secondServiceID},
		Claim: etcd.EnvironmentBlueprintStageClaim{RevisionID: taskID},
		CandidateProjection: etcd.EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: taskID, RenderGeneration: 2, ComposeArtifact: artifact,
		},
	}
	plan, err := (&TaskPlanResolver{volumeRoot: "/var/lib/groundplane/vol"}).buildZoneRemovalPlan(task, intent)
	if err != nil {
		t.Fatalf("buildZoneRemovalPlan() error = %v", err)
	}
	if len(plan.Steps) != 3 || plan.Steps[0].GetComposeApply().GetServiceIds()[0] != firstServiceID ||
		plan.Steps[1].GetComposeApply().GetServiceIds()[0] != secondServiceID ||
		plan.Steps[1].GetPrerequisiteStepId() != plan.Steps[0].GetStepId() ||
		plan.Steps[2].GetManagedNetworkRemove().GetNetworkId() != zoneID ||
		plan.Steps[2].GetPrerequisiteStepId() != plan.Steps[1].GetStepId() {
		t.Fatalf("Zone removal plan = %#v", plan)
	}
}

func zoneRemovalPlanTestLabels(environmentID, serviceID, planID string) []*agentpb.LabelPair {
	return []*agentpb.LabelPair{
		{Key: "com.groundplane.environment-id", Value: environmentID},
		{Key: "com.groundplane.kind", Value: "service"},
		{Key: "com.groundplane.managed", Value: "true"},
		{Key: "com.groundplane.plan-id", Value: planID},
		{Key: "com.groundplane.render-generation", Value: "2"},
		{Key: "com.groundplane.service-id", Value: serviceID},
	}
}
