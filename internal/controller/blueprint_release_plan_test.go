package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: a first Blueprint candidate has no predecessor to render. Its
// immutable plan must seal both lawful restoration alternatives and remain
// identical when applied predecessor state advances before claim.
func TestPrepareBlueprintReleaseTaskFirstCandidateAuthorityIsPredecessorIndependent(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	const candidateReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	task.Params[etcd.TaskReleasePublicationParam] = "publication"
	member := etcd.ReleaseTaskRenderMember{
		Intent: domain.Intent{
			ID: candidateReleaseID, EnvironmentID: reader.environment.ID, ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
			Image: "example/api:next", Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack,
		},
		Render: etcd.ReleaseRenderInput{
			ReleaseID: candidateReleaseID, PlanID: task.PlanID, ArtifactID: task.Params[EnvironmentBlueprintArtifactParam],
			ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api", Image: "example/api:next",
			Strategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID,
			ProjectSlug: reader.project.Slug, EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
			AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection,
		},
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	input := BlueprintReleasePlanInput{
		Members:                   []etcd.ReleaseTaskRenderMember{member},
		ApplyStepIDs:              []string{task.Steps[0].ID},
		HealthStepIDs:             []string{task.Steps[1].ID},
		RecoveryProbeStepIDs:      []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAY"},
		RecoveryCompensateStepIDs: []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAZ"},
		PostStepIDs:               [][]string{nil},
	}
	_, plan, err := resolver.PrepareBlueprintReleaseTask(context.Background(), task, input)
	if err != nil {
		t.Fatalf("PrepareBlueprintReleaseTask() error = %v", err)
	}
	if plan == nil || len(plan.Artifacts) != 1 || plan.GetCandidateReleaseProcedure() == nil {
		t.Fatalf("Blueprint Release plan = %#v, want one artifact and one procedure", plan)
	}
	procedureMember := plan.GetCandidateReleaseProcedure().GetMembers()[0]
	if procedureMember.GetCandidateAbsence() == nil || procedureMember.GetServingPredecessor() == nil {
		t.Fatalf("Blueprint restoration alternatives = %#v, want both", procedureMember)
	}
	input.Members[0].Intent.PriorServingReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FB0"
	input.Members[0].Render.PriorArtifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FB1"
	input.Members[0].Render.PriorImage = "example/api:advanced"
	_, advanced, err := resolver.PrepareBlueprintReleaseTask(context.Background(), task, input)
	if err != nil {
		t.Fatalf("PrepareBlueprintReleaseTask(advanced predecessor) error = %v", err)
	}
	if !bytes.Equal(plan.GetPlanHash(), advanced.GetPlanHash()) {
		t.Fatalf("Blueprint plan hash changed with applied predecessor: %x != %x", plan.GetPlanHash(), advanced.GetPlanHash())
	}
}

// Rationale: an addressable recreate candidate legitimately renders a stable
// proxy and a singleton workload with the same Service id; only the workload
// carries candidate Release ownership and must receive the candidate image.
func TestPrepareBlueprintReleaseTaskBindsAddressableRecreateWorkload(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {
			Name: "api", Image: "example/api:latest", Expose: []string{"8080"},
			Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
			Volumes: []composetypes.ServiceVolumeConfig{{
				Type: composetypes.VolumeTypeVolume, Source: "app-data", Target: "/data",
			}},
		}},
		Networks: composetypes.Networks{"frontend": {}},
		Volumes:  composetypes.Volumes{"app-data": {}},
	}
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatalf("marshal exposed normalized Compose fixture: %v", err)
	}
	reader.projection.NormalizedCompose = normalized
	const candidateReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	task.Params[etcd.TaskReleasePublicationParam] = "publication"
	member := etcd.ReleaseTaskRenderMember{
		Intent: domain.Intent{
			ID: candidateReleaseID, EnvironmentID: reader.environment.ID, ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
			Image: "example/api:next", Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack,
		},
		Render: etcd.ReleaseRenderInput{
			ReleaseID: candidateReleaseID, PlanID: task.PlanID, ArtifactID: task.Params[EnvironmentBlueprintArtifactParam],
			ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api", Image: "example/api:next",
			Strategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID,
			ProjectSlug: reader.project.Slug, EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
			AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection,
		},
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	_, plan, err := resolver.PrepareBlueprintReleaseTask(context.Background(), task, BlueprintReleasePlanInput{
		Members:                   []etcd.ReleaseTaskRenderMember{member},
		ApplyStepIDs:              []string{task.Steps[0].ID},
		HealthStepIDs:             []string{task.Steps[1].ID},
		RecoveryProbeStepIDs:      []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FB2"},
		RecoveryCompensateStepIDs: []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FB3"},
		PostStepIDs:               [][]string{nil},
	})
	if err != nil {
		t.Fatalf("PrepareBlueprintReleaseTask() error = %v", err)
	}
	if plan == nil || len(plan.Artifacts) != 1 || len(plan.Artifacts[0].Services) != 2 {
		t.Fatalf("addressable recreate artifact = %#v, want stable proxy plus singleton", plan)
	}
	candidateWorkloads := 0
	for _, service := range plan.Artifacts[0].Services {
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			continue
		}
		candidateWorkloads++
		if service.GetImageReference() != member.Render.Image {
			t.Fatalf("candidate workload image = %q, want %q", service.GetImageReference(), member.Render.Image)
		}
	}
	if candidateWorkloads != 1 {
		t.Fatalf("candidate recreate workloads = %d, want one", candidateWorkloads)
	}
}

// Rationale: every ordinary Blueprint execution step belongs to the Release
// forward path, while caller-owned step messages must remain reusable and
// unchanged after the immutable plan is prepared.
func TestPrepareBlueprintReleaseTaskOwnsForwardStepPolicies(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	const (
		candidateReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		serviceID          = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		applyStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FB6"
		healthStepID       = "step_01ARZ3NDEKTSV4RRFFQ69G5FB7"
		componentStepID    = "step_01ARZ3NDEKTSV4RRFFQ69G5FB5"
	)
	task.Params[etcd.TaskReleasePublicationParam] = "publication"
	member := etcd.ReleaseTaskRenderMember{
		Intent: domain.Intent{
			ID: candidateReleaseID, EnvironmentID: reader.environment.ID, ServiceID: serviceID,
			OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
			Image: "example/api:next", Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack,
		},
		Render: etcd.ReleaseRenderInput{
			ReleaseID: candidateReleaseID, PlanID: task.PlanID, ArtifactID: task.Params[EnvironmentBlueprintArtifactParam],
			ServiceID: serviceID, ServiceName: "api", Image: "example/api:next",
			Strategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID,
			ProjectSlug: reader.project.Slug, EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
			AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection,
		},
	}
	artifactID := task.Params[EnvironmentBlueprintArtifactParam]
	const componentID = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	projectionArtifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(reader.projection.ComposeArtifact, projectionArtifact); err != nil {
		t.Fatalf("unmarshal projection artifact: %v", err)
	}
	projectionArtifact.Services[0].OwnerComponentId = componentID
	projectionBytes, err := proto.Marshal(projectionArtifact)
	if err != nil {
		t.Fatalf("marshal component-owned projection artifact: %v", err)
	}
	reader.projection.ComposeArtifact = projectionBytes
	reader.projection.Components = []etcd.ComponentRecord{{
		Desired: etcd.ComponentDesiredRecord{ID: componentID},
		Runtime: etcd.ComponentRuntimeRecord{GeneratedServices: []string{serviceID}},
	}}
	member.Render.Projection = reader.projection
	prefix, err := BuildTaskMaterializationStep(
		task.Materializations[0], artifactID, uint32(task.TimeoutSeconds),
	)
	if err != nil {
		t.Fatalf("BuildTaskMaterializationStep() error = %v", err)
	}
	prefixStepID := prefix.GetStepId()
	definitionDigest := sha256.Sum256([]byte("component definition"))
	catalogDigest := sha256.Sum256([]byte("component catalog"))
	materializationDigest := sha256.Sum256(nil)
	component := &agentpb.ExecutionStep{
		StepId: componentStepID, TimeoutSeconds: uint32(task.TimeoutSeconds), PrerequisiteStepId: prefixStepID,
		Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
			ComponentId: componentID, DefinitionDigest: definitionDigest[:], CatalogDigest: catalogDigest[:],
			ActionId: "activate-config", ArtifactId: task.Materializations[0].MaterializationID,
			ArtifactDigest: materializationDigest[:], Generation: uint64(task.RenderGeneration), ManagedConfigContent: true,
		}},
	}
	prefixBefore := proto.Clone(prefix).(*agentpb.ExecutionStep)
	componentBefore := proto.Clone(component).(*agentpb.ExecutionStep)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	input := BlueprintReleasePlanInput{
		Members: []etcd.ReleaseTaskRenderMember{member}, PrefixSteps: []*agentpb.ExecutionStep{prefix},
		ComponentSteps:            []*agentpb.ExecutionStep{component},
		ApplyStepIDs:              []string{applyStepID},
		HealthStepIDs:             []string{healthStepID},
		RecoveryProbeStepIDs:      []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FB2"},
		RecoveryCompensateStepIDs: []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FB3"},
		PostStepIDs:               [][]string{nil},
	}
	_, plan, err := resolver.PrepareBlueprintReleaseTask(context.Background(), task, input)
	if err != nil {
		t.Fatalf("PrepareBlueprintReleaseTask() error = %v", err)
	}
	if !proto.Equal(prefix, prefixBefore) || !proto.Equal(component, componentBefore) {
		t.Fatalf("caller steps mutated: prefix = %#v, component = %#v", prefix, component)
	}
	wantForward := map[string]bool{
		prefixStepID: true, applyStepID: true, healthStepID: true, componentStepID: true,
	}
	for _, step := range plan.GetSteps() {
		if !wantForward[step.GetStepId()] {
			continue
		}
		if step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD {
			t.Errorf("step %q policy = %s, want RELEASE_FORWARD", step.GetStepId(), step.GetPolicy())
		}
		delete(wantForward, step.GetStepId())
	}
	if len(wantForward) != 0 {
		t.Fatalf("prepared plan omitted forward steps: %v", wantForward)
	}
	forwardStepIDs := plan.GetCandidateReleaseProcedure().GetMembers()[0].GetForwardStepIds()
	if len(forwardStepIDs) != 2 || forwardStepIDs[0] != applyStepID || forwardStepIDs[1] != healthStepID {
		t.Fatalf("candidate forward anchors = %q, want apply and health", forwardStepIDs)
	}
	originalDestination := prefix.GetMaterializeFile().GetDestination()
	plan.GetSteps()[0].GetMaterializeFile().Destination = "changed"
	if prefix.GetMaterializeFile().GetDestination() != originalDestination {
		t.Fatal("returned plan shares nested prefix-step state with its caller")
	}

	recoveryProbe := &agentpb.ExecutionStep{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FB8", TimeoutSeconds: uint32(task.TimeoutSeconds),
		Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
			CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
				CandidateArtifactId: artifactID, ServiceId: serviceID, CandidateReleaseId: candidateReleaseID,
			},
		},
	}
	recoveryCompensate := &agentpb.ExecutionStep{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FB9", TimeoutSeconds: uint32(task.TimeoutSeconds),
		Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
			CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
				CandidateArtifactId: artifactID, ServiceId: serviceID, CandidateReleaseId: candidateReleaseID,
			},
		},
	}
	alreadyPolicyBearingPrefix := proto.Clone(prefix).(*agentpb.ExecutionStep)
	alreadyPolicyBearingPrefix.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
	alreadyPolicyBearingComponent := proto.Clone(component).(*agentpb.ExecutionStep)
	alreadyPolicyBearingComponent.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
	tests := []struct {
		name   string
		mutate func(*BlueprintReleasePlanInput)
	}{
		{name: "recovery probe in prefix", mutate: func(candidate *BlueprintReleasePlanInput) {
			candidate.PrefixSteps = append(append([]*agentpb.ExecutionStep(nil), candidate.PrefixSteps...), recoveryProbe)
		}},
		{name: "recovery compensation in component stage", mutate: func(candidate *BlueprintReleasePlanInput) {
			candidate.ComponentSteps = append(
				append([]*agentpb.ExecutionStep(nil), candidate.ComponentSteps...), recoveryCompensate,
			)
		}},
		{name: "already policy-bearing prefix", mutate: func(candidate *BlueprintReleasePlanInput) {
			candidate.PrefixSteps = []*agentpb.ExecutionStep{alreadyPolicyBearingPrefix}
		}},
		{name: "already policy-bearing component", mutate: func(candidate *BlueprintReleasePlanInput) {
			candidate.ComponentSteps = []*agentpb.ExecutionStep{alreadyPolicyBearingComponent}
		}},
		{name: "Component payload in prefix", mutate: func(candidate *BlueprintReleasePlanInput) {
			candidate.PrefixSteps = []*agentpb.ExecutionStep{component}
		}},
		{name: "prefix payload in Component stage", mutate: func(candidate *BlueprintReleasePlanInput) {
			candidate.ComponentSteps = []*agentpb.ExecutionStep{prefix}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := input
			test.mutate(&candidate)
			if _, _, err := resolver.PrepareBlueprintReleaseTask(context.Background(), task, candidate); err == nil {
				t.Fatal("PrepareBlueprintReleaseTask() accepted an invalid caller-owned forward step")
			}
		})
	}
}

// Rationale: candidate image binding must fail closed when the sealed Compose
// artifact contains two candidate workloads for one stable Service identity.
func TestBindBlueprintCandidateServiceImagesRejectsDuplicateCandidateWorkload(t *testing.T) {
	const (
		serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		releaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		image     = "example/api:next"
	)
	labels := []*agentpb.LabelPair{{Key: composeLabelReleaseID, Value: releaseID}}
	artifact := &agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{
		{ServiceId: serviceID, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, ExpectedLabels: labels},
		{ServiceId: serviceID, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, ExpectedLabels: labels},
	}}
	members := []etcd.ReleaseTaskRenderMember{{
		Intent: domain.Intent{ID: releaseID},
		Render: etcd.ReleaseRenderInput{ServiceID: serviceID, Image: image, Strategy: domain.StrategyRecreate},
	}}
	if err := bindBlueprintCandidateServiceImages(artifact, members); err == nil {
		t.Fatal("bindBlueprintCandidateServiceImages() accepted duplicate candidate workloads")
	}
}
