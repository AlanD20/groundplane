package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: per-Service apply/post/health sequencing starts later Services
// after earlier hooks. Blueprint requires global barriers across every member.
// A separate setup image must keep this barrier and the consumer image binding.
func TestPrepareBlueprintReleaseTaskGlobalHookPhases(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	task.Params[etcd.TaskReleasePublicationParam] = "publication"
	project, err := LoadNormalizedEnvironmentProject(t.Context(), reader.projection)
	if err != nil {
		t.Fatal(err)
	}
	worker := project.Services["api"]
	worker.Name = "worker"
	project.Services["worker"] = worker
	reader.projection.NormalizedCompose, err = project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	second := reader.projection.DesiredServices[0]
	second.Desired.ID = ids.NewAt(ids.KindService, task.CreatedAt, 200)
	second.Desired.Name = "worker"
	reader.projection.DesiredServices = append(reader.projection.DesiredServices, second)
	freezeBlueprintNativeRuntimeFixture(t, reader, task, project)
	input := BlueprintReleasePlanInput{}
	for index, service := range reader.projection.DesiredServices {
		releaseID := ids.NewAt(ids.KindDeployment, task.CreatedAt, int64(300+index))
		seal := releaseTestWorkload("example/" + service.Desired.Name + ":next")
		member := etcd.ReleaseTaskRenderMember{
			Intent: domain.Intent{ID: releaseID, EnvironmentID: reader.environment.ID, ServiceID: service.Desired.ID,
				OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply, CandidateWorkload: seal,
				Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack},
			Render: etcd.ReleaseRenderInput{
				ReleaseID: releaseID, PlanID: task.PlanID, ArtifactID: task.Params[EnvironmentBlueprintArtifactParam],
				ServiceID: service.Desired.ID, ServiceName: service.Desired.Name, CandidateWorkload: seal, Strategy: domain.StrategyRecreate,
				CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
				TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug, ProjectID: reader.project.ID, ProjectSlug: reader.project.Slug,
				EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
				AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection,
			},
		}
		member.Render.Hooks = []etcd.ReleaseHookRenderInput{
			blueprintPhaseHookForTest(t, task, member, core.ScriptPreDeploy, int64(600+index*10), index == 0),
			blueprintPhaseHookForTest(t, task, member, core.ScriptPostDeploy, int64(400+index*10), false),
		}
		input.Members = append(input.Members, member)
		stepID := func(offset int64) string { return ids.NewAt(ids.KindStep, task.CreatedAt, int64(500+index*10)+offset) }
		input.ApplyStepIDs = append(input.ApplyStepIDs, stepID(0))
		input.HealthStepIDs = append(input.HealthStepIDs, stepID(1))
		input.RecoveryProbeStepIDs = append(input.RecoveryProbeStepIDs, stepID(2))
		input.RecoveryCompensateStepIDs = append(input.RecoveryCompensateStepIDs, stepID(3))
		input.PostStepIDs = append(input.PostStepIDs, []string{stepID(4)})
		input.PreStepIDs = append(input.PreStepIDs, []string{stepID(5)})
		for hookIndex, hookStep := range []string{stepID(5), stepID(4)} {
			task.Params[etcd.ReleaseHookStepMemberParam(hookStep)] = strconv.Itoa(index + 1)
			task.Params[etcd.ReleaseHookStepExecutionParam(hookStep)] = member.Render.Hooks[hookIndex].ScriptExecutionID
		}
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	updated, plan, err := resolver.PrepareBlueprintReleaseTask(t.Context(), task, input)
	if err != nil {
		t.Fatalf("PrepareBlueprintReleaseTask(): %v", err)
	}
	if _, err := executionplan.Validate(plan); err != nil {
		t.Fatal(err)
	}
	var explicitCount int
	for _, snapshot := range plan.ScriptRunnerSnapshots {
		if snapshot.ExplicitExecution == nil {
			continue
		}
		explicitCount++
		if snapshot.LocalImageId != "sha256:"+strings.Repeat("c", 64) ||
			snapshot.ExplicitExecution.ReleaseLocalImageId != input.Members[0].Intent.CandidateWorkload.LocalImageID ||
			snapshot.LocalImageId == snapshot.ExplicitExecution.ReleaseLocalImageId {
			t.Fatal("setup and consumer image authority were not preserved independently")
		}
	}
	if explicitCount != 1 {
		t.Fatalf("explicit setup snapshots = %d, want 1", explicitCount)
	}
	// Rationale: a first Script mount must not implicitly create an unowned
	// Docker volume before the candidate Compose step supplies its bind options.
	preparedVolumes := make(map[string]bool)
	var volumeSteps []string
	for _, step := range plan.Steps {
		if step.GetRunScript() != nil {
			break
		}
		if ensure := step.GetManagedVolumeEnsure(); ensure != nil {
			if ensure.ArtifactId != plan.Artifacts[0].ArtifactId {
				t.Fatal("volume preparation targets another artifact")
			}
			preparedVolumes[ensure.VolumeId] = true
			volumeSteps = append(volumeSteps, step.StepId)
		}
	}
	for _, volume := range plan.Artifacts[0].Volumes {
		if !preparedVolumes[volume.VolumeId] {
			t.Fatalf("managed volume %s is not prepared before pre-hooks", volume.VolumeId)
		}
	}
	replayedInput, next, err := blueprintReleaseProcedureStepIDs(
		updated,
		etcd.ReleaseTaskRenderInput{Members: input.Members, NativePredecessors: input.NativePredecessors},
		0,
	)
	if err != nil {
		t.Fatalf("published Blueprint cannot reconstruct its durable hook procedure: %v", err)
	}
	_, replayed, err := resolver.PrepareBlueprintReleaseTask(t.Context(), updated, replayedInput)
	if err != nil || next != len(updated.Steps) {
		t.Fatalf("rebuild durable Blueprint: next=%d error=%v", next, err)
	}
	if !proto.Equal(plan, replayed) {
		t.Fatal("durable Blueprint replay changed the sealed execution plan")
	}
	networkIDs, err := blueprintReleaseNetworkStepIDs(updated)
	if err != nil || len(networkIDs) != len(plan.Artifacts[0].Networks) || len(networkIDs) == 0 {
		t.Fatalf("network preparation identities = %v, error = %v", networkIDs, err)
	}
	for index, network := range plan.Artifacts[0].Networks {
		ensure := plan.Steps[index].GetManagedNetworkEnsure()
		if ensure == nil || ensure.NetworkId != network.NetworkId || ensure.ArtifactId != plan.Artifacts[0].ArtifactId {
			t.Fatalf("network %s was not prepared before pre-hooks", network.NetworkId)
		}
	}
	expected := append(append(networkIDs, volumeSteps...), []string{
		input.PreStepIDs[0][0],
		input.PreStepIDs[1][0],
		input.ApplyStepIDs[0],
		input.ApplyStepIDs[1],
		input.PostStepIDs[0][0],
		input.PostStepIDs[1][0],
		input.HealthStepIDs[0],
		input.HealthStepIDs[1],
	}...)
	for index, want := range expected {
		step := plan.Steps[index]
		if step.StepId != want {
			t.Fatalf("step %d=%s, want %s", index, step.StepId, want)
		}
		predecessor := ""
		if index > 0 {
			predecessor = expected[index-1]
		}
		if step.PrerequisiteStepId != predecessor {
			t.Fatalf("step %d prerequisite=%s, want %s", index, step.PrerequisiteStepId, predecessor)
		}
	}
	if len(plan.ScriptRunnerSnapshots) != 4 || len(plan.Steps) != 12+len(networkIDs)+len(volumeSteps) ||
		updated.PlanHash != hex.EncodeToString(plan.PlanHash) {
		t.Fatalf(
			"incomplete hook publication: snapshots=%d steps=%d taskHash=%s",
			len(plan.ScriptRunnerSnapshots),
			len(plan.Steps),
			updated.PlanHash,
		)
	}
}

func blueprintPhaseHookForTest(
	t *testing.T,
	task etcd.TaskRecord,
	member etcd.ReleaseTaskRenderMember,
	when core.ScriptHook,
	sequence int64,
	explicit bool,
) etcd.ReleaseHookRenderInput {
	t.Helper()
	executionID := strings.TrimPrefix(ids.NewAt(ids.KindConfig, task.CreatedAt, sequence), "cfg_")
	snapshotID := strings.TrimPrefix(ids.NewAt(ids.KindConfig, task.CreatedAt, sequence+1), "cfg_")
	scriptID := ids.NewAt(ids.KindScript, task.CreatedAt, sequence+2)
	labels := map[string]string{
		"com.groundplane.managed": "true", "com.groundplane.kind": "script-runner",
		"com.groundplane.tenant-id": member.Render.TenantID, "com.groundplane.project-id": member.Render.ProjectID,
		"com.groundplane.environment-id": member.Render.EnvironmentID, "com.groundplane.service-id": member.Render.ServiceID,
		"com.groundplane.script-id": scriptID, "com.groundplane.script-generation": "1", "com.groundplane.script-execution-id": executionID,
		"com.groundplane.release-id": member.Intent.ID, "com.groundplane.plan-id": task.PlanID,
		"com.groundplane.render-generation": strconv.FormatInt(
			int64(task.RenderGeneration),
			10,
		), "com.groundplane.operation-id": task.OperationID,
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	projection := &agentpb.ScriptRunnerProjection{
		SnapshotId: snapshotID, Name: "gp-script-" + strings.ToLower(executionID), Image: member.Render.CandidateWorkload.LocalImageID,
		Entrypoint: []string{"/bin/sh"}, Command: []string{"/groundplane-script-body"}, StopGraceSeconds: 10,
	}
	if explicit {
		projection.Image, projection.WorkingDir = "sha256:"+strings.Repeat("c", 64), "/"
	}
	for _, key := range keys {
		projection.Labels = append(projection.Labels, &agentpb.ScriptStringPair{Key: key, Value: labels[key]})
	}
	projectionBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	projectionHash := sha256.Sum256(projectionBytes)
	definitionHash := sha256.Sum256([]byte("service definition"))
	source := func() *agentpb.ScriptSourceAuthority {
		return &agentpb.ScriptSourceAuthority{Existing: &agentpb.ScriptExistingSourceAuthority{ModRevision: 1}}
	}
	snapshot := &agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID, TenantId: member.Render.TenantID, TenantModRevision: 1,
		ProjectId: member.Render.ProjectID, ProjectModRevision: 1, EnvironmentId: member.Render.EnvironmentID, EnvironmentModRevision: 1,
		ServiceId: member.Render.ServiceID, ServiceSource: source(), ServiceDefinitionSha256: definitionHash[:],
		ReleaseId: member.Intent.ID, ReleaseModRevision: 1, LocalImageId: member.Render.CandidateWorkload.LocalImageID,
		BlueprintBundleGeneration: task.ID, RenderGeneration: uint64(task.RenderGeneration), NetworkTopologySource: source(),
		AppliedEnvironmentRevisionId: task.ID, AppliedEnvironmentRenderGeneration: uint64(task.RenderGeneration),
		AppliedEnvironmentSource: source(), RunnerProjectionSha256: projectionHash[:],
	}
	if explicit {
		context := &agentpb.ScriptExplicitExecutionContext{
			ImageReference: "example.test/setup@sha256:" + strings.Repeat("c", 64), User: "0:0",
		}
		digest, err := executionplan.ScriptExecutionContextDigest(context)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.ExplicitExecution = &agentpb.ScriptExplicitExecutionAuthority{
			Context: context, ContextSha256: digest, ScriptModRevision: 1,
			ReleaseLocalImageId: member.Render.CandidateWorkload.LocalImageID,
		}
		snapshot.LocalImageId = projection.Image
	}
	snapshotBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	bodyHash := sha256.Sum256([]byte("true"))
	return etcd.ReleaseHookRenderInput{
		ScriptID: scriptID, ScriptSlug: string(when), ServiceID: member.Render.ServiceID, When: when, ScriptGeneration: 1,
		ScriptExecutionID: executionID, RunnerSnapshotID: snapshotID, BodySize: 4, BodySHA256: hex.EncodeToString(bodyHash[:]),
		ServiceDefinitionSHA256: hex.EncodeToString(
			definitionHash[:],
		), RunnerSnapshot: snapshotBytes, RunnerProjection: projectionBytes,
	}
}
