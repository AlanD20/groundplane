package controller

import (
	"context"
	"encoding/hex"
	"math"
	"slices"
	"sort"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const EntryMutationBaselineRevisionParam = "entry_baseline_revision_id"

// PrepareTask uses one procedure for initial publication and later
// reconstruction. Only Entry create/edit/bulk-upsert use this path; removal has
// its own terminal-publication and no-restart contract.
func (runtime EntryMutationRuntime) PrepareTask(
	volumeRoot string, task etcd.TaskRecord, candidate etcd.EnvironmentComposeProjection, applyStepID string,
) (etcd.TaskRecord, error) {
	baseline := runtime.Projection
	if runtime.EpochRevision <= 0 {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "Entry runtime capture epoch is absent")
	}
	artifact, err := entryMutationArtifact(candidate)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.EntryRuntime = &etcd.EntryTaskRuntime{RunningServiceIDs: append([]string{}, runtime.RunningServiceIDs...)}
	sort.Strings(task.EntryRuntime.RunningServiceIDs)
	selected, err := entryMutationConsumerIDs(baseline, candidate, artifact, task.EntryRuntime.RunningServiceIDs)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.Params = map[string]string{
		etcd.TaskResourceKindParam:               etcd.TaskResourceEntry,
		etcd.TaskMaterializationEnvironmentParam: candidate.EnvironmentID,
		etcd.EnvironmentDesiredRevisionParam:     candidate.RevisionID,
		etcd.TaskComposeArtifactParam:            artifact.ArtifactId,
		EntryMutationBaselineRevisionParam:       baseline.RevisionID,
		etcd.TaskEntryRuntimeEpochParam:          strconv.FormatInt(runtime.EpochRevision, 10),
	}
	references := append([]etcd.TaskMaterializationRecord(nil), task.Materializations...)
	sort.Slice(
		references,
		func(left, right int) bool { return references[left].Destination < references[right].Destination },
	)
	task.Steps = make([]etcd.TaskStepRecord, 0, len(references)+1)
	for _, reference := range references {
		task.Steps = append(task.Steps, etcd.TaskStepRecord{ID: reference.StepID, Kind: etcd.TaskStepOperation})
	}
	if len(selected) != 0 {
		task.Steps = append(task.Steps, etcd.TaskStepRecord{ID: applyStepID, Kind: etcd.TaskStepOperation})
	}
	plan, err := buildEntryMutationPlan(volumeRoot, task, baseline, candidate)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	return task, nil
}

func (resolver *TaskPlanResolver) resolveUpdatePlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if ids.Validate(ids.KindRoute, task.Target) == nil {
		return resolver.resolveRouteMutationPlan(ctx, task)
	}
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceEntry {
		return resolver.resolveEntryMutationPlan(ctx, task)
	}
	return resolver.resolveEnvironmentBlueprintPlan(ctx, task)
}

func (resolver *TaskPlanResolver) resolveEntryMutationPlan(
	ctx context.Context, task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.blueprints == nil {
		return nil, errs.New(errs.KindInternal, "Entry mutation plan resolver is not configured")
	}
	environmentID := task.Params[etcd.TaskMaterializationEnvironmentParam]
	baseline, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx, environmentID, task.Params[EntryMutationBaselineRevisionParam],
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "Entry mutation baseline is unavailable")
	}
	candidate, found, err := resolver.blueprints.GetEnvironmentComposeProjectionRevision(
		ctx, environmentID, task.Params[etcd.EnvironmentDesiredRevisionParam],
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "Entry mutation candidate is unavailable")
	}
	return buildEntryMutationPlan(resolver.volumeRoot, task, baseline.Record, candidate.Record)
}

func buildEntryMutationPlan(
	volumeRoot string, task etcd.TaskRecord, baseline, candidate etcd.EnvironmentComposeProjection,
) (*agentpb.ExecutionPlan, error) {
	if task.Type != etcd.TaskUpdate || task.Executor != etcd.TaskExecutorAgent || task.RenderGeneration <= 0 ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Params) != 6 ||
		task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceEntry ||
		task.Target != candidate.EnvironmentID || baseline.EnvironmentID != candidate.EnvironmentID ||
		task.Params[etcd.TaskMaterializationEnvironmentParam] != candidate.EnvironmentID ||
		task.Params[etcd.EnvironmentDesiredRevisionParam] != candidate.RevisionID ||
		task.Params[EntryMutationBaselineRevisionParam] != baseline.RevisionID ||
		candidate.RenderGeneration != uint64(
			task.RenderGeneration,
		) || baseline.RenderGeneration >= candidate.RenderGeneration {
		return nil, errs.New(errs.KindInternal, "Entry mutation Task identity is invalid")
	}
	if _, err := etcd.EntryRuntimeEpochRevision(task); err != nil {
		return nil, err
	}
	oldArtifact, err := entryMutationArtifact(baseline)
	if err != nil {
		return nil, err
	}
	newArtifact, err := entryMutationArtifact(candidate)
	if err != nil {
		return nil, err
	}
	// The sealed candidate owns the captured serving runtime. The older desired
	// revision supplies only the previous Entry decorations, not live slot choice.
	oldArtifact, err = mutateEnvironmentEntryArtifact(newArtifact, candidate,
		EnvironmentEntryArtifactMutation{ArtifactID: oldArtifact.ArtifactId, Entries: baseline.Entries})
	if err != nil {
		return nil, err
	}
	if newArtifact.ArtifactId != task.Params[etcd.TaskComposeArtifactParam] {
		return nil, errs.New(errs.KindInternal, "Entry mutation candidate artifact changed")
	}
	selected, err := entryMutationConsumerIDs(baseline, candidate, newArtifact, task.EntryRuntime.RunningServiceIDs)
	if err != nil {
		return nil, err
	}
	expectedSteps := len(task.Materializations)
	if len(selected) != 0 {
		expectedSteps++
	}
	if len(task.Materializations) == 0 || len(task.Steps) != expectedSteps {
		return nil, errs.New(errs.KindInternal, "Entry mutation Task step count is invalid")
	}
	references := make(map[string]etcd.TaskMaterializationRecord, len(task.Materializations))
	for _, reference := range task.Materializations {
		references[reference.StepID] = reference
	}
	steps := make([]*agentpb.ExecutionStep, 0, expectedSteps)
	for index, record := range task.Steps {
		if record.Kind != etcd.TaskStepOperation {
			return nil, errs.New(errs.KindInternal, "Entry mutation Task step kind is invalid")
		}
		if index == len(task.Materializations) {
			steps = append(steps, &agentpb.ExecutionStep{StepId: record.ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
					ArtifactId: newArtifact.ArtifactId, ServiceIds: selected, NoDependencies: true,
				}}})
			continue
		}
		reference, found := references[record.ID]
		if !found {
			return nil, errs.New(errs.KindInternal, "Entry mutation materialization is absent")
		}
		step, err := BuildTaskMaterializationStep(reference, newArtifact.ArtifactId, uint32(task.TimeoutSeconds))
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return BuildPlan(PlanBuildInput{VolumeRoot: volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{newArtifact, oldArtifact}, Steps: steps,
		EntryMutationProcedure: &agentpb.EntryMutationProcedure{
			BaselineArtifactId: oldArtifact.ArtifactId, CandidateArtifactId: newArtifact.ArtifactId,
		}})
}

func entryMutationArtifact(projection etcd.EnvironmentComposeProjection) (*agentpb.ComposeArtifact, error) {
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.OwnerId != projection.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "Entry mutation artifact owner is invalid")
	}
	return artifact, nil
}

func entryMutationConsumerIDs(
	baseline, candidate etcd.EnvironmentComposeProjection, artifact *agentpb.ComposeArtifact,
	runningServiceIDs []string,
) ([]string, error) {
	running := make(map[string]bool, len(runningServiceIDs))
	for _, id := range runningServiceIDs {
		found := false
		for _, service := range artifact.Services {
			if service.GetServiceId() == id &&
				service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				found = true
				break
			}
		}
		if !found {
			return nil, errs.New(errs.KindInternal, "Entry running Service is absent from captured runtime")
		}
		running[id] = true
	}
	previous := make(map[string]etcd.EntryRecord, len(baseline.Entries))
	for _, entry := range baseline.Entries {
		previous[entry.Entry.ID] = entry
	}
	exposed := make(map[string]bool)
	for _, entry := range candidate.Entries {
		prior, exists := previous[entry.Entry.ID]
		delete(previous, entry.Entry.ID)
		if exists && prior.CurrentValueGenerationID == entry.CurrentValueGenerationID &&
			slices.Equal(prior.Entry.Exposure, entry.Entry.Exposure) {
			continue
		}
		for _, name := range entry.Entry.Exposure {
			exposed[name] = true
		}
		for _, name := range prior.Entry.Exposure {
			exposed[name] = true
		}
	}
	if len(previous) != 0 {
		return nil, errs.New(errs.KindInternal, "Entry reconciliation cannot remove an Entry")
	}
	identities, err := ComposeIdentitySnapshotFromProjection(candidate)
	if err != nil {
		return nil, err
	}
	selected := make(map[string]bool)
	for _, identity := range identities.Services {
		if !running[identity.ID] || !exposed["all"] && !exposed[identity.Name] {
			continue
		}
		for _, service := range artifact.Services {
			if service.GetServiceId() == identity.ID &&
				service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				selected[identity.ID] = true
			}
		}
	}
	result := make([]string, 0, len(selected))
	for id := range selected {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}
