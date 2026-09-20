package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

// BuildReleaseHookRenderInput reuses the manual Script projection boundary
// while retaining the parent release Task and candidate Release identities.
func BuildReleaseHookRenderInput(
	ctx context.Context,
	input ManualScriptPlanInput,
) (etcd.ReleaseHookRenderInput, error) {
	plan, err := buildScriptRunnerPlan(ctx, input)
	if err != nil {
		return etcd.ReleaseHookRenderInput{}, err
	}
	snapshot := plan.ScriptRunnerSnapshots[0]
	projection := plan.ScriptRunnerProjections[0]
	body := plan.ScriptBodyArtifacts[0]
	run := plan.Steps[0].GetRunScript()
	snapshotBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
	if err != nil {
		return etcd.ReleaseHookRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	projectionBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(projection)
	if err != nil {
		clear(snapshotBytes)
		return etcd.ReleaseHookRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.ReleaseHookRenderInput{
		ScriptID:          input.Sources.Script.Record.Desired.ID,
		ScriptSlug:        input.Sources.Script.Record.Desired.Slug,
		ServiceID:         input.Sources.Service.Record.Desired.ID,
		When:              input.Sources.Script.Record.Desired.When,
		Order:             input.Sources.Script.Record.Desired.Order,
		ScriptGeneration:  input.Sources.BodyGeneration.Record.Generation,
		ScriptExecutionID: input.ExecutionID, RunnerSnapshotID: input.SnapshotID,
		BodySize: body.Size, BodySHA256: hex.EncodeToString(body.Sha256),
		ServiceDefinitionSHA256: hex.EncodeToString(run.ServiceDefinitionSha256),
		RunnerSnapshot:          snapshotBytes, RunnerProjection: projectionBytes,
	}, nil
}

// ReleaseHookPlanInput supplies task-owned IDs around candidate start and
// compensation checkpoints. Automatic hooks remain steps in the parent task.
type ReleaseHookPlanInput struct {
	Operation            domain.OperationKind
	CandidateReleaseID   string
	FailureReleaseID     string
	PostHookAnchorStepID string
	CompensationStepID   string
	PreStepIDs           []string
	PostStepIDs          []string
	FailureStepIDs       []string
	Hooks                []etcd.ReleaseHookRenderInput
}

// ReleaseHookPlan is typed plan material for BuildPlan.
type ReleaseHookPlan struct {
	PreSteps     []*agentpb.ExecutionStep
	PostSteps    []*agentpb.ExecutionStep
	FailureSteps []*agentpb.ExecutionStep
	Snapshots    []*agentpb.ResolvedRunnerSnapshot
	Projections  []*agentpb.ScriptRunnerProjection
	Bodies       []*agentpb.ScriptBodyArtifactMetadata
}

// BuildReleaseHookPlan orders hooks by captured numeric order then slug and chains each
// phase to its release checkpoint. It never starts a container or creates a
// compatibility execution path.
func BuildReleaseHookPlan(input ReleaseHookPlanInput) (ReleaseHookPlan, error) {
	var output ReleaseHookPlan
	preWhen, postWhen := core.ScriptPreDeploy, core.ScriptPostDeploy
	if input.Operation == domain.OperationRollback {
		preWhen, postWhen = core.ScriptPreRollback, core.ScriptPostRollback
	}
	var pre, post, failure []etcd.ReleaseHookRenderInput
	for _, hook := range input.Hooks {
		switch hook.When {
		case preWhen:
			pre = append(pre, hook)
		case postWhen:
			post = append(post, hook)
		case core.ScriptOnFailure:
			failure = append(failure, hook)
		}
	}
	sort.SliceStable(pre, func(i, j int) bool { return releaseHookBefore(pre[i], pre[j]) })
	sort.SliceStable(post, func(i, j int) bool { return releaseHookBefore(post[i], post[j]) })
	sort.SliceStable(failure, func(i, j int) bool { return releaseHookBefore(failure[i], failure[j]) })
	if len(input.PreStepIDs) != len(pre) || len(input.PostStepIDs) != len(post) ||
		len(input.FailureStepIDs) != len(failure) {
		return output, errs.New(errs.KindValidationFailed, "release hook step IDs do not match selected hooks")
	}
	appendPhase := func(
		hooks []etcd.ReleaseHookRenderInput,
		stepIDs []string,
		firstPrerequisite string,
		releaseID string,
		target *[]*agentpb.ExecutionStep,
	) error {
		prerequisite := firstPrerequisite
		for index, hook := range hooks {
			step, snapshot, projection, body, err := buildReleaseHookStep(
				hook, stepIDs[index], prerequisite, releaseID,
			)
			if err != nil {
				return err
			}
			*target = append(*target, step)
			output.Snapshots = append(output.Snapshots, snapshot)
			output.Projections = append(output.Projections, projection)
			output.Bodies = append(output.Bodies, body)
			prerequisite = stepIDs[index]
		}
		return nil
	}
	if err := appendPhase(pre, input.PreStepIDs, "", input.CandidateReleaseID, &output.PreSteps); err != nil {
		return output, err
	}
	if err := appendPhase(
		post, input.PostStepIDs, input.PostHookAnchorStepID, input.CandidateReleaseID, &output.PostSteps,
	); err != nil {
		return output, err
	}
	if err := appendPhase(
		failure, input.FailureStepIDs, input.CompensationStepID, input.FailureReleaseID, &output.FailureSteps,
	); err != nil {
		return output, err
	}
	return output, nil
}

func releaseHookBefore(left, right etcd.ReleaseHookRenderInput) bool {
	return core.ScriptBefore(
		core.Script{Order: left.Order, Slug: left.ScriptSlug},
		core.Script{Order: right.Order, Slug: right.ScriptSlug},
	)
}

func buildReleaseHookStep(
	hook etcd.ReleaseHookRenderInput,
	stepID, prerequisite, releaseID string,
) (*agentpb.ExecutionStep, *agentpb.ResolvedRunnerSnapshot, *agentpb.ScriptRunnerProjection, *agentpb.ScriptBodyArtifactMetadata, error) {
	if _, err := ulid.ParseStrict(hook.ScriptExecutionID); err != nil {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "release hook execution identity is invalid")
	}
	if _, err := ulid.ParseStrict(hook.RunnerSnapshotID); err != nil {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "release hook snapshot identity is invalid")
	}
	var snapshot agentpb.ResolvedRunnerSnapshot
	var projection agentpb.ScriptRunnerProjection
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(hook.RunnerSnapshot, &snapshot); err != nil {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "release hook runner snapshot is invalid")
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(hook.RunnerProjection, &projection); err != nil {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "release hook runner projection is invalid")
	}
	if snapshot.ScriptExecutionId != hook.ScriptExecutionID || snapshot.SnapshotId != hook.RunnerSnapshotID ||
		snapshot.ServiceId != hook.ServiceID || snapshot.ReleaseId != releaseID ||
		projection.SnapshotId != hook.RunnerSnapshotID {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "release hook runner references do not match")
	}
	bodySHA, err := hex.DecodeString(hook.BodySHA256)
	if err != nil || len(bodySHA) != 32 {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "release hook body digest is invalid")
	}
	serviceSHA, err := hex.DecodeString(hook.ServiceDefinitionSHA256)
	if err != nil || len(serviceSHA) != 32 {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "release hook service digest is invalid")
	}
	snapshotSHA := sha256.Sum256(hook.RunnerSnapshot)
	body := &agentpb.ScriptBodyArtifactMetadata{
		ScriptExecutionId: hook.ScriptExecutionID,
		ScriptId:          hook.ScriptID,
		Generation:        hook.ScriptGeneration,
		Size:              hook.BodySize,
		Sha256:            bodySHA,
		Uid:               projection.Uid,
		Gid:               projection.Gid,
	}
	run := &agentpb.RunScript{
		ScriptExecutionId:       hook.ScriptExecutionID,
		ScriptId:                hook.ScriptID,
		ScriptGeneration:        hook.ScriptGeneration,
		EnvironmentId:           snapshot.EnvironmentId,
		ServiceId:               snapshot.ServiceId,
		ReleaseId:               snapshot.ReleaseId,
		RenderGeneration:        snapshot.RenderGeneration,
		ServiceDefinitionSha256: serviceSHA,
		BodySha256:              bodySHA,
		RunnerSnapshotId:        hook.RunnerSnapshotID,
		RunnerSnapshotSha256:    snapshotSHA[:],
	}
	policy := agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK
	switch hook.When {
	case core.ScriptPreDeploy, core.ScriptPreRollback:
		policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK
	case core.ScriptPostDeploy, core.ScriptPostRollback:
		policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK
	}
	step := &agentpb.ExecutionStep{
		StepId: stepID, Policy: policy, PrerequisiteStepId: prerequisite,
		TimeoutSeconds: executionplan.ScriptExecutionTimeoutSeconds,
		Payload:        &agentpb.ExecutionStep_RunScript{RunScript: run},
	}
	return step, &snapshot, &projection, body, nil
}
