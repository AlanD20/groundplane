package etcd_test

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// This assembles the existing wire fields from an actual persisted claim. The
// real Agent admission validates its digest, exact PlanHash-bound native bytes,
// explicit member selections and existing aggregate witness bounds. It does not
// run Docker or claim that gRPC transport/physical execution was exercised.
func proveBoundedBlueprintAgentAdmission(t *testing.T, claim etcd.TaskAssignment, plan *agentpb.ExecutionPlan) {
	t.Helper()
	record, authority := claim.Assignment.Record, claim.Assignment.Record.RestorationAuthority
	wire := &agentpb.ReleaseRestorationAuthority{TaskId: authority.TaskID, OperationId: authority.OperationID,
		PlanHash: decodeTestDigest(t, authority.PlanHash), EnvironmentId: authority.EnvironmentID,
		CandidateArtifactId: authority.CandidateArtifactID,
		AuthoritySha256:     decodeTestDigest(t, record.RestorationAuthoritySHA256)}
	for _, candidate := range authority.Candidates {
		target := agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE
		if candidate.Target == etcd.ReleaseRestorationServingPredecessor {
			target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR
		}
		wire.Candidates = append(wire.Candidates, &agentpb.ReleaseRestorationCandidate{
			ServiceId: candidate.ServiceID, ReleaseId: candidate.ReleaseID, Target: target,
		})
	}
	for _, native := range authority.NativePredecessors {
		wire.NativePredecessors = append(wire.NativePredecessors, &agentpb.ReleaseNativePredecessorAuthority{
			ServiceId: native.ServiceID, CurrentArtifact: slices.Clone(native.CurrentArtifact),
			RetainedPriorArtifact: slices.Clone(native.RetainedPriorArtifact),
		})
	}
	if applied := authority.AppliedPredecessor; applied != nil {
		wire.AppliedPredecessor = &agentpb.ReleaseAppliedPredecessorAuthority{KeyRevision: applied.KeyRevision,
			RevisionId: applied.RevisionID, RenderGeneration: applied.RenderGeneration,
			ComposeArtifact:       slices.Clone(applied.ComposeArtifact),
			ComposeArtifactSha256: decodeTestDigest(t, applied.ComposeArtifactSHA256)}
	}
	pool := agent.NewWorkerPool(
		1,
		"/var/lib/groundplane/vol",
		runner.NewFake(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := pool.Submit(ctx, agent.Assignment{AssignmentID: record.AssignmentID, TaskID: record.TaskID,
		OperationID: authority.OperationID, Plan: plan, ExecutionEpoch: record.ExecutionEpoch,
		ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		Deadline:      record.Deadline, ForwardDeadline: record.Deadline, RecoveryDeadline: record.RecoveryDeadline,
		RestorationAuthority: wire})
	if err != nil {
		t.Fatalf("real Agent bounded assignment admission: %v", err)
	}
	t.Logf("Agent restoration wire=%d bytes, sealed plan=%d bytes", proto.Size(wire), proto.Size(plan))
}
