package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent"
	migratedcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/docker/composeobserver"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// This double is only the Docker side-effect boundary. Admission, execution,
// postcondition checking, durable event ACKs, aggregation and terminal validation
// all use the actual Agent and Controller persistence implementations.
type mixedRecoveryRuntime struct {
	t            *testing.T
	expected     testtaskjournal.TaskResultRecord
	witness      *agentpb.ComposeArtifact
	restored     map[string]bool
	calls        []string
	observations int
}

func (runtime *mixedRecoveryRuntime) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	var step *agentpb.ExecutionStep
	for _, candidate := range request.Plan.Steps {
		if candidate.StepId == request.StepId {
			step = candidate
		}
	}
	if step == nil {
		runtime.t.Fatal("helper received an undeclared step")
	}
	serviceID := step.GetCandidateRestorationProbe().GetServiceId()
	compensate := step.GetCandidateRestorationCompensate() != nil
	if compensate {
		serviceID = step.GetCandidateRestorationCompensate().GetServiceId()
	}
	if serviceID == "" {
		runtime.t.Fatal("recovery invoked a forward/helper step")
	}
	runtime.calls = append(runtime.calls, step.StepId)
	response := &agentpb.ComposeHelperResponse{
		Schema:     1,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
	for _, member := range request.RestorationAuthority.Candidates {
		if member.ServiceId != serviceID {
			continue
		}
		if compensate {
			runtime.restored[serviceID] = true
		}
		if member.Target == agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE {
			response.CandidateAbsenceEvidence = &agentpb.CandidateAbsenceEvidence{
				AssignmentId: request.AssignmentId, PlanHash: slices.Clone(request.Plan.PlanHash),
				AuthoritySha256: slices.Clone(request.RestorationAuthority.AuthoritySha256),
				CandidateArtifactId: memberArtifact(
					request.Plan,
					serviceID,
				), ComposeProjectName: runtime.witness.ProjectName,
				Candidates:    []*agentpb.CandidateReleaseService{{ServiceId: serviceID, ReleaseId: member.ReleaseId}},
				AbsenceProven: runtime.restored[serviceID],
			}
			return response, nil
		}
		if !runtime.restored[serviceID] {
			response.Outcome = agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED
			return response, nil
		}
		for _, evidence := range runtime.expected.ProxyEvidence {
			if evidence.ServiceID == serviceID {
				response.ProxyEvidence = &agentpb.ServiceProxyEvidence{
					ServiceId:       serviceID,
					ReleaseId:       evidence.ReleaseID,
					Target:          evidence.Target,
					Compensated:     true,
					ProxyGeneration: evidence.ProxyGeneration,
					ConfigSha256:    decodeTestDigest(runtime.t, evidence.ConfigSHA256),
				}
			}
		}
		for _, evidence := range runtime.expected.RecreateEvidence {
			if evidence.ServiceID == serviceID {
				response.RecreateEvidence = &agentpb.ServiceRecreateEvidence{
					ServiceId:   serviceID,
					ReleaseId:   evidence.ReleaseID,
					ArtifactId:  evidence.ArtifactID,
					Target:      evidence.Target,
					Compensated: true,
				}
			}
		}
		return response, nil
	}
	runtime.t.Fatal("helper received an unselected member")
	return nil, nil
}

func memberArtifact(plan *agentpb.ExecutionPlan, serviceID string) string {
	for _, member := range plan.CandidateReleaseProcedure.Members {
		if member.ServiceId == serviceID {
			return member.CandidateArtifactId
		}
	}
	return ""
}

func (runtime *mixedRecoveryRuntime) Observe(
	context.Context,
	*agentpb.ExecutionPlan,
	string,
) (*agentpb.ObservedProject, error) {
	runtime.t.Fatal("historical recovery used current-plan observation")
	return nil, nil
}

func (runtime *mixedRecoveryRuntime) ObserveReleaseRestoration(
	context.Context,
	*agentpb.ExecutionPlan,
	string,
	string,
) (*agentpb.ObservedProject, error) {
	runtime.t.Fatal("historical recovery used forward release-restoration observation")
	return nil, nil
}

func (runtime *mixedRecoveryRuntime) ObserveRestoration(
	ctx context.Context,
	observation *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	runtime.observations++
	artifact := observation.Artifact()
	if artifact == nil || !proto.Equal(artifact, runtime.witness) {
		runtime.t.Fatal("observer did not receive exact sealed predecessor")
	}
	observer, err := composeobserver.NewWithEngine(newMixedRecoveryEngine(runtime))
	if err != nil {
		runtime.t.Fatal(err)
	}
	defer observer.Close()
	return observer.ObserveRestoration(ctx, observation)
}

func executeMixedWorkerRecovery(t *testing.T, fixture *ExecutedArtifactFixture, recovery etcd.TaskAssignment,
	plan *agentpb.ExecutionPlan, expected testtaskjournal.TaskResultRecord,
) testtaskjournal.TaskResultRecord {
	t.Helper()
	assignment := mixedWorkerAssignment(t, recovery, plan)
	if len(plan.ScriptBodyArtifacts) != 0 {
		scripts, _ := fixture.HookDependencies(t)
		artifacts, err := scripts.ResolveScriptAssignmentArtifacts(context.Background(), recovery.Task.Record, plan)
		if err != nil {
			t.Fatalf("actual mixed recovery Script artifacts: %v", err)
		}
		assignment.ScriptArtifacts = artifacts
		service, err := testtaskplanning.NewScriptArtifactService(scripts, unexpectedHookEntryResolver{t: t})
		if err != nil {
			t.Fatal(err)
		}
		assignment.ScriptCheckpoints, err = service.ResolveScriptExecutionCheckpoints(
			context.Background(),
			recovery.Task.Record,
			plan,
		)
		if err != nil {
			t.Fatalf("actual mixed recovery Script checkpoints: %v", err)
		}
	}
	witness := proveMixedObservationBoundary(t, assignment)
	runtime := &mixedRecoveryRuntime{t: t, witness: witness, expected: expected, restored: make(map[string]bool)}
	compose, err := migratedcomposeruntime.New(runtime, runtime)
	if err != nil {
		t.Fatal(err)
	}
	pool := agent.NewWorkerPoolWithRuntimes(
		1,
		"/var/lib/groundplane/vol",
		runner.NewFake(),
		slog.New(slog.NewTextHandler(os.Stderr, nil)),
		compose,
		nil,
		nil,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	joined := make(chan struct{})
	go func() { defer close(joined); pool.Run(ctx) }()
	defer func() { cancel(); <-joined }()
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("real mixed Worker admission: %v", err)
	}
	var actual *agent.TaskResult
	for actual == nil {
		select {
		case <-ctx.Done():
			t.Fatal("mixed Worker did not finish")
		case output := <-pool.Outputs():
			if output.Result != nil {
				actual = output.Result
				continue
			}
			progress := output.Progress
			if progress == nil {
				t.Fatal("unexpected Worker output")
			}
			state, wireState := testtaskjournal.TaskEventStateRunning, agentpb.TaskState_TASK_STATE_RUNNING
			if progress.State == agent.TaskProgressCompleted {
				state, wireState = testtaskjournal.TaskEventStateCompleted, agentpb.TaskState_TASK_STATE_COMPLETED
			} else if progress.State == agent.TaskProgressFailed {
				state, wireState = testtaskjournal.TaskEventStateFailed, agentpb.TaskState_TASK_STATE_FAILED
			} else if progress.State != agent.TaskProgressRunning {
				t.Fatalf("mixed Worker step %s failed: %v", progress.StepID, progress.State)
			}
			_, err := fixture.Tasks.AppendTaskEvent(
				ctx,
				testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
					AssignmentID: progress.AssignmentID, AgentID: recovery.Assignment.Record.AgentID, AgentGeneration: 1,
					TaskID: progress.TaskID, StepID: progress.StepID, Attempt: progress.ExecutionEpoch, Ordinal: progress.Ordinal,
				}, State: state, Payload: json.RawMessage(`{"message":"actual worker recovery"}`)},
				time.Now().UTC(),
			)
			if err != nil {
				t.Fatalf("durable mixed Worker event: %v", err)
			}
			if err := pool.AcceptTaskEventAck(ctx, &agentpb.TaskEventAck{
				TaskId: progress.TaskID, AssignmentId: progress.AssignmentID, PlanHash: progress.PlanHash[:],
				StepId: progress.StepID, ExecutionEpoch: progress.ExecutionEpoch, Ordinal: progress.Ordinal, State: wireState,
			}); err != nil {
				t.Fatalf("Worker event ACK: %v", err)
			}
		}
	}
	if actual.Terminal != agent.TaskTerminalCompleted || actual.Compose.GetReconciliationRequired() ||
		actual.Compose.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE ||
		runtime.observations != 1 || len(runtime.restored) != 2 || !slices.Equal(runtime.calls, recovery.ReleaseRecovery.StepIDs) {
		t.Fatalf(
			"mixed Worker recovery did not prove both members: terminal=%v result=%v observations=%d calls=%v",
			actual.Terminal,
			actual.Compose,
			runtime.observations,
			runtime.calls,
		)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind:                        testtaskjournal.TaskResultCompose,
		Diagnostic:                  testtaskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch:              actual.ExecutionEpoch,
		ReleaseRecoveryRecordSHA256: hex.EncodeToString(actual.ReleaseRecoveryRecordSHA256),
	}
	for _, evidence := range actual.Compose.ProxyEvidence {
		result.ProxyEvidence = append(
			result.ProxyEvidence,
			testtaskjournal.TaskProxyEvidence{ServiceID: evidence.ServiceId, ReleaseID: evidence.ReleaseId,
				Target: evidence.Target, Compensated: evidence.Compensated, ProxyGeneration: evidence.ProxyGeneration, ConfigSHA256: hex.EncodeToString(evidence.ConfigSha256)},
		)
	}
	for _, evidence := range actual.Compose.RecreateEvidence {
		result.RecreateEvidence = append(
			result.RecreateEvidence,
			testtaskjournal.TaskRecreateEvidence{ServiceID: evidence.ServiceId, ReleaseID: evidence.ReleaseId,
				ArtifactID: evidence.ArtifactId, Target: evidence.Target, Compensated: evidence.Compensated},
		)
	}
	if evidence := actual.Compose.CandidateAbsenceEvidence; evidence != nil {
		result.CandidateAbsenceEvidence = &testtaskjournal.TaskCandidateAbsenceEvidence{
			AssignmentID: evidence.AssignmentId,
			PlanHash:     hex.EncodeToString(evidence.PlanHash),
			AuthoritySHA256: hex.EncodeToString(
				evidence.AuthoritySha256,
			),
			CandidateArtifactID: evidence.CandidateArtifactId,
			ComposeProjectName:  evidence.ComposeProjectName,
			AbsenceProven:       evidence.AbsenceProven,
		}
		for _, member := range evidence.Candidates {
			result.CandidateAbsenceEvidence.Candidates = append(
				result.CandidateAbsenceEvidence.Candidates,
				testtaskjournal.TaskCandidateAbsenceCandidate{ServiceID: member.ServiceId, ReleaseID: member.ReleaseId},
			)
		}
	}
	return result
}

func mixedWorkerAssignment(
	t *testing.T,
	recovery etcd.TaskAssignment,
	plan *agentpb.ExecutionPlan,
) testtaskassignment.Assignment {
	t.Helper()
	record, authority := recovery.Assignment.Record, recovery.Assignment.Record.RestorationAuthority
	wire := &agentpb.ReleaseRestorationAuthority{
		TaskId:              authority.TaskID,
		OperationId:         authority.OperationID,
		PlanHash:            decodeTestDigest(t, authority.PlanHash),
		EnvironmentId:       authority.EnvironmentID,
		CandidateArtifactId: authority.CandidateArtifactID,
		AuthoritySha256:     decodeTestDigest(t, record.RestorationAuthoritySHA256),
	}
	for _, member := range authority.Candidates {
		target := agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE
		if member.Target == testtaskassignments.ReleaseRestorationServingPredecessor {
			target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR
		}
		wire.Candidates = append(
			wire.Candidates,
			&agentpb.ReleaseRestorationCandidate{
				ServiceId: member.ServiceID,
				ReleaseId: member.ReleaseID,
				Target:    target,
			},
		)
	}
	witness := authority.AppliedPredecessor
	wire.AppliedPredecessor = &agentpb.ReleaseAppliedPredecessorAuthority{
		KeyRevision:           witness.KeyRevision,
		RevisionId:            witness.RevisionID,
		RenderGeneration:      witness.RenderGeneration,
		ComposeArtifact:       slices.Clone(witness.ComposeArtifact),
		ComposeArtifactSha256: decodeTestDigest(t, witness.ComposeArtifactSHA256),
	}
	for _, predecessor := range authority.NativePredecessors {
		wire.NativePredecessors = append(wire.NativePredecessors, &agentpb.ReleaseNativePredecessorAuthority{
			ServiceId:             predecessor.ServiceID,
			CurrentArtifact:       slices.Clone(predecessor.CurrentArtifact),
			RetainedPriorArtifact: slices.Clone(predecessor.RetainedPriorArtifact),
		})
	}
	digest := decodeTestDigest(t, record.ReleaseRecoveryRecordSHA256)
	return testtaskassignment.Assignment{
		AssignmentID:                record.AssignmentID,
		TaskID:                      record.TaskID,
		OperationID:                 authority.OperationID,
		Plan:                        plan,
		Deadline:                    record.RecoveryDeadline,
		ForwardDeadline:             record.Deadline,
		RecoveryDeadline:            record.RecoveryDeadline,
		ExecutionEpoch:              record.ExecutionEpoch,
		ExecutionMode:               agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY,
		RestorationAuthority:        wire,
		ReleaseRecoveryRecordSHA256: digest,
		ReleaseRecoveryDirective: &agentpb.ReleaseRecoveryDirective{
			Phase: agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE, Cursor: recovery.ReleaseRecovery.Cursor,
			StepIds: slices.Clone(
				recovery.ReleaseRecovery.StepIDs,
			), ApplicableCompensationStepIds: slices.Clone(recovery.ReleaseRecovery.ApplicableCompensationStepIDs),
			ReleaseRecoveryRecordSha256: digest,
		},
	}
}

func decodeTestDigest(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("invalid fixture digest: %v", err)
	}
	return decoded
}
