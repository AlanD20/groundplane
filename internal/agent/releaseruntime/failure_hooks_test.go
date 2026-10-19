package releaseruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// TASK-80: run actual failure-hook/Script/journal owners after a supplied
// restoration boundary. Failed hooks retain their own outcome, but only an
// acknowledged cleanup receipt can let recovery continue. No Docker or
// Controller settlement is supplied by this test.
func TestFailureHookRequiresAcknowledgedCleanupAndRetainsItsOwnOutcome(t *testing.T) {
	for _, scenario := range []string{"success", "nonzero exit", "runtime failure", "cleanup failure", "no serving release", "wrong serving release"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFailureHookFixture(t, scenario != "no serving release")
			switch scenario {
			case "nonzero exit":
				f.engine.exitCode = 29
			case "runtime failure":
				f.engine.runErr = errors.New("hook container failed")
			case "cleanup failure":
				f.engine.cleanupErr = errors.New("hook container still exists")
			case "wrong serving release":
				f.delivery.Run.ReleaseId = ids.New(ids.KindDeployment)
			}
			original := f.input.Failure
			proof, err := f.hooks(t).RunRecovery(t.Context(), f.input, f.report)
			if !reflect.DeepEqual(f.input.Failure, original) || f.input.Forward.Outcome != workexecution.Failed {
				t.Fatal("hook changed the original deployment failure")
			}
			if scenario == "cleanup failure" {
				if !errors.Is(err, f.engine.cleanupErr) || proof.Checkpoint != nil {
					t.Fatalf("unproven cleanup advanced recovery: %v", err)
				}
				return
			}
			if err != nil || proof.Validate(f.input, f.report) != nil || proof.Checkpoint == nil {
				t.Fatalf("failure hook did not return its cleanup receipt: %v", err)
			}
			record, err := f.journal.Script(t.Context(), f.input.Input.Attempt.ID)
			if err != nil || record == nil || record.Ack == nil ||
				record.Report.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
				t.Fatalf("hook proof lacks durable cleanup acknowledgement: %v", err)
			}
			if scenario == "no serving release" || scenario == "wrong serving release" {
				if !slices.Equal(f.engine.events, []string{"cleanup"}) ||
					f.delivery.Checkpoint.Outcome.Reason != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE {
					t.Fatal("unselected serving Release authorized a hook command")
				}
			} else {
				if !slices.Equal(f.engine.events, []string{"prepare", "recover", "create", "run", "cleanup"}) {
					t.Fatalf("hook effects: %v", f.engine.events)
				}
				outcome := f.delivery.Checkpoint.Outcome
				if scenario == "runtime failure" {
					if outcome.Reason != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE {
						t.Fatal("hook runtime failure was lost")
					}
				} else if outcome.GetExitCode() != f.engine.exitCode {
					t.Fatal("hook exit outcome was lost")
				}
			}
		})
	}
}

// TASK-80: losing the final Script ACK and reopening the Agent journal must
// resend that exact report, not run a failed hook again or repeat its cleanup.
func TestFailureHookResumesLostCleanupAcknowledgementWithoutExecution(t *testing.T) {
	f := newFailureHookFixture(t, true)
	f.engine.exitCode, f.loseCleanupAck = 29, true
	proof, err := f.hooks(t).RunRecovery(t.Context(), f.input, f.report)
	if !errors.Is(err, errHookCleanupAckLost) || proof.Checkpoint != nil {
		t.Fatalf("lost cleanup ACK: %v", err)
	}
	before := slices.Clone(f.engine.events)
	if err := f.journal.Close(); err != nil {
		t.Fatal(err)
	}
	f.journal, err = agentworkjournal.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	proof, err = f.hooks(t).RunRecovery(t.Context(), f.input, f.report)
	if err != nil || proof.Validate(f.input, f.report) != nil || !slices.Equal(before, f.engine.events) ||
		f.cleanupSends != 2 {
		t.Fatalf("journal resume repeated hook effects or lost receipt: %v", err)
	}
}

// TASK-80: the actual recovery loop stops on failed hook cleanup. Explicit
// resumption completes cleanup, not a second command, even for a failed hook.
func TestFailureHookRecoveryResumesCleanupWithoutRetryingCommand(t *testing.T) {
	f := newFailureHookFixture(t, true)
	f.engine.exitCode = 29
	f.engine.cleanupErr = errors.New("cleanup refused")
	evidence, err := checkpointdelivery.NewRecoveryEffect(
		f.input,
		f.journal,
		func(_ context.Context, _ *agentpb.WorkAttemptIdentity, raw []byte, reference workexecution.Input) (workexecution.Input, error) {
			if _, err := releaseexecution.DecodeRecoveryEffect(f.input, f.report, raw, reference); err != nil {
				return workexecution.Input{}, err
			}
			return reference, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	effects := &hookOnlyRecoveryEffects{hooks: f.hooks(t), evidence: evidence}
	report, err := RunRecovery(t.Context(), f.input, f.checkpoints, effects)
	if !errors.Is(err, f.engine.cleanupErr) || report == nil ||
		report.Progress.Phase != releaseexecution.FailureHooks ||
		effects.calls != 1 {
		t.Fatalf("failed cleanup was retried or recovery continued: %v", err)
	}
	f.engine.cleanupErr = nil
	report, err = RunRecovery(t.Context(), f.input, f.checkpoints, effects)
	if err != nil || report == nil || report.Progress.Phase != releaseexecution.RecoveryComplete ||
		f.input.Forward.Outcome != workexecution.Failed || f.delivery.Checkpoint.Outcome.GetExitCode() != 29 ||
		!slices.Equal(f.engine.events, []string{"prepare", "recover", "create", "run", "cleanup", "cleanup"}) {
		t.Fatalf("explicit recovery resume did not preserve failed hook/forward outcome: %v", err)
	}
	if _, err := RunRecovery(t.Context(), f.input, f.checkpoints, effects); err != nil || effects.calls != 2 {
		t.Fatalf("completed recovery replay repeated effects: %v", err)
	}
}

// The earlier host phases are outside this case. Only the hook branch is
// supplied, through the actual Script runtime and recovery-evidence journal.
type hookOnlyRecoveryEffects struct {
	hooks    *Hooks
	evidence *checkpointdelivery.RecoveryEffect
	calls    int
}

func (effects *hookOnlyRecoveryEffects) ExecuteRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (workexecution.Input, error) {
	return effects.InspectRecovery(ctx, input, report)
}

func (effects *hookOnlyRecoveryEffects) InspectRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (workexecution.Input, error) {
	effects.calls++
	proof, err := effects.hooks.RunRecovery(ctx, input, report)
	if err != nil {
		return workexecution.Input{}, err
	}
	return effects.evidence.Record(ctx, report, releaseexecution.RecoveryEffectResult{Hook: &proof})
}

// TASK-80: an interrupted hook can carry cleanup proof only. Mixed delivery
// must fail rather than treating supplied plaintext as permission to execute.
func TestInterruptedFailureHookConsumesOnlyCleanupEvidence(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup only", true: "mixed executable delivery"}[mixed], func(t *testing.T) {
			f := newFailureHookFixture(t, true)
			body := proto.CloneOf(f.delivery.Body)
			f.delivery.Cleanup = &agentpb.ScriptCleanupArtifacts{BodyMetadata: proto.CloneOf(body.Metadata)}
			f.delivery.Body, f.delivery.Snapshot, f.delivery.Checkpoint = nil, nil, nil
			f.delivery.InterruptedCleanup = &agentpb.WorkInputReference{
				InputId: ids.New(ids.KindPlan),
				Sha256:  bytes.Repeat([]byte{7}, 32),
			}
			if mixed {
				f.delivery.Body = body
			}
			proof, err := f.hooks(t).RunRecovery(t.Context(), f.input, f.report)
			if mixed {
				if err == nil {
					t.Fatal("mixed cleanup/execution delivery was accepted")
				}
			} else if err != nil || proof.InterruptedCleanup == nil || proof.Validate(f.input, f.report) != nil {
				t.Fatalf("cleanup-only hook did not settle its reference: %v", err)
			}
			if len(f.engine.events) != 0 {
				t.Fatal("interrupted hook executed again")
			}
		})
	}
}

var errHookCleanupAckLost = errors.New("script cleanup response lost")

type failureHookFixture struct {
	input          releaseexecution.RecoveryInput
	report         releaseexecution.RecoveryReport
	delivery       *agentpb.ReleaseHookDelivery
	journal        *agentworkjournal.Journal
	path           string
	engine         *failureHookEngine
	loseCleanupAck bool
	cleanupSends   int
	lostCleanup    *agentpb.ScriptCheckpointRequest
	checkpoints    *checkpointdelivery.Recovery
}

func newFailureHookFixture(t *testing.T, preserve bool) *failureHookFixture {
	t.Helper()
	f := &failureHookFixture{input: failureHookInput(t, preserve), path: filepath.Join(t.TempDir(), "journal")}
	var err error
	f.journal, err = agentworkjournal.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.journal.Close(); err != nil {
			t.Error(err)
		}
	})
	checkpoints, err := checkpointdelivery.NewRecovery(
		f.input,
		f.journal,
		func(_ context.Context, request *agentpb.ReleaseRecoveryCheckpointRequest) (*agentpb.ReleaseRecoveryCheckpointAck, error) {
			report, err := releaseexecution.DecodeRecoveryRequest(f.input, request)
			if err != nil {
				return nil, err
			}
			return releaseexecution.EncodeRecoveryAck(f.input, report)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	f.checkpoints = checkpoints
	progress, err := releaseexecution.NewRecoveryProgress(f.input, *f.input.Input.Attempt.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	// Earlier runtime/configuration effects are supplied, not executed. Use the
	// actual recovery journal so Script checkpoints share its global sequence.
	for index := 0; index <= 6; index++ {
		progress, err = progress.Begin(f.input, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := checkpoints.Checkpoint(t.Context(), progress); err != nil {
			t.Fatal(err)
		}
		if index == 6 {
			break
		}
		boundary, err := checkpoints.Current(t.Context())
		if err != nil || boundary == nil {
			t.Fatalf("supplied recovery boundary %d: %v", index, err)
		}
		effect, err := f.journal.PrepareRecoveryEffect(t.Context(), f.input, *boundary,
			suppliedHookPrecondition(t, f.input, *boundary), time.Now().UTC())
		if err != nil {
			t.Fatalf("supplied recovery effect %d: %v", index, err)
		}
		if err := f.journal.AcknowledgeRecoveryEffect(t.Context(), f.input, *boundary, effect.Reference); err != nil {
			t.Fatal(err)
		}
		progress, err = progress.Confirm(f.input, effect.Reference, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if err := checkpoints.Checkpoint(t.Context(), progress); err != nil {
			t.Fatal(err)
		}
	}
	report, err := checkpoints.Current(t.Context())
	if err != nil || report == nil {
		t.Fatalf("failure-hook boundary: %v", err)
	}
	f.report = *report
	hook, err := report.Progress.ActiveHook(f.input)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := workexecution.EncodeAgentAttempt(f.input.Input.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("echo captured-failure-hook\n")
	digest := sha256.Sum256(body)
	snapshotID := ids.NewULID()
	f.delivery = &agentpb.ReleaseHookDelivery{
		Attempt:     attempt,
		OperationId: f.input.Input.Release.OperationId,
		Input:       proto.CloneOf(hook.Input),
		Run: &agentpb.RunScript{ScriptExecutionId: hook.ExecutionId, ScriptId: hook.ScriptId, ScriptGeneration: 1,
			RunnerSnapshotId: snapshotID, EnvironmentId: f.input.Input.Release.EnvironmentId, ServiceId: f.input.Input.Release.ServiceId,
			ReleaseId: f.input.Input.Release.ReleaseId},
		Snapshot: &agentpb.ResolvedRunnerSnapshot{
			SnapshotId:   snapshotID,
			LocalImageId: "sha256:" + strings.Repeat("b", 64),
		},
		Projection: &agentpb.ScriptRunnerProjection{SnapshotId: snapshotID, Image: "sha256:" + strings.Repeat("b", 64)},
		Body: &agentpb.ScriptBodyArtifact{
			Metadata: &agentpb.ScriptBodyArtifactMetadata{ScriptExecutionId: hook.ExecutionId,
				ScriptId: hook.ScriptId, Generation: 1, Size: uint32(len(body)), Sha256: digest[:]},
			Body: body,
		},
		Checkpoint: &agentpb.ScriptExecutionCheckpoint{
			ScriptExecutionId: hook.ExecutionId,
			State:             agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
		},
	}
	f.engine = &failureHookEngine{bodyDigest: digest, before: func(state agentpb.ScriptExecutionState) {
		record, err := f.journal.Script(t.Context(), f.input.Input.Attempt.ID)
		if err != nil || record == nil || record.Ack == nil || record.Report.State != state {
			t.Fatalf("hook effect ran before its durable acknowledged boundary %s: %v", state, err)
		}
	}}
	return f
}

// This fixture supplies only the earlier host observations required to reach
// a failure-hook boundary. Their host effects are outside this case's claim.
func suppliedHookPrecondition(
	t *testing.T,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) releaseexecution.RecoveryEffectResult {
	t.Helper()
	at := time.Now().UTC()
	result := releaseexecution.RecoveryEffectResult{}
	switch report.Progress.Phase {
	case releaseexecution.QuiesceForward:
		proof := &releaseexecution.QuiescenceProof{ForwardAttemptID: input.Forward.ID,
			RecoveryAttemptID: input.Input.Attempt.ID, Executor: input.Input.Attempt.Executor, HelperAbsentAt: at, OwnerCheckedAt: at}
		for _, hook := range input.Input.Release.Hooks {
			proof.Hooks = append(proof.Hooks, releaseexecution.HookQuiescenceProof{ExecutionID: hook.ExecutionId,
				Input: workexecution.Input{
					ID:     hook.Input.InputId,
					SHA256: hex.EncodeToString(hook.Input.Sha256),
				}, Absent: true,
				ObservedAt: at, Cleanup: scriptexecution.CleanupProof{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}})
		}
		result.Quiescence = proof
	case releaseexecution.RestoreConfiguration, releaseexecution.VerifyRestoration:
		proof, err := releaseexecution.RecoveryConfigurationResult(input, report, nil)
		if err != nil {
			t.Fatal(err)
		}
		result.Configuration = &proof
	}
	switch report.Progress.Phase {
	case releaseexecution.InspectFailedRuntime,
		releaseexecution.RestoreServingRuntime,
		releaseexecution.RemoveFailedCandidate,
		releaseexecution.VerifyRestoration:
		proof := &releaseexecution.RecoveryRuntimeProof{AttemptID: input.Input.Attempt.ID, StageID: report.StageID,
			Phase: report.Progress.Phase, Inspection: true, ObservedAt: at}
		if report.Progress.Phase == releaseexecution.InspectFailedRuntime {
			proof.Inventory = &releaseexecution.RecoveryInventory{ObservedAt: at}
		}
		result.Runtime = proof
	}
	return result
}

func (f *failureHookFixture) hooks(t *testing.T) *Hooks {
	t.Helper()
	runtime, err := scriptruntime.New(f.engine)
	if err != nil {
		t.Fatal(err)
	}
	loader, err := NewHookLoader(
		func(context.Context, *agentpb.ReleaseHookRequest) (*agentpb.ReleaseHookDelivery, error) {
			return proto.CloneOf(f.delivery), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	hooks, err := NewHooks(
		runtime,
		f.journal,
		loader,
		func(_ context.Context, request *agentpb.ScriptCheckpointRequest) (*agentpb.ScriptCheckpointAck, error) {
			checkpoint := f.delivery.Checkpoint
			checkpoint.State = request.State
			switch request.State {
			case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED:
				checkpoint.StartAuthorized = true
			case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED:
				checkpoint.BodyPrepared = proto.CloneOf(request.GetBodyPrepared())
			case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED:
				checkpoint.ContainerCreated = proto.CloneOf(request.GetContainerCreated())
			case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED:
				checkpoint.Outcome = proto.CloneOf(request.GetOutcome())
			case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
				checkpoint.Cleanup = proto.CloneOf(request.GetCleanup())
			}
			if request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
				f.cleanupSends++
				if f.loseCleanupAck {
					f.loseCleanupAck = false
					f.lostCleanup = proto.CloneOf(request)
					return nil, errHookCleanupAckLost
				}
				if f.lostCleanup != nil && !proto.Equal(f.lostCleanup, request) {
					t.Fatal("reconnect reconstructed a different cleanup report")
				}
			}
			digest, err := executionplan.ComputeScriptCheckpointPayloadDigest(request)
			if err != nil {
				return nil, err
			}
			return &agentpb.ScriptCheckpointAck{
				TaskId:            request.TaskId,
				WorkItemId:        request.WorkItemId,
				AttemptId:         request.AttemptId,
				OperationId:       request.OperationId,
				ScriptExecutionId: request.ScriptExecutionId,
				Sequence:          request.Sequence,
				StageId:           request.StageId,
				State:             request.State,
				RequestSha256:     digest,
				EvidenceId:        ids.New(ids.KindPlan),
				EvidenceSha256:    bytes.Repeat([]byte{9}, 32),
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return hooks
}

type failureHookEngine struct {
	events             []string
	bodyDigest         [32]byte
	exitCode           int32
	runErr, cleanupErr error
	before             func(agentpb.ScriptExecutionState)
}

func (e *failureHookEngine) PrepareBody(
	context.Context,
	scriptexecution.Request,
) (scriptexecution.BodyEvidence, error) {
	e.events = append(e.events, "prepare")
	e.before(agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED)
	return scriptexecution.BodyEvidence{
		SHA256: e.bodyDigest[:],
		UID:    1000,
		GID:    1000,
		Device: 1,
		Inode:  2,
		Leaf:   "body",
	}, nil
}

func (e *failureHookEngine) RecoverContainer(
	context.Context,
	scriptexecution.Request,
	scriptexecution.BodyEvidence,
) (scriptexecution.ContainerRecovery, error) {
	e.events = append(e.events, "recover")
	e.before(agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED)
	return scriptexecution.ContainerRecovery{}, nil
}

func (e *failureHookEngine) CreateContainer(
	context.Context,
	scriptexecution.Request,
	scriptexecution.BodyEvidence,
) (scriptexecution.ContainerEvidence, error) {
	e.events = append(e.events, "create")
	e.before(agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED)
	return scriptexecution.ContainerEvidence{
		ID:                    strings.Repeat("a", 64),
		OwnershipLabelsSHA256: bytes.Repeat([]byte{3}, 32),
	}, nil
}

func (e *failureHookEngine) RunContainer(
	context.Context,
	scriptexecution.Request,
	scriptexecution.BodyEvidence,
	scriptexecution.ContainerEvidence,
) (scriptexecution.RunResult, error) {
	e.events = append(e.events, "run")
	e.before(agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED)
	return scriptexecution.RunResult{ExitCode: e.exitCode}, e.runErr
}

func (e *failureHookEngine) Cleanup(
	_ scriptexecution.CleanupRequest,
	body *scriptexecution.BodyEvidence,
	container *scriptexecution.ContainerEvidence,
) (scriptexecution.CleanupProof, error) {
	e.events = append(e.events, "cleanup")
	e.before(agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED)
	proof := scriptexecution.CleanupProof{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}
	if body != nil {
		proof.BodyDevice, proof.BodyInode, proof.BodyLeaf = body.Device, body.Inode, body.Leaf
	}
	if container != nil {
		proof.ContainerID = container.ID
	}
	return proof, e.cleanupErr
}

func (e *failureHookEngine) Quiesce(
	context.Context,
	scriptexecution.QuiescenceRequest,
) (scriptexecution.QuiescenceProof, error) {
	return scriptexecution.QuiescenceProof{}, errors.New("unexpected hook quiescence")
}
func (e *failureHookEngine) Close() error { return nil }

func failureHookInput(t *testing.T, preserve bool) releaseexecution.RecoveryInput {
	t.Helper()
	at := time.Now().UTC().Add(-time.Minute)
	forward := workexecution.Attempt{
		ID:         ids.New(ids.KindAttempt),
		TaskID:     ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem),
		Number:     1,
		Direction:  workexecution.Forward,
		AssignedAt: at,
		StartedAt:  &at,
		Deadline:   at.Add(time.Hour),
		Input:      workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		Executor: workexecution.Identity{
			Executor:   workexecution.Agent,
			HostID:     ids.New(ids.KindHost),
			AgentID:    ids.New(ids.KindAgent),
			Generation: 1,
		},
	}
	environmentID, serviceID, releaseID := ids.New(
		ids.KindEnvironment,
	), ids.New(
		ids.KindService,
	), ids.New(
		ids.KindDeployment,
	)
	labels := map[string]string{
		"com.groundplane.environment-id":    environmentID,
		"com.groundplane.service-id":        serviceID,
		"com.groundplane.plan-id":           forward.Input.ID,
		"com.groundplane.render-generation": "1",
		"com.groundplane.runtime-role":      "singleton",
		"com.groundplane.release-id":        releaseID,
		"com.groundplane.managed":           "true",
		"com.groundplane.kind":              "service",
	}
	image := "sha256:" + strings.Repeat("b", 64)
	raw, err := yaml.Marshal(
		map[string]any{
			"services": map[string]any{"app": map[string]any{"image": image, "network_mode": "none", "labels": labels}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	service := &agentpb.ComposeService{
		ComposeName:      "app",
		ServiceId:        serviceID,
		ImageReference:   image,
		ExpectedReplicas: 1,
		Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: labels[key]})
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.New(ids.KindConfig),
		OwnerId:    environmentID,
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		ProjectName: "gp-" + strings.ToLower(
			environmentID,
		),
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/" + environmentID,
		CanonicalYaml:       raw,
		YamlSha256:          digest[:],
		Services:            []*agentpb.ComposeService{service},
	}
	release := &agentpb.ReleaseWorkInput{
		OperationId:      ids.New(ids.KindOperation),
		EnvironmentId:    environmentID,
		ServiceId:        serviceID,
		ReleaseId:        releaseID,
		RenderGeneration: 1,
		Candidate:        artifact,
		Strategy:         agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
		FailurePolicy:    agentpb.WorkReleaseFailurePolicy_WORK_RELEASE_FAILURE_POLICY_LEAVE_ACTIVE,
		Predecessor: &agentpb.ReleaseWorkPredecessor{
			Absent: true,
		},
		Configuration: &agentpb.WorkConfigurationReference{
			ConfigurationId: ids.New(ids.KindConfig),
			EnvironmentId:   environmentID,
			Generation:      1,
			Sha256:          digest[:],
		},
		Hooks: []*agentpb.ReleaseWorkHook{
			{
				ExecutionId: ids.NewULID(),
				ScriptId:    ids.New(ids.KindScript),
				Phase:       "on-failure",
				Order:       1,
				Input:       &agentpb.WorkInputReference{InputId: ids.New(ids.KindPlan), Sha256: digest[:]},
			},
		},
	}
	failedAt := at.Add(time.Second)
	failure := releaseexecution.Failure{
		AttemptID:  forward.ID,
		Input:      forward.Input,
		Reason:     releaseexecution.ExecutionFailed,
		RecordedAt: failedAt,
	}
	_, reference, err := releaseexecution.EncodeFailure(forward, failure)
	if err != nil {
		t.Fatal(err)
	}
	forward.FinishedAt, forward.Result, forward.Outcome = &failedAt, &reference, workexecution.Failed
	attempt := forward
	started := failedAt.Add(time.Second)
	attempt.ID, attempt.Number, attempt.Direction, attempt.AssignedAt, attempt.StartedAt = ids.New(
		ids.KindAttempt,
	), 2, workexecution.Recovery, started, &started
	attempt.FinishedAt, attempt.Result, attempt.Outcome = nil, nil, ""
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(attempt, release)
	if err != nil {
		t.Fatal(err)
	}
	disposition := agentpb.WorkReleaseRecoveryDisposition_WORK_RELEASE_RECOVERY_DISPOSITION_RESTORE_CAPTURED
	if preserve {
		disposition = agentpb.WorkReleaseRecoveryDisposition_WORK_RELEASE_RECOVERY_DISPOSITION_PRESERVE_SERVING
	}
	input := releaseexecution.RecoveryInput{
		Input:       releaseexecution.Input{Attempt: attempt, Release: release, Artifacts: artifacts},
		Forward:     forward,
		Failure:     failure,
		Disposition: disposition,
	}
	if err := input.Validate(); err != nil {
		t.Fatal(err)
	}
	return input
}
