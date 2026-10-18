package scriptruntime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// QA: TASK-25. Real journal/delivery persistence is covered separately by TASK-22.
// Rationale: lost control acknowledgements after setup effects must resume only
// cleanup, or the durable result, never create or start another setup container.
func TestExplicitScriptLostAcknowledgementResumesWithoutDuplicateStart(t *testing.T) {
	for _, state := range []agentpb.ScriptExecutionState{
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
		agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN,
	} {
		for _, outcome := range []struct {
			name   string
			runErr error
			reason agentpb.ScriptOutcomeReason
		}{
			{name: "success", reason: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT},
			{name: "abort", runErr: context.Canceled, reason: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT},
			{name: "uncertain runtime", runErr: errs.New(errs.KindStateConflict, "container outcome is unproven"),
				reason: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE},
		} {
			t.Run(state.String()+"/"+outcome.name, func(t *testing.T) {
				invocation, digest := capturedContainerFailureFixture()
				setExplicitScriptFixture(t, &invocation)
				durable := invocation.Checkpoint
				lostAck := errs.New(errs.KindRequestFailed, "checkpoint acknowledgement lost")
				checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
					persistScriptCheckpointForTest(t, durable, request)
					if request.State == state {
						return lostAck
					}
					return nil
				}
				events := []string{}
				engine := &capturedProjectionScriptEngine{checkpointOrderScriptEngine: &checkpointOrderScriptEngine{
					events: &events, bodyDigest: digest, runErr: outcome.runErr,
				}}
				runtime, err := New(engine)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.ExecuteScript(context.Background(), invocation, checkpoint); !errors.Is(
					err,
					lostAck,
				) {
					t.Fatalf("first execution error = %v, want lost acknowledgement", err)
				}
				assertExplicitScriptCapture(t, invocation, engine)
				if durable.State != state || durable.GetOutcome().GetReason() != outcome.reason {
					t.Fatalf("retained state/outcome = %s/%s", durable.State, durable.GetOutcome().GetReason())
				}
				if len(events) < 4 || !slices.Equal(events[:4], []string{
					"engine:prepare", "engine:recover", "engine:create", "engine:run",
				}) {
					t.Fatalf("initial setup events = %v", events)
				}
				resumedEvents := []string{}
				resumed, err := New(
					&checkpointOrderScriptEngine{events: &resumedEvents, bodyDigest: digest},
				)
				if err != nil {
					t.Fatal(err)
				}
				acknowledge := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
					persistScriptCheckpointForTest(t, durable, request)
					return nil
				}
				_, resumeErr := resumed.ExecuteScript(context.Background(), invocation, acknowledge)
				if (resumeErr != nil) != (outcome.runErr != nil) {
					t.Fatalf("resumed result = %v, want original outcome %s", resumeErr, outcome.reason)
				}
				want := []string{}
				if state == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED {
					want = append(want, "engine:cleanup")
				}
				if !slices.Equal(resumedEvents, want) ||
					durable.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
					t.Fatalf("resume effects/state = %v/%s, want %v/cleanup proven", resumedEvents, durable.State, want)
				}
				if durable.ReconciliationRequired != (outcome.name == "uncertain runtime") {
					t.Fatal("resume changed the recovery-invariant fence")
				}
			})
		}
	}
}

func persistScriptCheckpointForTest(
	t *testing.T,
	durable *agentpb.ScriptExecutionCheckpoint,
	request *agentpb.ScriptCheckpointRequest,
) {
	t.Helper()
	// Runtime callbacks receive an unsequenced draft. The journal/delivery owner
	// adds the attempt-wide sequence before validating the complete wire report.
	if _, err := executionplan.ComputeScriptCheckpointPayloadDigest(request); err != nil {
		t.Fatal(err)
	}
	if request.ExpectedState != durable.State {
		t.Fatal("checkpoint did not extend the retained state")
	}
	durable.State = request.State
	switch request.State {
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED:
		durable.StartAuthorized = true
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED:
		durable.BodyPrepared = proto.Clone(request.GetBodyPrepared()).(*agentpb.ScriptBodyPreparedCheckpoint)
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED:
		durable.ContainerCreated = proto.Clone(request.GetContainerCreated()).(*agentpb.ScriptContainerCreatedCheckpoint)
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED:
		durable.Outcome = proto.Clone(request.GetOutcome()).(*agentpb.ScriptOutcomeCheckpoint)
		durable.ReconciliationRequired = durable.Outcome.Reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	case agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN:
		durable.Cleanup = proto.Clone(request.GetCleanup()).(*agentpb.ScriptCleanupCheckpoint)
	}
	if _, err := executionplan.ValidateScriptExecutionCheckpoint(durable); err != nil {
		t.Fatal(err)
	}
}

// This received native invocation exercises execution, not source publication.
func setExplicitScriptFixture(t *testing.T, invocation *Invocation) {
	t.Helper()
	snapshot, projection := invocation.Snapshot, invocation.Projection
	context := &agentpb.ScriptExplicitExecutionContext{
		ImageReference: "example.test/setup@sha256:" + strings.Repeat("c", 64), User: "1000:1000",
		Volumes: []*agentpb.ScriptExplicitVolumeGrant{{
			VolumeId: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Target: "/etc/tls", ReadOnly: false,
		}},
		EntryIds: []string{"ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}
	digest, err := executionplan.ScriptExecutionContextDigest(context)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ExplicitExecution = &agentpb.ScriptExplicitExecutionAuthority{
		Context: context, ContextSha256: digest, ScriptModRevision: 10,
		ReleaseLocalImageId: "sha256:" + strings.Repeat("b", 64),
	}
	snapshot.LocalImageId = "sha256:" + strings.Repeat("c", 64)
	projection.Image, projection.WorkingDir, projection.Uid, projection.Gid = snapshot.LocalImageId, "/", 1000, 1000
	snapshot.Mounts = []*agentpb.ScriptRunnerMount{{
		SourceId: context.Volumes[0].VolumeId,
		RenderedMount: &agentpb.ScriptMount{
			Type: "volume", Source: "gp_vol_" + context.Volumes[0].VolumeId,
			Target: "/etc/tls", ReadOnly: false, VolumeNoCopy: true,
		},
	}}
	projection.Mounts = snapshot.Mounts
	snapshot.EntryBindings = []*agentpb.ScriptRunnerEntryBinding{{
		EntryId: context.EntryIds[0], ValueGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV, EnvironmentKey: "SETUP_VALUE",
	}}
	projection.EntryBindings = snapshot.EntryBindings
	invocation.Entries = []*agentpb.ScriptEntryArtifact{
		{Binding: snapshot.EntryBindings[0], Value: []byte("declared-fixture-value")},
		{Binding: &agentpb.ScriptRunnerEntryBinding{
			EntryId: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW", ValueGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV, EnvironmentKey: "AMBIENT_VALUE",
		}, Value: []byte("unselected-fixture-value")},
	}
}

type capturedProjectionScriptEngine struct {
	*checkpointOrderScriptEngine
	projection *agentpb.ScriptRunnerProjection
	entries    []*agentpb.ScriptEntryArtifact
}

func (engine *capturedProjectionScriptEngine) CreateContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
) (scriptexecution.ContainerEvidence, error) {
	engine.projection = proto.Clone(request.Projection).(*agentpb.ScriptRunnerProjection)
	engine.entries = cloneScriptEntries(request.Entries)
	return engine.checkpointOrderScriptEngine.CreateContainer(ctx, request, body)
}

func assertExplicitScriptCapture(
	t *testing.T,
	invocation Invocation,
	engine *capturedProjectionScriptEngine,
) {
	t.Helper()
	if !proto.Equal(engine.projection, invocation.Projection) {
		t.Fatal("container creation did not receive the exact explicit runner projection")
	}
	if engine.createdImage == invocation.Snapshot.ExplicitExecution.ReleaseLocalImageId {
		t.Fatal("setup container used the consumer Release image")
	}
	if len(engine.entries) != 1 ||
		!proto.Equal(engine.entries[0].Binding, invocation.Snapshot.EntryBindings[0]) ||
		string(engine.entries[0].Value) != "declared-fixture-value" {
		t.Fatal("container creation did not receive only its declared Entry generation")
	}
}
