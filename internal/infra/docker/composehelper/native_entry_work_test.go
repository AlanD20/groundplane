package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ENTRY-04: a workload stopped after capture must not be started by an Entry
// edit; unacknowledged execution must never reach Docker at all.
func TestNativeEntryHelperRefusesStoppedConsumerAndChangedAcknowledgement(t *testing.T) {
	request := nativeEntryConsumerRequest(t)
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, command runner.RunCmdOpts) (runner.Result, error) {
		if command.Name != DockerExecutable || !slices.Contains(command.Args, "label=com.docker.compose.service=app--blue") ||
			len(command.Args) < 2 || command.Args[0] != "container" ||
			command.Args[1] != "ls" {
			t.Fatalf("stopped Entry consumer triggered runtime changes or selected another workload: %v", command.Args)
		}
		return runner.Result{}, nil
	}
	response, err := ExecuteEntryConfigurationWork(t.Context(), fake, request)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || response != nil || len(fake.RecordedCalls()) != 1 {
		t.Fatalf("missing captured consumer did not stop before Compose: %v", err)
	}
	request.Acknowledgement.Sequence++
	response, err = ExecuteEntryConfigurationWork(t.Context(), fake, request)
	if err == nil || response != nil || len(fake.RecordedCalls()) != 1 {
		t.Fatal("changed acknowledgement reached host execution")
	}
}

func nativeEntryConsumerRequest(t *testing.T) *agentpb.EntryConfigurationWorkHelperRequest {
	t.Helper()
	baseline := nativeAttachConsumerFixture(t).Baseline
	candidate := proto.CloneOf(baseline)
	candidate.ArtifactId = ids.New(ids.KindConfig)
	digest := sha256.Sum256(nil)
	input := &agentpb.EntryConfigurationWorkInput{OperationId: ids.New(ids.KindOperation),
		EnvironmentId: baseline.OwnerId, RenderGeneration: 2, Baseline: baseline, Candidate: candidate,
		RunningConsumerIds: []string{baseline.Services[0].ServiceId},
		Configuration: &agentpb.WorkConfigurationReference{ConfigurationId: ids.New(ids.KindConfig),
			EnvironmentId: baseline.OwnerId, Generation: 2, Sha256: digest[:]},
		Files: []*agentpb.MaterializeFile{{MaterializationId: ids.New(ids.KindConfig), ArtifactId: candidate.ArtifactId,
			EnvironmentId: baseline.OwnerId, Destination: "settings.ini", Mode: 0o444,
			OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE, Sha256: digest[:]}},
	}
	_, raw, reference, err := executionplan.CaptureEntryConfigurationWorkInput(ids.New(ids.KindPlan), input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Second)
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem), Number: 1, Direction: workexecution.Forward,
		Input: reference, AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute),
		Executor: workexecution.Identity{Executor: workexecution.Agent,
			HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1}}
	progress, err := entryexecution.NewProgress(attempt, input, at)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []entryexecution.Stage{entryexecution.WriteConfiguration, entryexecution.VerifyConfiguration} {
		progress, err = progress.Begin(attempt, input, at)
		if err != nil {
			t.Fatal(err)
		}
		progress, err = progress.Confirm(attempt, input, entryexecution.Observation{EnvironmentID: baseline.OwnerId,
			Input: reference, Stage: stage, FilesWritten: stage == entryexecution.WriteConfiguration,
			FilesVerified: stage == entryexecution.VerifyConfiguration, At: at}, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	progress, err = progress.Begin(attempt, input, at)
	if err != nil || progress.Stage != entryexecution.ReconcileConsumers {
		t.Fatal("consumer stage was not reached after file verification", err)
	}
	checkpoint, err := progress.Checkpoint(attempt, input)
	if err != nil {
		t.Fatal(err)
	}
	report, err := entryexecution.EncodeProgress(attempt, input, progress)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.EntryConfigurationWorkHelperRequest{
		Assignment: &agentpb.EntryConfigurationWorkAssignment{Attempt: wire, Input: input},
		Start:      &agentpb.WorkStartAck{Attempt: proto.CloneOf(wire), StartedAt: timestamppb.New(at)},
		Report:     &agentpb.EntryConfigurationWorkCheckpointRequest{Attempt: proto.CloneOf(wire), Progress: report},
		Acknowledgement: &agentpb.EntryConfigurationWorkCheckpointAck{Attempt: proto.CloneOf(wire),
			Sequence: checkpoint.Sequence, StageId: checkpoint.StageID, RecordedAt: timestamppb.New(checkpoint.RecordedAt),
			Evidence: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: evidence}},
	}
}
