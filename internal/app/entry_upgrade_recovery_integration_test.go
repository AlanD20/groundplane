package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/entryruntime"
	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A replacement Agent may account for retained terminal receipts, but the old
// generation's journal cannot become a new execution grant. An explicit retry
// retains the failed forward identity while assigning recovery to the current
// generation.
func TestEntryUpgradeRetainsFailedReceiptsAcrossAgentGeneration(t *testing.T) {
	procedure, input := entryUpgradeProcedure(t)
	hostID, agentID := ids.New(ids.KindHost), ids.New(ids.KindAgent)
	oldIdentity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     hostID,
		AgentID:    agentID,
		Generation: 1,
	}
	currentIdentity := oldIdentity
	currentIdentity.Generation = 2
	assignedAt := time.Now().UTC().Add(-time.Minute)
	startedAt := assignedAt.Add(time.Second)
	forward := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), WorkItemID: ids.New(ids.KindWorkItem), TaskID: ids.New(ids.KindTask),
		Number: 1, Executor: oldIdentity, Direction: workexecution.Forward, Input: input,
		AssignedAt: assignedAt, StartedAt: &startedAt, Deadline: assignedAt.Add(time.Hour),
	}
	progress, err := entryexecution.NewProgress(forward, procedure, startedAt)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = progress.Begin(forward, procedure, startedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := progress.Checkpoint(forward, procedure)
	if err != nil {
		t.Fatal(err)
	}

	journalPath := filepath.Join(t.TempDir(), "work-journal")
	journal, err := agentworkjournal.Open(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PrepareEntryConfiguration(t.Context(), forward, procedure, progress); err != nil {
		t.Fatal(err)
	}
	if err := journal.AcknowledgeEntryConfiguration(t.Context(), forward, procedure, checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.PrepareEntryConfigurationFailure(
		t.Context(), forward, procedure, entryexecution.ExecutionInterrupted,
	); err != nil {
		t.Fatal(err)
	}

	forwardFinishedAt := progress.UpdatedAt.Add(time.Second)
	failure := entryexecution.Failure{
		AttemptID: forward.ID, Input: input, Reason: entryexecution.ExecutionInterrupted,
		Checkpoint: &checkpoint, RecordedAt: forwardFinishedAt,
	}
	failureRaw, failureEvidence, err := entryexecution.EncodeFailure(forward, procedure, failure)
	if err != nil {
		t.Fatal(err)
	}
	terminalForward := forward
	terminalForward.FinishedAt, terminalForward.Outcome, terminalForward.Result =
		&forwardFinishedAt, workexecution.Failed, &failureEvidence
	forwardWire, err := workexecution.EncodeAgentAttempt(forward)
	if err != nil {
		t.Fatal(err)
	}
	forwardRequest := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationWorkFailureRequest{
		EntryConfigurationWorkFailureRequest: &agentpb.EntryConfigurationWorkFailureRequest{
			Attempt: forwardWire, Reason: string(entryexecution.ExecutionInterrupted),
		},
	}}
	forwardReply := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EntryConfigurationWorkFailureAck{
		EntryConfigurationWorkFailureAck: &agentpb.EntryConfigurationWorkFailureAck{
			Attempt: proto.CloneOf(forwardWire), Evidence: wireEntryUpgradeInput(t, failureEvidence),
			Failure: failureRaw, Outcome: string(workexecution.Failed), StartedAt: timestamppb.New(startedAt),
			FinishedAt: timestamppb.New(forwardFinishedAt),
		},
	}}
	settleEntryUpgradeReceipt(t, journal, forwardRequest, forwardReply, forward.ID)

	recoveryAssignedAt := forwardFinishedAt.Add(time.Second)
	recoveryStartedAt := recoveryAssignedAt.Add(time.Second)
	recovery := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), WorkItemID: forward.WorkItemID, TaskID: forward.TaskID,
		Number: 2, Executor: oldIdentity, Direction: workexecution.Recovery, Input: input,
		AssignedAt: recoveryAssignedAt, StartedAt: &recoveryStartedAt, Deadline: recoveryAssignedAt.Add(time.Hour),
	}
	source := entryexecution.Recovery{Forward: terminalForward, Failure: failure, Evidence: failureEvidence}
	if _, err := journal.PrepareEntryRecovery(t.Context(), recovery, procedure, source, progress); err != nil {
		t.Fatal(err)
	}
	recoveryObservedAt := recoveryStartedAt.Add(time.Second)
	report := entryexecution.RecoveryReport{
		AttemptID: recovery.ID, ForwardID: forward.ID, Failure: failureEvidence,
		EnvironmentID: procedure.EnvironmentId, Input: input, EffectsStopped: true,
		ObservedAt: recoveryObservedAt,
	}
	if err := journal.SaveEntryRecoveryReport(t.Context(), recovery, procedure, report); err != nil {
		t.Fatal(err)
	}
	reportRaw, reportEvidence, err := entryexecution.EncodeRecoveryReport(recovery, source, procedure, progress, report)
	if err != nil {
		t.Fatal(err)
	}
	recoveryFinishedAt := recoveryObservedAt.Add(time.Second)
	recoveryWire, err := workexecution.EncodeAgentAttempt(recovery)
	if err != nil {
		t.Fatal(err)
	}
	recoveryRequest := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationWorkRecoveryReportRequest{
		EntryConfigurationWorkRecoveryReportRequest: &agentpb.EntryConfigurationWorkRecoveryReportRequest{
			Attempt: recoveryWire, Report: reportRaw,
		},
	}}
	recoveryReply := &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_EntryConfigurationWorkRecoveryReportAck{
			EntryConfigurationWorkRecoveryReportAck: &agentpb.EntryConfigurationWorkRecoveryReportAck{
				Attempt: proto.CloneOf(recoveryWire), Evidence: wireEntryUpgradeInput(t, reportEvidence),
				PredecessorRestored: false, Outcome: string(workexecution.Failed),
				StartedAt: timestamppb.New(recoveryStartedAt), FinishedAt: timestamppb.New(recoveryFinishedAt),
			},
		},
	}
	settleEntryUpgradeReceipt(t, journal, recoveryRequest, recoveryReply, recovery.ID)
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := agentworkjournal.Open(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	page, err := reopened.Inventory(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 2 || len(page.Uncommitted) != 0 || page.After != "" {
		t.Fatalf("unexpected retained Entry journal inventory: %+v", page)
	}
	retained := make(map[string]agentworkjournal.JournalEntry, len(page.Entries))
	for _, entry := range page.Entries {
		retained[entry.Attempt.ID] = entry
	}
	for _, expected := range []workexecution.Attempt{forward, recovery} {
		entry, ok := retained[expected.ID]
		if !ok || entry.Delivery == nil || len(entry.Delivery.Reply) == 0 ||
			!workexecution.SameAssignment(entry.Attempt, expected) {
			t.Fatalf("settled failed Entry receipt was not retained after reopen: %s", expected.ID)
		}
		identity, err := workexecution.EncodeAgentAttemptIdentity(entry.Attempt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := workexecution.DecodeAgentAttempt(identity, currentIdentity); err == nil {
			t.Fatalf("old generation journal became a current execution assignment: %s", expected.ID)
		}
	}

	retryAssignedAt := recoveryFinishedAt.Add(time.Second)
	retry := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), WorkItemID: forward.WorkItemID, TaskID: forward.TaskID,
		Number: 3, Executor: currentIdentity, Direction: workexecution.Recovery, Input: input,
		AssignedAt: retryAssignedAt, Deadline: retryAssignedAt.Add(time.Hour),
	}
	retryWire, err := workexecution.EncodeAgentAttempt(retry)
	if err != nil {
		t.Fatal(err)
	}
	sourceRaw, err := entryexecution.EncodeRecovery(source, procedure)
	if err != nil {
		t.Fatal(err)
	}
	boundaryRaw, err := entryexecution.EncodeProgress(forward, procedure, progress)
	if err != nil {
		t.Fatal(err)
	}
	assignment := &agentpb.EntryConfigurationWorkAssignment{
		Attempt: retryWire, Input: procedure, RecoverySource: sourceRaw, RecoveryBoundary: boundaryRaw,
	}
	if _, err := entryruntime.DecodeAssignment(assignment, currentIdentity); err != nil {
		t.Fatalf("current generation could not decode explicit Entry retry: %v", err)
	}
	decodedRetry, err := workexecution.DecodeAgentAttempt(retryWire, currentIdentity)
	if err != nil {
		t.Fatal(err)
	}
	decodedSource, err := entryexecution.DecodeRecovery(sourceRaw, procedure)
	if err != nil {
		t.Fatal(err)
	}
	if decodedRetry.StartedAt != nil || decodedRetry.Executor != currentIdentity ||
		decodedSource.Forward.Executor != oldIdentity || decodedSource.Forward.ID != forward.ID ||
		decodedRetry.Input != decodedSource.Forward.Input || decodedRetry.Executor.HostID != decodedSource.Forward.Executor.HostID {
		t.Fatal("explicit retry did not preserve the old forward and current recovery identities")
	}
}

func entryUpgradeProcedure(t *testing.T) (*agentpb.EntryConfigurationWorkInput, workexecution.Input) {
	t.Helper()
	content, yaml := []byte("retained configuration\n"), []byte("services: {}\n")
	digest, yamlDigest := sha256.Sum256(content), sha256.Sum256(yaml)
	environment := ids.New(ids.KindEnvironment)
	baseline := &agentpb.ComposeArtifact{
		ArtifactId: ids.New(ids.KindConfig), OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environment, ProjectName: "gp-" + strings.ToLower(environment), CanonicalYaml: yaml,
		YamlSha256: yamlDigest[:], AuthorizedVolumeDir: "/var/lib/groundplane/vol/" +
			ids.New(ids.KindTenant) + "/" + ids.New(ids.KindProject) + "/" + environment,
	}
	candidate := proto.CloneOf(baseline)
	candidate.ArtifactId = ids.New(ids.KindConfig)
	procedure := &agentpb.EntryConfigurationWorkInput{
		OperationId: ids.New(ids.KindOperation), EnvironmentId: environment, RenderGeneration: 2,
		Baseline: baseline, Candidate: candidate,
		Configuration: &agentpb.WorkConfigurationReference{
			ConfigurationId: ids.New(ids.KindConfig), EnvironmentId: environment, Generation: 2,
			Sha256: bytes.Repeat([]byte{0x32}, sha256.Size),
		},
		Files: []*agentpb.MaterializeFile{{
			ArtifactId: candidate.ArtifactId, MaterializationId: ids.New(ids.KindConfig), EnvironmentId: environment,
			Destination: "config/settings.ini", Uid: 1000, Gid: 1000, Mode: 0o600,
			Length: uint64(len(content)), Sha256: digest[:],
			OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_SECRET_FILE,
		}},
	}
	owned, raw, reference, err := executionplan.CaptureEntryConfigurationWorkInput(ids.New(ids.KindPlan), procedure)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	return owned, reference
}

func wireEntryUpgradeInput(t *testing.T, input workexecution.Input) *agentpb.WorkInputReference {
	t.Helper()
	digest, err := hex.DecodeString(input.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.WorkInputReference{InputId: input.ID, Sha256: digest}
}

func settleEntryUpgradeReceipt(
	t *testing.T,
	journal *agentworkjournal.Journal,
	request *agentpb.AgentMessage,
	reply *agentpb.ControllerMessage,
	attemptID string,
) {
	t.Helper()
	previous, err := journal.PrepareTerminalDelivery(t.Context(), request)
	if err != nil || previous != nil {
		t.Fatalf("prepare terminal receipt: previous=%v err=%v", previous, err)
	}
	if err := journal.AcknowledgeTerminalDelivery(t.Context(), request, reply); err != nil {
		t.Fatal(err)
	}
	if err := journal.SettleTerminalDelivery(t.Context(), attemptID); err != nil {
		t.Fatal(err)
	}
}
