package agent

import (
	"bytes"
	"context"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-115: a refusal wakes only its exact caller, retains the unacknowledged
// durable report across reopen, and permits an unrelated terminal exchange.
// Real exchange/disk journal, supplied reports/Controller replies; no Script
// execution or Controller storage settlement is simulated as successful work.
func TestNativeReportRejectionRetainsEvidenceAndOtherExchanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "journal")
	journal, err := agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	exchange := newNativeExchange(ctx, journal)
	defer exchange.close()
	exchange.identity = workexecution.Identity{Executor: workexecution.Agent,
		HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1}
	first := nativeReportRejectionFixture(t, journal, exchange.identity)
	second := nativeReportRejectionFixture(t, journal, exchange.identity)
	firstResult, secondResult := make(chan nativeResponse, 1), make(chan nativeResponse, 1)
	go func() {
		message, err := exchange.request(
			ctx,
			first,
			first.GetScriptWorkCompletionRequest().Attempt,
			"script_completion",
			"",
		)
		firstResult <- nativeResponse{message: message, err: err}
	}()
	nativeReportSent(t, ctx, exchange, first)
	go func() {
		message, err := exchange.request(
			ctx,
			second,
			second.GetScriptWorkCompletionRequest().Attempt,
			"script_completion",
			"",
		)
		secondResult <- nativeResponse{message: message, err: err}
	}()
	nativeReportSent(t, ctx, exchange, second)
	rejection, err := workreceipt.Reject(first, "state.conflict", "captured operation state changed")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"digest", "identity", "kind"} {
		forged := proto.CloneOf(rejection)
		switch field {
		case "digest":
			forged.GetWorkReportRejection().ReportSha256[0] ^= 1
		case "identity":
			forged.GetWorkReportRejection().Attempt.Input.Sha256[0] ^= 1
		case "kind":
			forged.GetWorkReportRejection().Kind = "script_failure"
		}
		if handled, err := exchange.accept(forged); !handled || err == nil {
			t.Fatalf("forged %s refusal changed a pending report: handled=%v err=%v", field, handled, err)
		}
	}
	if handled, err := exchange.accept(rejection); !handled || err != nil {
		t.Fatalf("exact refusal closed the exchange: handled=%v err=%v", handled, err)
	}
	select {
	case result := <-firstResult:
		if result.err == nil || result.message != nil {
			t.Fatal("rejected report was returned as successful settlement")
		}
	case <-ctx.Done():
		t.Fatal("refusal did not wake its own caller")
	}
	secondAck := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkCompletionAck{
		ScriptWorkCompletionAck: &agentpb.ScriptWorkCompletionAck{
			Attempt: proto.CloneOf(second.GetScriptWorkCompletionRequest().Attempt),
		},
	}}
	if handled, err := exchange.accept(secondAck); !handled || err != nil {
		t.Fatalf("unrelated result could not settle transport: handled=%v err=%v", handled, err)
	}
	select {
	case result := <-secondResult:
		if result.err != nil || !proto.Equal(result.message, secondAck) {
			t.Fatalf("unrelated caller received incorrect settlement: %v", result.err)
		}
	case <-ctx.Done():
		t.Fatal("unrelated result was blocked")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	page, err := journal.Inventory(ctx, "")
	if err != nil || len(page.Entries) != 2 {
		t.Fatalf("lost retained report inventory: entries=%d err=%v", len(page.Entries), err)
	}
	for _, entry := range page.Entries {
		if entry.Delivery == nil {
			t.Fatal("terminal result lost its durable request")
		}
		message, err := entry.Delivery.Message()
		if err != nil {
			t.Fatal(err)
		}
		if entry.Attempt.ID == first.GetScriptWorkCompletionRequest().Attempt.AttemptId {
			if !proto.Equal(message, first) || len(entry.Delivery.Reply) != 0 {
				t.Fatal("rejection acknowledged or changed unresolved report")
			}
		} else if !proto.Equal(message, second) || len(entry.Delivery.Reply) == 0 {
			t.Fatal("unrelated acknowledgement was not retained independently")
		}
	}
}

func nativeReportSent(t *testing.T, ctx context.Context, exchange *nativeExchange, expected *agentpb.AgentMessage) {
	t.Helper()
	select {
	case actual := <-exchange.outgoing:
		if !proto.Equal(actual, expected) {
			t.Fatal("sent result differs from captured report")
		}
	case <-ctx.Done():
		t.Fatal("report was not sent")
	}
}

func nativeReportRejectionFixture(t *testing.T, journal *agentworkjournal.Journal,
	identity workexecution.Identity,
) *agentpb.AgentMessage {
	t.Helper()
	at := time.Now().UTC()
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem), Number: 1, Executor: identity, Direction: workexecution.Forward,
		Input:      workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute)}
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = journal.PrepareScript(t.Context(), attempt, &agentpb.ScriptCheckpointRequest{
		TaskId: attempt.TaskID, WorkItemId: attempt.WorkItemID, AttemptId: attempt.ID,
		OperationId: ids.New(ids.KindOperation), ScriptExecutionId: ids.NewULID(),
		InputId: attempt.Input.ID, InputSha256: bytes.Repeat([]byte{0xaa}, 32), StageId: ids.New(ids.KindStep),
		ExpectedState: agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
		State:         agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
		Evidence: &agentpb.ScriptCheckpointRequest_StartAuthorized{
			StartAuthorized: &agentpb.ScriptStartAuthorizedCheckpoint{},
		},
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hex.DecodeString(attempt.Input.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{
		ScriptWorkCompletionRequest: &agentpb.ScriptWorkCompletionRequest{Attempt: wire, CheckpointSequence: 1,
			CheckpointEvidence: &agentpb.WorkInputReference{InputId: attempt.Input.ID, Sha256: digest}},
	}}
}
