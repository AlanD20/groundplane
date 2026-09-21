package etcd_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func completeEntryRuntimePublication(
	t *testing.T, fixture *etcd.ExecutedArtifactFixture, task etcd.TaskRecord, race string,
) {
	t.Helper()
	if task.EntryRuntime == nil || len(task.EntryRuntime.Updates) == 0 {
		claim, found, err := fixture.Tasks.ClaimNextTask(
			t.Context(), ids.New(ids.KindAgent), 1, task.CreatedAt.Add(time.Second),
		)
		if err != nil || !found {
			t.Fatalf("claim materialization-only Entry = %t, %v", found, err)
		}
		if _, err := fixture.Tasks.AcknowledgeTask(t.Context(), claim.Assignment.Record.AgentID, 1, task.ID,
			claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
				Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, task.CreatedAt.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		return
	}
	serviceID := task.EntryRuntime.Updates[0].ServiceID
	previous := fixture.AcknowledgedRuntime(t, serviceID)
	if race == "before claim" {
		fixture.AdvanceAcknowledgedRuntime(t, serviceID)
		if _, found, err := fixture.Tasks.ClaimNextTask(
			t.Context(), ids.New(ids.KindAgent), 1, task.CreatedAt.Add(time.Second),
		); found || !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("claim accepted changed Entry runtime: found=%t err=%v", found, err)
		}
		return
	}
	agentID := ids.New(ids.KindAgent)
	claim, found, err := fixture.Tasks.ClaimNextTask(t.Context(), agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("claim Entry runtime = %t, %v", found, err)
	}
	if race == "before acknowledgement" {
		fixture.AdvanceAcknowledgedRuntime(t, serviceID)
		if _, err := fixture.Tasks.AcknowledgeTask(t.Context(), agentID, 1, task.ID,
			claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
				Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, task.CreatedAt.Add(2*time.Second)); !errors.Is(
			err, errs.New(errs.KindStateConflict, ""),
		) {
			t.Fatalf("terminal accepted changed Entry runtime: %v", err)
		}
		return
	}
	terminal, err := fixture.Tasks.AcknowledgeTask(
		t.Context(),
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
			Diagnostic: testtaskjournal.TaskResultDiagnosticNone},
		task.CreatedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	acknowledged := fixture.AcknowledgedRuntime(t, serviceID)
	if terminal.Revision != acknowledged.Revision || acknowledged.Record.Source.TaskID != task.ID ||
		acknowledged.Record.Source.AssignmentID != claim.Assignment.Record.AssignmentID ||
		acknowledged.Record.Source.ExecutionEpoch != 1 || acknowledged.Record.Runtime.ReleaseID != previous.Record.Runtime.ReleaseID ||
		acknowledged.Record.Runtime.Target != previous.Record.Runtime.Target {
		t.Fatal("Entry terminal outcome and runtime receipt are not one exact acknowledgement")
	}
	beforeCurrent, afterCurrent := new(agentpb.ComposeArtifact), new(agentpb.ComposeArtifact)
	if proto.Unmarshal(previous.Record.Runtime.CurrentArtifact, beforeCurrent) != nil ||
		proto.Unmarshal(acknowledged.Record.Runtime.CurrentArtifact, afterCurrent) != nil {
		t.Fatal("open current Entry receipt")
	}
	expectedDestination := "secrets/.env." + task.Target + ".api"
	if bytes.Contains(beforeCurrent.CanonicalYaml, []byte(expectedDestination)) ||
		!bytes.Contains(afterCurrent.CanonicalYaml, []byte(expectedDestination)) ||
		!proto.Equal(entryReceiptProxy(beforeCurrent), entryReceiptProxy(afterCurrent)) {
		t.Fatal("Entry receipt changed its stable proxy or omitted the selected current workload environment")
	}
	if len(previous.Record.Runtime.RetainedPriorArtifact) == 0 ||
		len(acknowledged.Record.Runtime.RetainedPriorArtifact) == 0 {
		t.Fatal("Entry receipt lost the acknowledged retained slot")
	}
	retained := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(acknowledged.Record.Runtime.RetainedPriorArtifact, retained) != nil ||
		!bytes.Contains(retained.CanonicalYaml, []byte(expectedDestination)) ||
		entryReceiptProxy(retained) != nil {
		t.Fatal("Entry receipt changed or omitted the selected retained workload")
	}
}

func entryReceiptProxy(artifact *agentpb.ComposeArtifact) *agentpb.ComposeService {
	for _, service := range artifact.GetServices() {
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			return service
		}
	}
	return nil
}
