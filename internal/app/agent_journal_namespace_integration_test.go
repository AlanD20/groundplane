//go:build linux

package app

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Real Script bodies may coexist while other attempts allocate durable reports.
// Workspace directories must neither poison journal allocation nor reconnect
// inventory. This uses the production state-root factory and real filesystem
// workspaces without requiring Docker or mutating the host state root.
func TestAgentJournalIsolatedFromConcurrentScriptWorkspaces(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	journal, err := agentworkjournal.OpenState(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if journal != nil {
			journal.Close()
		}
	}()
	at := time.Now().UTC()
	type workspaceResult struct {
		err error
	}
	results := make(chan workspaceResult, 4)
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			attempt := workexecution.Attempt{
				ID:         ids.New(ids.KindAttempt),
				TaskID:     ids.New(ids.KindTask),
				WorkItemID: ids.New(ids.KindWorkItem),
				Number:     1,
				Executor: workexecution.Identity{
					Executor:   workexecution.Agent,
					HostID:     ids.New(ids.KindHost),
					AgentID:    ids.New(ids.KindAgent),
					Generation: 1,
				},
				Direction:  workexecution.Forward,
				Input:      workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
				AssignedAt: at,
				StartedAt:  &at,
				Deadline:   at.Add(time.Minute),
			}
			executionID := ids.NewULID()
			// A running Script owns this sibling workspace; its private body
			// must not become a member of the execution journal inventory.
			workspace := filepath.Join(root, "work", attempt.WorkItemID, executionID)
			if err := os.MkdirAll(workspace, 0700); err != nil {
				results <- workspaceResult{err: err}
				return
			}
			if err := os.WriteFile(filepath.Join(workspace, "body"), []byte("sleep 15"), 0400); err != nil {
				results <- workspaceResult{err: err}
				return
			}
			request := &agentpb.ScriptCheckpointRequest{
				TaskId:     attempt.TaskID,
				WorkItemId: attempt.WorkItemID,
				AttemptId:  attempt.ID,
				OperationId: ids.New(
					ids.KindOperation,
				),
				ScriptExecutionId: executionID,
				InputId:           attempt.Input.ID,
				InputSha256:       bytes.Repeat([]byte{0xaa}, 32),
				StageId:           ids.New(ids.KindStep),
				ExpectedState:     agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
				State:             agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
				Evidence: &agentpb.ScriptCheckpointRequest_StartAuthorized{
					StartAuthorized: &agentpb.ScriptStartAuthorizedCheckpoint{},
				},
			}
			_, err := journal.PrepareScript(t.Context(), attempt, request, at)
			results <- workspaceResult{err: err}
		}()
	}
	workers.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Fatalf("another live workspace blocked allocation: %v", result.err)
		}
	}
	records, err := journal.Scripts(t.Context())
	if err != nil || len(records) != 4 {
		t.Fatalf("workspace poisoned live inventory: count=%d %v", len(records), err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = agentworkjournal.OpenState(root)
	if err != nil {
		t.Fatal(err)
	}
	records, err = journal.Scripts(t.Context())
	if err != nil || len(records) != 4 {
		t.Fatalf("workspace poisoned reconnect: count=%d %v", len(records), err)
	}
	if err := os.RemoveAll(filepath.Join(root, "work")); err != nil {
		t.Fatal(err)
	}
	records, err = journal.Scripts(t.Context())
	if err != nil || len(records) != 4 {
		t.Fatalf("body cleanup changed journal history: count=%d %v", len(records), err)
	}
}
