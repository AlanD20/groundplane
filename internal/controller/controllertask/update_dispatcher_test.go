package controllertask

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: restore and execution must select the same closed recovery owner;
// missing native bootstrap may never delegate native claims to Agent updates.
func TestUpdateDispatcherKeepsRecoveryOwnersSeparate(t *testing.T) {
	for _, kind := range []string{testtaskjournal.TaskResourceAgent, testtaskjournal.TaskResourceController} {
		t.Run(kind, func(t *testing.T) {
			agent, native := &fakeUpdateExecutor{
				status: testtaskjournal.TaskStatusCompleted,
			}, &fakeUpdateExecutor{
				status: testtaskjournal.TaskStatusFailed,
			}
			dispatcher, err := NewUpdateDispatcher(agent, native)
			if err != nil {
				t.Fatal(err)
			}
			task := etcd.TaskRecord{
				ID:       "task-test",
				Executor: testtaskjournal.TaskExecutorController,
				Type:     testtaskjournal.TaskUpdate,
				Params:   map[string]string{testtaskjournal.TaskResourceKindParam: kind},
			}
			claim := etcd.TaskAssignment{Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: task}}
			if err := dispatcher.Restore(context.Background(), claim); err != nil {
				t.Fatal(err)
			}
			if _, err := dispatcher.Execute(context.Background(), task, time.Now()); err != nil {
				t.Fatal(err)
			}
			if kind == testtaskjournal.TaskResourceAgent && (agent.executions != 1 || native.executions != 0) ||
				kind == testtaskjournal.TaskResourceController && (native.executions != 1 || agent.executions != 0) {
				t.Fatal("wrong update owner")
			}
			dispatcher.controller = nil
			if kind == testtaskjournal.TaskResourceController {
				if err := dispatcher.Restore(context.Background(), claim); err == nil {
					t.Fatal("native claim accepted without bootstrap")
				}
			}
		})
	}
}
