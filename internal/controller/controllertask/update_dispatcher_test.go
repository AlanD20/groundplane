package controllertask

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: restore and execution must select the same closed recovery owner;
// missing native bootstrap may never delegate native claims to Agent updates.
func TestUpdateDispatcherKeepsRecoveryOwnersSeparate(t *testing.T) {
	for _, kind := range []string{etcd.TaskResourceAgent, etcd.TaskResourceController} {
		t.Run(kind, func(t *testing.T) {
			agent, native := &fakeUpdateExecutor{
				status: etcd.TaskStatusCompleted,
			}, &fakeUpdateExecutor{
				status: etcd.TaskStatusFailed,
			}
			dispatcher, err := NewUpdateDispatcher(agent, native)
			if err != nil {
				t.Fatal(err)
			}
			task := etcd.TaskRecord{ID: "task-test", Executor: etcd.TaskExecutorController, Type: etcd.TaskUpdate,
				Params: map[string]string{etcd.TaskResourceKindParam: kind}}
			claim := etcd.TaskAssignment{Task: etcd.Versioned[etcd.TaskRecord]{Record: task}}
			if err := dispatcher.Restore(context.Background(), claim); err != nil {
				t.Fatal(err)
			}
			if _, err := dispatcher.Execute(context.Background(), task, time.Now()); err != nil {
				t.Fatal(err)
			}
			if kind == etcd.TaskResourceAgent && (agent.executions != 1 || native.executions != 0) ||
				kind == etcd.TaskResourceController && (native.executions != 1 || agent.executions != 0) {
				t.Fatal("wrong update owner")
			}
			dispatcher.controller = nil
			if kind == etcd.TaskResourceController {
				if err := dispatcher.Restore(context.Background(), claim); err == nil {
					t.Fatal("native claim accepted without bootstrap")
				}
			}
		})
	}
}
