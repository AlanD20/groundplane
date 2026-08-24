package agentchannel

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestEnvironmentDirectoryAcknowledgementUsesAtomicProvisioningPath(t *testing.T) {
	t.Parallel()
	const (
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		agentID       = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		assignmentID  = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	planHash := make([]byte, 32)
	for index := range planHash {
		planHash[index] = byte(index + 1)
	}
	store := &environmentAcknowledgementStore{task: etcd.TaskRecord{
		ID: taskID, Type: etcd.TaskCreate, Target: environmentID, PlanHash: hex.EncodeToString(planHash),
	}}
	server := &Server{tasks: store, now: func() time.Time {
		return time.Date(2026, 8, 22, 17, 0, 0, 0, time.UTC)
	}}
	acknowledgement := &agentpb.TaskAck{
		TaskId: taskID, AssignmentId: assignmentID, PlanHash: planHash,
		Terminal: agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED,
		Result: &agentpb.TaskAck_EnvironmentDirectoryResult{
			EnvironmentDirectoryResult: &agentpb.EnvironmentDirectoryTaskResult{},
		},
	}
	if err := server.acknowledge(context.Background(), agentID, 7, acknowledgement); err != nil {
		t.Fatalf("acknowledge() error = %v", err)
	}
	if store.genericCalled || !store.environmentCalled || store.environmentID != environmentID ||
		store.result.Kind != etcd.TaskResultEnvironmentDirectory {
		t.Fatalf("acknowledgement path = generic %t, environment %t/%q, result %#v",
			store.genericCalled, store.environmentCalled, store.environmentID, store.result)
	}
}

type environmentAcknowledgementStore struct {
	task              etcd.TaskRecord
	genericCalled     bool
	environmentCalled bool
	environmentID     string
	result            etcd.TaskResultRecord
}

func (store *environmentAcknowledgementStore) ListAgentAssignments(
	context.Context, string, uint64, int32,
) ([]etcd.TaskAssignment, error) {
	return nil, nil
}

func (store *environmentAcknowledgementStore) ClaimNextTask(
	context.Context, string, uint64, time.Time,
) (etcd.TaskAssignment, bool, error) {
	return etcd.TaskAssignment{}, false, nil
}

func (store *environmentAcknowledgementStore) GetTask(
	context.Context, string,
) (etcd.Versioned[etcd.TaskRecord], error) {
	return etcd.Versioned[etcd.TaskRecord]{Record: store.task, Revision: 1, ReadRevision: 1}, nil
}

func (store *environmentAcknowledgementStore) AppendTaskEvent(
	context.Context, etcd.TaskEventInput, time.Time,
) (etcd.TaskEventAppend, error) {
	return etcd.TaskEventAppend{}, nil
}

func (store *environmentAcknowledgementStore) AcknowledgeTask(
	context.Context,
	string,
	uint64,
	string,
	string,
	etcd.TaskStatus,
	etcd.TaskResultRecord,
	time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
	store.genericCalled = true
	return etcd.Versioned[etcd.TaskRecord]{}, nil
}

func (store *environmentAcknowledgementStore) AcknowledgeEnvironmentCreation(
	_ context.Context,
	_ string,
	_ uint64,
	_ string,
	_ string,
	environmentID string,
	_ etcd.TaskStatus,
	result etcd.TaskResultRecord,
	_ time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
	store.environmentCalled = true
	store.environmentID = environmentID
	store.result = result
	return etcd.Versioned[etcd.TaskRecord]{Record: store.task}, nil
}
