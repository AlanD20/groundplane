package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeTaskAbortRepository struct {
	task              etcd.TaskRecord
	assignment        etcd.TaskAssignment
	abortPendingCalls int
}

func (repository *fakeTaskAbortRepository) GetTask(
	context.Context,
	string,
) (etcd.Versioned[etcd.TaskRecord], error) {
	return etcd.Versioned[etcd.TaskRecord]{Record: repository.task}, nil
}

func (repository *fakeTaskAbortRepository) GetTaskAssignment(
	context.Context,
	string,
) (etcd.TaskAssignment, error) {
	return repository.assignment, nil
}

func (repository *fakeTaskAbortRepository) AbortPendingTask(
	_ context.Context,
	_ string,
	_ time.Time,
) (etcd.Versioned[etcd.TaskRecord], error) {
	repository.abortPendingCalls++
	repository.task.Status = etcd.TaskStatusAborted
	return etcd.Versioned[etcd.TaskRecord]{Record: repository.task}, nil
}

func TestTaskAbortServiceRejectsInternalBackupPruneBeforeMutation(t *testing.T) {
	// Rationale: backup pruning is observable internal maintenance, so the
	// generic operator abort surface must fail before it changes journal state.
	t.Parallel()
	taskID := ids.NewAt(ids.KindTask, time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC), 101)
	repository := &fakeTaskAbortRepository{task: etcd.TaskRecord{
		ID: taskID, Type: etcd.TaskBackupPrune, Status: etcd.TaskStatusPending,
	}}
	service, err := newTaskAbortService(
		repository,
		&fakeTaskAbortAgentChannel{terminal: make(chan error, 1)},
		&fakeTaskAbortControllerRunner{repository: repository},
	)
	if err != nil {
		t.Fatalf("newTaskAbortService() error = %v", err)
	}
	if _, err := service.AbortTask(context.Background(), taskID, "abort-prune-key"); !errors.Is(
		err,
		errs.New(errs.KindTaskNotAbortable, ""),
	) {
		t.Fatalf("AbortTask(backup_prune) error = %v, want task.not_abortable", err)
	}
	if repository.abortPendingCalls != 0 || repository.task.Status != etcd.TaskStatusPending {
		t.Fatalf("backup_prune mutation calls/status = %d/%s", repository.abortPendingCalls, repository.task.Status)
	}
}

type fakeTaskAbortAgentChannel struct {
	repository      *fakeTaskAbortRepository
	terminal        chan error
	subscribed      bool
	delivered       bool
	deliveredReason string
}

func (channel *fakeTaskAbortAgentChannel) TaskTerminal(
	context.Context,
	string,
	uint64,
	string,
	string,
) (<-chan error, error) {
	channel.subscribed = true
	return channel.terminal, nil
}

func (channel *fakeTaskAbortAgentChannel) AbortTask(
	_ context.Context,
	_ string,
	_ uint64,
	_ string,
	_ string,
	reason string,
) error {
	channel.delivered = channel.subscribed
	channel.deliveredReason = reason
	channel.repository.task.Status = etcd.TaskStatusAborted
	channel.terminal <- nil
	return nil
}

type fakeTaskAbortControllerRunner struct {
	repository *fakeTaskAbortRepository
}

func (runner *fakeTaskAbortControllerRunner) AbortTask(context.Context, string) error {
	runner.repository.task.Status = etcd.TaskStatusAborted
	return nil
}

func TestTaskAbortServiceTerminalizesPendingTaskAndReturnsItsIdentity(t *testing.T) {
	// Rationale: abort is an action on the existing Task, so pending work must
	// terminalize without manufacturing a second Task or operation identity.
	t.Parallel()
	taskID := ids.NewAt(ids.KindTask, time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC), 1)
	repository := &fakeTaskAbortRepository{task: etcd.TaskRecord{ID: taskID, Status: etcd.TaskStatusPending}}
	service, err := newTaskAbortService(
		repository,
		&fakeTaskAbortAgentChannel{terminal: make(chan error, 1)},
		&fakeTaskAbortControllerRunner{repository: repository},
	)
	if err != nil {
		t.Fatalf("newTaskAbortService() error = %v", err)
	}
	response, err := service.AbortTask(context.Background(), taskID, "abort-key")
	assertTaskAbortAccepted(t, response, err, taskID)
	if repository.task.Status != etcd.TaskStatusAborted {
		t.Fatalf("pending Task status = %s", repository.task.Status)
	}
}

func TestTaskAbortServiceSubscribesBeforeFencedAgentDelivery(t *testing.T) {
	// Rationale: natural completion may race the abort send, so subscription
	// must precede generation-fenced delivery and the response must await the
	// durable terminal acknowledgement.
	t.Parallel()
	now := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	agentID := ids.NewAt(ids.KindAgent, now, 3)
	record := etcd.TaskAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, now, 5),
		TaskID:       taskID, Executor: etcd.TaskExecutorAgent, AgentID: agentID, AgentGeneration: 4,
		ClaimedTaskRevision: 1, AssignedAt: now, Deadline: now.Add(time.Minute),
	}
	repository := &fakeTaskAbortRepository{
		task: etcd.TaskRecord{ID: taskID, Status: etcd.TaskStatusRunning},
		assignment: etcd.TaskAssignment{
			Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: record},
			Task: etcd.Versioned[etcd.TaskRecord]{Record: etcd.TaskRecord{
				ID: taskID, Status: etcd.TaskStatusRunning,
			}},
		},
	}
	agents := &fakeTaskAbortAgentChannel{repository: repository, terminal: make(chan error, 1)}
	service, err := newTaskAbortService(
		repository,
		agents,
		&fakeTaskAbortControllerRunner{repository: repository},
	)
	if err != nil {
		t.Fatalf("newTaskAbortService() error = %v", err)
	}
	response, err := service.AbortTask(context.Background(), taskID, "abort-key")
	assertTaskAbortAccepted(t, response, err, taskID)
	if !agents.delivered || agents.deliveredReason != taskAbortReasonOperatorRequested {
		t.Fatalf("Agent abort delivery = %t/%q", agents.delivered, agents.deliveredReason)
	}
}

func assertTaskAbortAccepted(
	t *testing.T,
	response etcd.IdempotencyResponse,
	err error,
	taskID string,
) {
	t.Helper()
	if err != nil {
		t.Fatalf("AbortTask() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if decodeErr := json.Unmarshal(response.Body, &accepted); decodeErr != nil ||
		response.Status != http.StatusAccepted || accepted.TaskID != taskID {
		t.Fatalf("AbortTask() response = %#v/%#v, %v", response, accepted, decodeErr)
	}
}
