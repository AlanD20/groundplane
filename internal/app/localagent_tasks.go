package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type localAgentTaskAssignments interface {
	ListAgentAssignments(context.Context, string, uint64, int32) ([]etcd.TaskAssignment, error)
}

type localAgentTaskChannel interface {
	TaskTerminal(context.Context, string, uint64, string) (<-chan error, error)
	AbortTask(context.Context, string, uint64, string, string) error
}

// localAgentTasksAdapter closes the delivery-versus-acknowledgement race in
// Agent removal. Assignment fencing is owned by the lifecycle manager before
// this adapter is called; this adapter subscribes before delivery and returns
// only after the durable terminal transaction has committed.
type localAgentTasksAdapter struct {
	assignments localAgentTaskAssignments
	channel     localAgentTaskChannel
}

type localAgentTaskSubscription struct {
	cancel context.CancelFunc
	result <-chan error
}

func newLocalAgentTasksAdapter(
	assignments localAgentTaskAssignments,
	channel localAgentTaskChannel,
) (*localAgentTasksAdapter, error) {
	if assignments == nil {
		return nil, errs.New(errs.KindInternal, "local agent Task assignments are required")
	}
	if channel == nil {
		return nil, errs.New(errs.KindInternal, "local agent Task channel is required")
	}
	return &localAgentTasksAdapter{assignments: assignments, channel: channel}, nil
}

func (adapter *localAgentTasksAdapter) RequireIdle(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
) error {
	assignments, err := adapter.assignments.ListAgentAssignments(ctx, agentID, generation, maximum)
	if err != nil {
		return err
	}
	if len(assignments) != 0 {
		return errs.New(errs.KindResourceInUse, "local Agent has active Task assignments")
	}
	return nil
}

func (adapter *localAgentTasksAdapter) AbortActive(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
	reason string,
) error {
	first, err := adapter.assignments.ListAgentAssignments(ctx, agentID, generation, maximum)
	if err != nil {
		return err
	}
	if len(first) == 0 {
		return nil
	}

	subscriptions := make(map[string]localAgentTaskSubscription, len(first))
	order := make([]string, 0, len(first))
	defer func() {
		for _, subscription := range subscriptions {
			subscription.cancel()
		}
	}()

	for _, assignment := range first {
		taskID, err := localAgentAssignmentTaskID(assignment)
		if err != nil {
			return err
		}
		if _, exists := subscriptions[taskID]; exists {
			return errs.New(errs.KindInternal, "local agent Task assignment snapshot contains a duplicate")
		}
		subscriptionContext, cancel := context.WithCancel(ctx)
		terminal, err := adapter.channel.TaskTerminal(subscriptionContext, agentID, generation, taskID)
		if err != nil {
			cancel()
			return err
		}
		if terminal == nil {
			cancel()
			return errs.New(errs.KindInternal, "local agent Task terminal subscription is nil")
		}
		subscriptions[taskID] = localAgentTaskSubscription{cancel: cancel, result: terminal}
		order = append(order, taskID)
	}

	second, err := adapter.assignments.ListAgentAssignments(ctx, agentID, generation, maximum)
	if err != nil {
		return err
	}
	remaining := make(map[string]struct{}, len(second))
	for _, assignment := range second {
		taskID, err := localAgentAssignmentTaskID(assignment)
		if err != nil {
			return err
		}
		if _, subscribed := subscriptions[taskID]; !subscribed {
			return errs.New(errs.KindInternal, "local agent Task assignment appeared after assignment fencing")
		}
		if _, duplicate := remaining[taskID]; duplicate {
			return errs.New(errs.KindInternal, "local agent Task assignment snapshot contains a duplicate")
		}
		remaining[taskID] = struct{}{}
	}
	for taskID, subscription := range subscriptions {
		if _, present := remaining[taskID]; present {
			continue
		}
		subscription.cancel()
		delete(subscriptions, taskID)
	}

	for _, taskID := range order {
		subscription, active := subscriptions[taskID]
		if !active {
			continue
		}
		if terminalErr, completed := localAgentTaskTerminalResult(subscription.result); completed {
			if terminalErr != nil {
				return terminalErr
			}
			subscription.cancel()
			delete(subscriptions, taskID)
			continue
		}
		if err := adapter.channel.AbortTask(ctx, agentID, generation, taskID, reason); err != nil {
			if terminalErr, completed := localAgentTaskTerminalResult(subscription.result); completed {
				if terminalErr != nil {
					return terminalErr
				}
				subscription.cancel()
				delete(subscriptions, taskID)
				continue
			}
			return err
		}
	}

	for _, taskID := range order {
		subscription, active := subscriptions[taskID]
		if !active {
			continue
		}
		select {
		case terminalErr, open := <-subscription.result:
			if !open {
				return errs.New(errs.KindInternal, "local agent Task terminal subscription closed without a result")
			}
			if terminalErr != nil {
				return terminalErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func localAgentAssignmentTaskID(assignment etcd.TaskAssignment) (string, error) {
	taskID := assignment.Assignment.Record.TaskID
	if taskID == "" || assignment.Task.Record.ID != taskID {
		return "", errs.New(errs.KindInternal, "local agent Task assignment identity is inconsistent")
	}
	return taskID, nil
}

func localAgentTaskTerminalResult(result <-chan error) (error, bool) {
	select {
	case terminalErr, open := <-result:
		if !open {
			return errs.New(errs.KindInternal, "local agent Task terminal subscription closed without a result"), true
		}
		return terminalErr, true
	default:
		return nil, false
	}
}

var _ localagent.Tasks = (*localAgentTasksAdapter)(nil)
