package agent

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/remotefetchruntime"
	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (c *Client) ConfigureRemoteImage(executor *remotefetchruntime.Executor) error {
	if executor == nil {
		return errs.New(errs.KindInternal, "Agent remote image Fetch executor is missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.remoteImage != nil {
		return errs.New(errs.KindStateConflict, "Agent remote image Fetch executor is already configured")
	}
	c.remoteImage = executor
	return nil
}

func (workers *executionWorkers) submitRemoteImage(
	message *agentpb.RemoteImageAssignment,
	available int,
	exchange *nativeExchange,
) error {
	if workers.remoteImage == nil {
		return errs.New(errs.KindStateConflict, "remote image Fetch is unavailable on this Agent")
	}
	attempt, plan, err := remotefetch.DecodeAssignment(message, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "remote image Fetch execution session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.remoteImage == nil || !proto.Equal(active.remoteImage, message) {
			return errs.New(errs.KindStateConflict, "remote image Fetch already has a different active assignment")
		}
		return nil
	}
	if available <= 0 || !workers.canAdmit(attempt.TaskID) {
		return errs.New(errs.KindStateConflict, "Agent execution capacity is exhausted")
	}
	lease, err := workers.ownership.Begin(attempt)
	if err != nil {
		return err
	}
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, remoteImage: proto.CloneOf(message)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		err := workers.remoteImage.Run(workers.executionContext(attempt), attempt, plan, exchange)
		err = errors.Join(err, lease.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}

func (exchange *nativeExchange) ReportRemoteImage(
	ctx context.Context,
	request *agentpb.RemoteImageResultRequest,
) error {
	_, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_RemoteImageResultRequest{
		RemoteImageResultRequest: request,
	}}, request.GetAttempt(), "remote_image_result", "")
	return err
}
