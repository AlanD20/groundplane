package agent

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/ingressruntime"
	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (c *Client) ConfigurePublicIngress(executor *ingressruntime.Executor) error {
	if executor == nil {
		return errs.New(errs.KindInternal, "Agent public ingress executor is missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.publicIngress != nil {
		return errs.New(errs.KindStateConflict, "Agent public ingress executor is already configured")
	}
	c.publicIngress = executor
	return nil
}

func (workers *executionWorkers) submitPublicIngress(
	message *agentpb.PublicIngressAssignment,
	available int,
	exchange *nativeExchange,
) error {
	if workers.publicIngress == nil {
		return errs.New(errs.KindStateConflict, "public ingress is unavailable on this Agent")
	}
	attempt, plan, err := ingressexecution.DecodeAssignment(message, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "public ingress execution session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.publicIngress == nil || !proto.Equal(active.publicIngress, message) {
			return errs.New(errs.KindStateConflict, "public ingress already has a different active assignment")
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
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, publicIngress: proto.CloneOf(message)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		err := workers.publicIngress.Run(workers.executionContext(attempt), attempt, plan, exchange)
		err = errors.Join(err, lease.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}

func (exchange *nativeExchange) ReportPublicIngress(
	ctx context.Context,
	request *agentpb.PublicIngressResultRequest,
) error {
	_, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_PublicIngressResultRequest{
		PublicIngressResultRequest: request,
	}}, request.GetAttempt(), "public_ingress_result", "")
	return err
}
