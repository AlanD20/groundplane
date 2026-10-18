// Package environmentruntime executes the single captured Environment directory
// effect. Start is its durable boundary; reconnect only observes that effect.
package environmentruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Exchange interface {
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	ReportEnvironmentProvision(context.Context, *agentpb.EnvironmentProvisionResultRequest) (*agentpb.EnvironmentProvisionResultAck, error)
}

type Host interface {
	ExecuteEnvironmentProvision(context.Context, string, *agentpb.EnvironmentProvisionHelperRequest) (*agentpb.EnvironmentProvisionHelperResponse, error)
}

type Executor struct {
	root    string
	journal *agentworkjournal.Journal
	host    Host
}

func NewExecutor(root string, journal *agentworkjournal.Journal, host Host) (*Executor, error) {
	if environmentpath.ValidateRoot(root) != nil || journal == nil || host == nil {
		return nil, errs.New(errs.KindInternal, "environment provisioning requires its host root, journal and helper runtime")
	}
	return &Executor{root: root, journal: journal, host: host}, nil
}

func (executor *Executor) Run(ctx context.Context, input environmentexecution.Input, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || exchange == nil || input.Validate() != nil || input.Creation.VolumeRoot != executor.root {
		return workexecution.Attempt{}, errs.New(errs.KindValidationFailed, "environment provisioning input does not match this host")
	}
	savedAttempt, pending, err := executor.journal.LoadEnvironmentProvision(ctx, input.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if pending != nil && pending.Receipt != nil {
		return *pending.Receipt, nil
	}
	if pending != nil && pending.Submitted {
		input.Attempt = *savedAttempt
		return executor.deliver(ctx, input, exchange)
	}
	start, err := exchange.StartAttempt(ctx, input.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	input, reconcile, err := input.WithStart(start)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	saved, err := executor.journal.EnvironmentProvision(ctx, input.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved != nil && saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved != nil && saved.Submitted {
		return executor.deliver(ctx, input, exchange)
	}
	assignment, err := environmentexecution.EncodeAssignment(input)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	inspect := reconcile || input.Attempt.Direction == workexecution.Recovery
	request := &agentpb.EnvironmentProvisionHelperRequest{Assignment: assignment, Start: proto.CloneOf(start), InspectOnly: inspect}
	result := environmentexecution.Result{AttemptID: input.Attempt.ID, ObservedAt: time.Now().UTC()}
	if saved != nil {
		result = saved.Result
		request.InspectOnly = true
	}
	if saved == nil {
		effect, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
		response, effectErr := executor.host.ExecuteEnvironmentProvision(effect, executor.root, request)
		cancel()
		if effectErr == nil {
			effectErr = environmentexecution.ValidateHelperResponse(input, request.InspectOnly, response)
		}
		if effectErr == nil {
			result.DirectoryReady, result.HelpersAbsent, result.ObservedAt = response.DirectoryReady, true, time.Now().UTC()
			if !response.DirectoryReady && input.Attempt.Direction == workexecution.Forward {
				result.Failure = "The captured Environment directory is absent after interrupted provisioning; Retry can create it after this inspection settles"
			}
		} else {
			result.Failure = "Environment directory creation or readiness verification failed"
			if failure := taskprogress.CaptureControllerFailure(effectErr); failure != nil {
				result.Failure += ": " + failure.Summary()
			}
			result.CleanupFailure = "The captured directory helper must be stopped and its effects inspected"
			result.ObservedAt = time.Now().UTC()
		}
	}
	durable, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	err = executor.journal.SaveEnvironmentProvision(durable, input.Attempt, result)
	cancel()
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if !result.HelpersAbsent {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
		request.InspectOnly = true
		response, cleanupErr := executor.host.ExecuteEnvironmentProvision(cleanup, executor.root, request)
		if cleanupErr == nil {
			cleanupErr = environmentexecution.ValidateHelperResponse(input, true, response)
		}
		if cleanupErr == nil {
			result.DirectoryReady, result.HelpersAbsent, result.CleanupFailure = response.DirectoryReady, true, ""
		} else if failure := taskprogress.CaptureControllerFailure(cleanupErr); failure != nil {
			result.CleanupFailure = "Could not stop or inspect the captured provisioning helper: " + failure.Summary()
		}
		result.ObservedAt = time.Now().UTC()
		err = executor.journal.SaveEnvironmentProvision(cleanup, input.Attempt, result)
		cancel()
		if err != nil {
			return workexecution.Attempt{}, err
		}
	}
	return executor.deliver(ctx, input, exchange)
}

func (executor *Executor) deliver(ctx context.Context, input environmentexecution.Input, exchange Exchange) (workexecution.Attempt, error) {
	saved, err := executor.journal.SubmitEnvironmentProvision(ctx, input.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	request, err := environmentexecution.EncodeResult(input.Attempt, saved.Result)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.ReportEnvironmentProvision(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeEnvironmentProvision(ctx, input.Attempt, ack)
}

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.Retire(ctx, terminal)
}
