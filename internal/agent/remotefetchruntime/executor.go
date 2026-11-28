// Package remotefetchruntime performs one exact digest-only Agent Fetch.
package remotefetchruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Journal interface {
	BeginRemoteImage(context.Context, workexecution.Attempt) (bool, *string, error)
	SettleTerminalDelivery(context.Context, string) error
}

type Host interface {
	FetchPublicImage(context.Context, imagefetch.Plan) (string, error)
	ObservePublicImage(context.Context, imagefetch.Plan) (string, error)
}

type Exchange interface {
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	ReportRemoteImage(context.Context, *agentpb.RemoteImageResultRequest) error
}

type Executor struct {
	journal Journal
	host    Host
}

func New(journal Journal, host Host) (*Executor, error) {
	if journal == nil || host == nil {
		return nil, errs.New(errs.KindInternal, "Remote Fetch requires a Host Docker runtime and Agent journal")
	}
	return &Executor{journal: journal, host: host}, nil
}

func (executor *Executor) Run(ctx context.Context, attempt workexecution.Attempt,
	plan imagefetch.Plan, exchange Exchange) error {
	if attempt.Direction != workexecution.Forward {
		return errs.New(errs.KindStateConflict, "Remote Fetch has no mutating recovery")
	}
	if _, err := remotefetch.EncodeAssignment(attempt, plan); err != nil {
		return err
	}
	start, err := exchange.StartAttempt(ctx, attempt)
	if err != nil {
		return err
	}
	started, reconcile, err := workexecution.DecodeAgentStartAck(start, attempt)
	if err != nil {
		return err
	}
	attempt.StartedAt = &started
	fresh, saved, err := executor.journal.BeginRemoteImage(ctx, attempt)
	if err != nil {
		return err
	}
	localID := ""
	if saved != nil {
		localID = *saved
	} else {
		effect, cancel := context.WithDeadline(ctx, attempt.Deadline)
		action := executor.host.FetchPublicImage
		if !fresh || reconcile {
			action = executor.host.ObservePublicImage
		}
		observed, err := action(effect, plan)
		cancel()
		if err == nil && (observed == plan.ConfigDigest || observed == plan.ManifestDigest) {
			localID = observed
		}
	}
	request, err := remotefetch.EncodeReport(attempt, localID)
	if err != nil {
		return err
	}
	if err := exchange.ReportRemoteImage(ctx, request); err != nil {
		return err
	}
	return executor.journal.SettleTerminalDelivery(ctx, attempt.ID)
}
