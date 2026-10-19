package dnsresolver

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *WorkReports) CompleteResolver(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ResolverWorkCompletionRequest,
) (*agentpb.ResolverWorkResultAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil ||
		proto.Size(request) > resolverexecution.MaximumReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "CoreDNS completion report is invalid or oversized")
	}
	claimed, err := resolverexecution.DecodeReference(request.Evidence)
	if err != nil {
		return nil, err
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, true)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		change, expected, err := service.records.PrepareCompleted(ctx, view, request.ServingProof, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		if expected != claimed {
			return nil, errs.New(
				errs.KindStateConflict,
				"CoreDNS completion names different acknowledged runtime evidence",
			)
		}
		if err := service.commitReport(ctx, guards, change); err != nil {
			return nil, err
		}
		view, err = service.reportView(ctx, authorization, request.Attempt, true)
		if err != nil {
			return nil, err
		}
	}
	record, _, err := service.records.ReadCompleted(ctx, view)
	if err != nil {
		return nil, err
	}
	if record.Attempt.Result == nil || *record.Attempt.Result != claimed ||
		!proto.Equal(record.Proof, request.ServingProof) {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS completion replay differs from its saved result")
	}
	service.reportTerminal(ctx, view, "completion")
	return resolverResultAck(request.Attempt, view.Attempt.Attempt, "completion")
}

func (service *WorkReports) FailResolver(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ResolverWorkFailureRequest,
) (*agentpb.ResolverWorkResultAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "CoreDNS failure request is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, false)
	if err != nil {
		return nil, err
	}
	reason := resolverexecution.FailureReason(request.Reason)
	if view.Attempt.Attempt.FinishedAt == nil {
		change, _, err := service.records.PrepareFailure(ctx, view, reason, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		if err := service.commitReport(ctx, guards, change); err != nil {
			return nil, err
		}
		view, err = service.reportView(ctx, authorization, request.Attempt, false)
		if err != nil {
			return nil, err
		}
	}
	failure, raw, _, err := service.records.ReadFailure(ctx, view)
	if err != nil {
		return nil, err
	}
	if failure.Reason != reason {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS failure replay differs from its saved cause")
	}
	ack, err := resolverResultAck(request.Attempt, view.Attempt.Attempt, "failure")
	if err != nil {
		return nil, err
	}
	ack.Failure = raw
	service.reportTerminal(ctx, view, "failure")
	return ack, nil
}

func (service *WorkReports) RecoverResolver(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ResolverWorkRecoveryRequest,
) (*agentpb.ResolverWorkResultAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || len(request.Report) == 0 ||
		len(request.Report) > resolverexecution.MaximumReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "CoreDNS cleanup report is missing or oversized")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, true)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		change, _, err := service.records.PrepareRecoveryResult(ctx, view, request.Report, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		if err := service.commitReport(ctx, guards, change); err != nil {
			return nil, err
		}
		view, err = service.reportView(ctx, authorization, request.Attempt, true)
		if err != nil {
			return nil, err
		}
	}
	saved, _, err := service.records.ReadRecoveryResult(ctx, view)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(saved, request.Report) {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS cleanup replay differs from its saved result")
	}
	service.reportTerminal(ctx, view, "recovery")
	return resolverResultAck(request.Attempt, view.Attempt.Attempt, "recovery")
}
