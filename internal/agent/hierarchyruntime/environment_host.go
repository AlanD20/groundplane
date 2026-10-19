package hierarchyruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type EnvironmentHelper interface {
	StopEnvironmentHelpers(context.Context, workexecution.Attempt, workexecution.Attempt) error
	ExecuteEnvironmentRemoval(
		context.Context,
		string,
		*agentpb.EnvironmentRemovalHelperRequest,
	) (*agentpb.EnvironmentRemovalHelperResponse, error)
}

type Host struct {
	root      string
	journal   *agentworkjournal.Journal
	helper    EnvironmentHelper
	ownership *agentownership.Lease
}

func NewHost(
	volumeRoot string,
	journal *agentworkjournal.Journal,
	helper EnvironmentHelper,
	ownership *agentownership.Lease,
) (*Host, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || journal == nil || helper == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Environment removal host requires its configured root, journal and helper",
		)
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &Host{root: volumeRoot, journal: journal, helper: helper, ownership: ownership}, nil
}

func (host *Host) RemoveRuntime(
	ctx context.Context,
	input hierarchyexecution.EnvironmentInput,
	progress hierarchyexecution.EnvironmentProgress,
	inspectOnly bool,
) (hierarchyexecution.EnvironmentObservation, error) {
	if progress.Phase != hierarchyexecution.RemoveRuntime {
		return hierarchyexecution.EnvironmentObservation{}, errs.New(
			errs.KindStateConflict,
			"Environment runtime removal requires its acknowledged runtime stage",
		)
	}
	return host.execute(ctx, input, progress, inspectOnly)
}

func (host *Host) RemoveDirectory(
	ctx context.Context,
	input hierarchyexecution.EnvironmentInput,
	progress hierarchyexecution.EnvironmentProgress,
	inspectOnly bool,
) (hierarchyexecution.EnvironmentObservation, error) {
	if progress.Phase != hierarchyexecution.RemoveDirectory {
		return hierarchyexecution.EnvironmentObservation{}, errs.New(
			errs.KindStateConflict,
			"Environment directory removal requires its acknowledged directory stage",
		)
	}
	return host.execute(ctx, input, progress, inspectOnly)
}

func (host *Host) execute(
	ctx context.Context,
	input hierarchyexecution.EnvironmentInput,
	progress hierarchyexecution.EnvironmentProgress,
	inspectOnly bool,
) (hierarchyexecution.EnvironmentObservation, error) {
	saved, err := host.journal.EnvironmentRemoval(ctx, input)
	if err != nil {
		return hierarchyexecution.EnvironmentObservation{}, err
	}
	if saved == nil || !saved.Acknowledged || !progress.InFlight ||
		!hierarchyexecution.EqualEnvironmentProgress(saved.Report.Progress, progress) {
		return hierarchyexecution.EnvironmentObservation{}, errs.New(
			errs.KindStateConflict,
			"Environment removal host action has no acknowledged journal boundary",
		)
	}
	if input.Recovery != nil {
		if err := host.ownership.CheckRecovery(input.Recovery.Forward, input.Attempt); err != nil {
			return hierarchyexecution.EnvironmentObservation{}, err
		}
		if err := host.helper.StopEnvironmentHelpers(ctx, input.Recovery.Forward, input.Attempt); err != nil {
			return hierarchyexecution.EnvironmentObservation{}, err
		}
		if err := host.ownership.CheckRecovery(input.Recovery.Forward, input.Attempt); err != nil {
			return hierarchyexecution.EnvironmentObservation{}, err
		}
	}
	assignment, err := hierarchyexecution.EncodeEnvironmentAssignment(input)
	if err != nil {
		return hierarchyexecution.EnvironmentObservation{}, err
	}
	report, err := hierarchyexecution.EncodeEnvironmentCheckpointRequest(input, saved.Report)
	if err != nil {
		return hierarchyexecution.EnvironmentObservation{}, err
	}
	ack, err := hierarchyexecution.EncodeEnvironmentCheckpointAck(input, saved.Report)
	if err != nil {
		return hierarchyexecution.EnvironmentObservation{}, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return hierarchyexecution.EnvironmentObservation{}, err
	}
	response, err := host.helper.ExecuteEnvironmentRemoval(ctx, host.root, &agentpb.EnvironmentRemovalHelperRequest{
		Assignment: assignment, Start: &agentpb.WorkStartAck{Attempt: attempt, StartedAt: timestamppb.New(*input.Attempt.StartedAt)},
		Report: report, Acknowledgement: ack, InspectOnly: inspectOnly,
	})
	if err != nil {
		return hierarchyexecution.EnvironmentObservation{}, err
	}
	return hierarchyexecution.DecodeEnvironmentHelperResponse(input, progress, inspectOnly, response)
}
