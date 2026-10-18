package blueprintrelease

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// LoadActiveConfiguration serves only the configuration selected by an active,
// acknowledged file boundary. The channel must authenticate the supplied view;
// loading these sources grants no new mutation and never follows a desired head.
func (service *Service) LoadActiveConfiguration(ctx context.Context, view workqueue.ResumeView) (releaseexecution.Input, releaseexecution.CheckpointReport, []taskmaterialization.Record, error) {
	var empty releaseexecution.Input
	var noReport releaseexecution.CheckpointReport
	input, err := service.LoadCheckpointInput(ctx, view)
	if err != nil {
		return empty, noReport, nil, err
	}
	saved, err := service.ledger.LoadReleaseWorkProgress(ctx, view, input)
	if err != nil {
		return empty, noReport, nil, err
	}
	if saved.Record == nil || saved.Revision <= 0 || !saved.Record.Progress.InFlight || saved.Record.Progress.Phase != releaseexecution.Configuration ||
		view.Checkpoint.Record == nil || view.Checkpoint.Revision != saved.Revision {
		return empty, noReport, nil, errs.New(errs.KindStateConflict, "Release has no active configuration boundary")
	}
	report := releaseexecution.CheckpointReport{Sequence: saved.Record.CheckpointSequence, StageID: saved.Record.StageID, Progress: saved.Record.Progress}
	ack, err := report.Checkpoint(input.Attempt)
	if err != nil || !releaseexecution.EqualCheckpoint(ack, *view.Checkpoint.Record) {
		return empty, noReport, nil, errs.New(errs.KindStateConflict, "Release configuration checkpoint differs from its saved receipt")
	}
	report.Ack = &ack
	reference, files, err := service.ledger.LoadBlueprintReleaseConfiguration(ctx, view)
	if err != nil {
		return empty, noReport, nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil || !proto.Equal(input.Release.Configuration, &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID,
		EnvironmentId: reference.EnvironmentID, Generation: reference.Generation, Sha256: digest}) {
		return empty, noReport, nil, errs.New(errs.KindStateConflict, "Release configuration source differs from the captured input")
	}
	return input, report, files, nil
}
