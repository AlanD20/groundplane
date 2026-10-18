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

func (service *Service) LoadActiveRecoveryConfiguration(ctx context.Context, view workqueue.ResumeView) (releaseexecution.RecoveryInput, releaseexecution.RecoveryReport, []taskmaterialization.Record, error) {
	empty, noReport := releaseexecution.RecoveryInput{}, releaseexecution.RecoveryReport{}
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return empty, noReport, nil, err
	}
	saved, err := service.ledger.LoadReleaseRecoveryProgress(ctx, view, input)
	if err != nil {
		return empty, noReport, nil, err
	}
	if saved.Record == nil || saved.Revision <= 0 || view.Checkpoint.Record == nil || view.Checkpoint.Revision != saved.Revision {
		return empty, noReport, nil, errs.New(errs.KindStateConflict, "Release recovery has no saved configuration checkpoint")
	}
	report := saved.Record.Report
	if !report.Progress.InFlight || (report.Progress.Phase != releaseexecution.RestoreConfiguration && report.Progress.Phase != releaseexecution.VerifyRestoration) {
		return empty, noReport, nil, errs.New(errs.KindStateConflict, "Release recovery is not at a captured-file boundary")
	}
	ack, err := report.Checkpoint(input)
	if err != nil || !releaseexecution.EqualCheckpoint(ack, *view.Checkpoint.Record) {
		return empty, noReport, nil, errs.New(errs.KindStateConflict, "Release recovery file checkpoint differs from its saved receipt")
	}
	report.Ack = &ack
	reference, files, err := service.ledger.LoadBlueprintReleaseRecoveryConfiguration(ctx, view)
	if err != nil {
		return empty, noReport, nil, err
	}
	var expected *agentpb.WorkConfigurationReference
	if reference != nil {
		digest, err := hex.DecodeString(reference.SHA256)
		if err != nil {
			return empty, noReport, nil, err
		}
		expected = &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID, EnvironmentId: reference.EnvironmentID,
			Generation: reference.Generation, Sha256: digest}
	}
	if !proto.Equal(expected, input.Input.Release.PriorConfiguration) {
		return empty, noReport, nil, errs.New(errs.KindStateConflict, "Release recovery source differs from its captured prior configuration")
	}
	return input, report, files, nil
}
