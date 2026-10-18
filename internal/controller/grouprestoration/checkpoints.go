package grouprestoration

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Service) PrepareRecoveryEvidence(ctx context.Context, view workqueue.ResumeView, input releaseexecution.RecoveryInput,
	previous releasework.RecoverySnapshot, report releaseexecution.RecoveryReport,
) (workqueue.Change, error) {
	if !view.ActiveAttempt || !view.CurrentExecutor || input.Group == nil || input.Validate() != nil ||
		!workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Group restoration checkpoint does not own the current attempt")
	}
	var progress *releaseexecution.RecoveryProgress
	if previous.Record != nil {
		progress = &previous.Record.Report.Progress
	}
	if err := releaseexecution.ValidateRecoveryTransition(input, progress, report.Progress); err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := service.activeConditions(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: conditions}
	if !report.Progress.InFlight {
		proof, err := service.ledger.RecoveryEffectConditions(ctx, view, input, previous, report)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, proof.Conditions...)
	}
	return change, nil
}

func (service *Service) activeBoundary(ctx context.Context, view workqueue.ResumeView) (releaseexecution.RecoveryInput, releaseexecution.RecoveryReport, error) {
	input, err := service.LoadRecoveryInput(ctx, view)
	if err != nil {
		return releaseexecution.RecoveryInput{}, releaseexecution.RecoveryReport{}, err
	}
	saved, err := service.ledger.LoadReleaseRecoveryProgress(ctx, view, input)
	if err != nil {
		return input, releaseexecution.RecoveryReport{}, err
	}
	if saved.Record == nil || view.Checkpoint.Record == nil || saved.Revision != view.Checkpoint.Revision || !saved.Record.Report.Progress.InFlight {
		return input, releaseexecution.RecoveryReport{}, errs.New(errs.KindStateConflict, "Group restoration has no saved active boundary")
	}
	report := saved.Record.Report
	checkpoint, err := report.Checkpoint(input)
	if err != nil || !releaseexecution.EqualCheckpoint(checkpoint, *view.Checkpoint.Record) {
		return input, report, errs.New(errs.KindStateConflict, "Group restoration boundary differs from its retained checkpoint")
	}
	report.Ack = &checkpoint
	return input, report, nil
}

func (service *Service) LoadRecoveryHookOwnership(ctx context.Context, view workqueue.ResumeView, executionID string) (*etcd.ScriptWorkOwnership, error) {
	input, report, err := service.activeBoundary(ctx, view)
	if err != nil {
		return nil, err
	}
	if report.Progress.Phase != releaseexecution.QuiesceForward {
		return nil, errs.New(errs.KindStateConflict, "Group hook cleanup is outside its acknowledged quiescence boundary")
	}
	sources, err := service.ledger.LoadGroupRestorationSources(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer sources.Clear()
	for _, hook := range input.Input.Release.Hooks {
		if hook.ExecutionId == executionID {
			return service.artifacts.ResolveReleaseScriptOwnership(ctx, sources.Source.Source, executionID,
				workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)})
		}
	}
	return nil, errs.New(errs.KindStateConflict, "requested Script is not a captured hook of this Group member")
}

func (service *Service) LoadActiveRecoveryConfiguration(ctx context.Context, view workqueue.ResumeView) (releaseexecution.RecoveryInput, releaseexecution.RecoveryReport, []taskmaterialization.Record, error) {
	input, report, err := service.activeBoundary(ctx, view)
	if err != nil {
		return input, report, nil, err
	}
	if report.Progress.Phase != releaseexecution.RestoreConfiguration && report.Progress.Phase != releaseexecution.VerifyRestoration {
		return input, report, nil, errs.New(errs.KindStateConflict, "Group restoration files are outside their acknowledged boundary")
	}
	sources, err := service.ledger.LoadGroupRestorationSources(ctx, view.Work)
	if err != nil {
		return input, report, nil, err
	}
	defer sources.Clear()
	reference, files, err := service.ledger.LoadCapturedRestorationConfiguration(ctx, sources.Release.Input.Configuration, view.Work.ReadRevision)
	if err != nil {
		return input, report, nil, err
	}
	var expected *agentpb.WorkConfigurationReference
	if reference != nil {
		digest, err := hex.DecodeString(reference.SHA256)
		if err != nil {
			return input, report, nil, err
		}
		expected = &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID, EnvironmentId: reference.EnvironmentID,
			Generation: reference.Generation, Sha256: digest}
	}
	if !proto.Equal(expected, input.Input.Release.PriorConfiguration) {
		return input, report, nil, errs.New(errs.KindStateConflict, "Group restoration files differ from the captured prior configuration")
	}
	return input, report, files, nil
}
