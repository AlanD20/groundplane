package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AgentWork delivers only accepted Backup input and committed source cursors.
// The scheduler joins its start guards with the native attempt start mutation.
type AgentWork struct {
	inputs          *backupwork.InputRepository
	queue           *workqueue.Repository
	manifests       *backupvolumemanifest.Repository
	configSnapshots *ConfigSnapshotProducer
	configRuntime   *etcd.BackupRuntimeRepository
	configRestores  *ConfigRestoreProducer
	configTransfers *backupconfiguration.ConfigTransferRepository
	diagnostics     *executiondiagnostics.Repository
}

func NewAgentWork(
	inputs *backupwork.InputRepository,
	queue *workqueue.Repository,
	manifests *backupvolumemanifest.Repository,
	configSnapshots *ConfigSnapshotProducer,
	configRuntime *etcd.BackupRuntimeRepository,
	configRestores *ConfigRestoreProducer,
	diagnostics *executiondiagnostics.Repository,
) (*AgentWork, error) {
	if inputs == nil || queue == nil || manifests == nil || configSnapshots == nil || configRuntime == nil ||
		configRestores == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "Backup Agent work requires captured input storage")
	}
	return &AgentWork{
		inputs:          inputs,
		queue:           queue,
		manifests:       manifests,
		configSnapshots: configSnapshots,
		configTransfers: configRuntime.ConfigTransferRepository,
		configRuntime:   configRuntime,
		configRestores:  configRestores,
		diagnostics:     diagnostics,
	}, nil
}

func (owner *AgentWork) LoadWorkAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.BackupWorkAssignment, error) {
	assignment, _, err := owner.inputs.LoadAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	if err := owner.captureWorkConfig(ctx, view, assignment); err != nil {
		return nil, err
	}
	return assignment, nil
}

func (owner *AgentWork) LoadRecoveryAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.BackupRecoveryAssignment, error) {
	assignment, _, err := owner.inputs.LoadRecoveryAssignment(ctx, view)
	return assignment, err
}

func (owner *AgentWork) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	at := time.Now()
	if attempt.Direction == workexecution.Recovery {
		if attempt.StartedAt != nil || !at.Before(attempt.Deadline) || !at.Before(item.Deadline) {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Backup recovery cannot obtain a new start grant",
			)
		}
		_, guards, err := owner.inputs.LoadRecoveryAssignment(ctx, view)
		return guards, err
	}
	if attempt.StartedAt != nil || task.Stop != nil || item.ForwardRevoked ||
		!at.Before(attempt.Deadline) || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Backup cannot obtain a new start grant")
	}
	_, guards, err := owner.inputs.LoadAssignment(ctx, view)
	return guards, err
}
