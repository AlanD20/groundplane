package taskcheckpoint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Each role owns its captured effect authority, not the transport. A restoration
// operation does not implement fake forward-deployment or hook-execution methods.
type ReleaseForwardOwner interface {
	LoadActiveConfiguration(context.Context, workqueue.ResumeView) (releaseexecution.Input, releaseexecution.CheckpointReport, []taskmaterialization.Record, error)
	LoadActiveHook(context.Context, workqueue.ResumeView) (*taskplanning.ScriptWorkDelivery, error)
	LoadCheckpointInput(context.Context, workqueue.ResumeView) (releaseexecution.Input, error)
	PrepareCheckpointEvidence(context.Context, workqueue.ResumeView, releaseexecution.Input, releasework.Snapshot, releaseexecution.CheckpointReport) (workqueue.Change, error)
}

type ReleaseRestorationOwner interface {
	LoadRecoveryHookOwnership(context.Context, workqueue.ResumeView, string) (*etcd.ScriptWorkOwnership, error)
	LoadRecoveryInput(context.Context, workqueue.ResumeView) (releaseexecution.RecoveryInput, error)
	PrepareRecoveryEvidence(context.Context, workqueue.ResumeView, releaseexecution.RecoveryInput, releasework.RecoverySnapshot, releaseexecution.RecoveryReport) (workqueue.Change, error)
	LoadActiveRecoveryConfiguration(context.Context, workqueue.ResumeView) (releaseexecution.RecoveryInput, releaseexecution.RecoveryReport, []taskmaterialization.Record, error)
}

type ReleaseOutcomeOwner interface {
	SettleCompleted(context.Context, workqueue.ResumeView, []keyvalue.Condition, time.Time) error
	SettleFailed(context.Context, workqueue.ResumeView, releaseexecution.FailureReason, []keyvalue.Condition, time.Time) error
}

type ReleaseCheckpointRegistration struct {
	Operation   workexecution.Operation
	Forward     ReleaseForwardOwner
	Restoration ReleaseRestorationOwner
	Outcome     ReleaseOutcomeOwner
}

// The compiled role catalog selects the procedure; this is not a fallback
// decoder. A restoration-only operation's first forward attempt still runs its
// captured restorative procedure, not the ordinary deployment procedure.
func (service *ReleaseCheckpointService) restoring(view workqueue.ResumeView) bool {
	return view.Attempt.Attempt.Direction == workexecution.Recovery || service.forwards[view.Work.Item.Item.Operation] == nil
}
