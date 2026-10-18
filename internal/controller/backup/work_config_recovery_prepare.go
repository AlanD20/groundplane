package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareConfigRecovery finishes only the captured canonical publication.
// Replay reconstructs that same generation; it neither creates another Restore
// nor authorizes the Agent to materialize files before publication finishes.
func (owner *AgentWork) PrepareConfigRecovery(ctx context.Context, authorization agentregistration.WorkAuthorization,
	wire *agentpb.WorkAttemptIdentity, ordinal uint32,
) (*agentpb.BackupConfigRecoveryPrepareAck, error) {
	sessionGuards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(wire, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, authorization.Identity())
	if err != nil {
		return nil, err
	}
	assignment, _, err := owner.inputs.LoadRecoveryAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	binding, err := backupconfigtransfer.BindRecoverySource(assignment, authorization.Identity(), ordinal,
		agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
	if err != nil {
		return nil, err
	}
	_, _, err = owner.inputs.LoadConfigRecoveryTransfer(ctx, owner.queue, binding, attempt, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, view.Attempt.Attempt.Deadline)
	defer cancel()
	step := assignment.Input.Steps[ordinal]
	transfer, err := configRestoreTransferOwner(backupconfiguration.ConfigTransferOwner{Binding: binding}, step.GetRestore())
	if err != nil {
		return nil, err
	}
	candidate, err := owner.configRestores.PrepareRecoveryCandidate(bounded, transfer,
		step.GetRestore().GetConfig().GetExpectedArchive().GetContent(), step.GetRestore().PointId,
		assignment, authorization.Identity())
	if err != nil {
		return nil, err
	}
	defer candidate.Clear()
	if err := owner.configRuntime.ResumeConfigRestorePublication(bounded, candidate.SourceSeal,
		candidate.Projection, candidate.Identities, view.Attempt.Attempt); err != nil {
		return nil, err
	}
	view, err = owner.queue.ReadResume(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, authorization.Identity())
	if err != nil {
		return nil, err
	}
	assignment, guards, err := owner.inputs.LoadRecoveryAssignment(bounded, view)
	if err != nil {
		return nil, err
	}
	_, transferGuards, err := owner.inputs.LoadConfigRecoveryTransfer(bounded, owner.queue, binding, attempt, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	guards.Conditions = append(guards.Conditions, transferGuards...)
	seal, found, err := owner.configTransfers.ReadConfigRestoreGeneration(bounded, transfer, view.Work.ReadRevision)
	if err != nil {
		return nil, err
	}
	if !found || seal.Record.Completed == nil {
		return nil, configSnapshotGuardConflict()
	}
	guards.Conditions = append(guards.Conditions, keyvalue.Condition{
		Key: backupconfiguration.ConfigRestoreGenerationKey(transfer), ModRevision: seal.Revision,
	})
	guards.Conditions = append(guards.Conditions, sessionGuards...)
	result, err := owner.queue.Commit(bounded, guards)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded || !time.Now().Before(view.Attempt.Attempt.Deadline) {
		return nil, configSnapshotGuardConflict()
	}
	return &agentpb.BackupConfigRecoveryPrepareAck{Assignment: assignment, SourceOrdinal: ordinal, Completed: seal.Record.Completed}, nil
}
