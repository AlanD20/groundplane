package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) volumeCaptureStaging(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredStage, source backupwork.StagingSource,
) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error) {
	run, guard, err := owner.inputs.LoadStagingCapture(ctx, source)
	if err != nil {
		return nil, nil, err
	}
	if source.Input.Steps[source.Ordinal].GetCapture().GetVolume() == nil || entry.DatabaseExecution {
		return nil, nil, invalidCaptureStaging()
	}
	item := source.View.Item.Item
	settled := item.Phase == workexecution.Settled && (item.Cleanup == workexecution.CleanupNone || item.Cleanup == workexecution.CleanupComplete)
	terminal := run.State == backupruntime.BackupRunCompleted || run.State == backupruntime.BackupRunFailed ||
		run.State == backupruntime.BackupRunAborted || run.State == backupruntime.BackupRunTimedOut
	guards := append(append([]keyvalue.Condition(nil), source.Conditions...), guard)
	if settled && terminal {
		return discardCapturedFiles(entry), guards, nil
	}
	if terminal {
		return nil, nil, invalidCaptureStaging()
	}
	resume := source.Sources[source.Ordinal].GetCapture()
	if resume == nil {
		return nil, nil, invalidCaptureStaging()
	}
	if resume.PreparedArtifact != nil {
		decision, liveGuards, err := owner.resumePreparedCapture(ctx, identity, entry, source)
		if err != nil {
			return nil, nil, err
		}
		return decision, append(guards, liveGuards...), nil
	}
	// A partial Volume archive has not reached Prepared's durable receipt, so
	// it cannot have been uploaded. Discard only under the original live work;
	// a replacement or timed-out attempt is not itself proof of stopped effects.
	_, liveGuards, err := owner.stagingAssignment(ctx, identity, source)
	if err != nil {
		return nil, nil, err
	}
	return discardCapturedFiles(entry), append(guards, liveGuards...), nil
}

func discardCapturedFiles(entry *agentpb.BackupRecoveredStage) *agentpb.BackupStagingDisposition {
	return &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
		Disposition: &agentpb.BackupStagingDisposition_DiscardRecovered{DiscardRecovered: &agentpb.BackupDiscardRecovered{}}}
}
