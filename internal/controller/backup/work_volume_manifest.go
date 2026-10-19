package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) BeginVolumeManifest(ctx context.Context, authorization agentregistration.WorkAuthorization,
	attempt *agentpb.WorkAttemptIdentity, ordinal uint32, direction agentpb.BackupVolumeManifestDirection,
) (*agentpb.BackupVolumeManifestAckCredit, bool, error) {
	manifestOwner, guards, err := owner.volumeManifestOwner(ctx, authorization, attempt, ordinal, direction)
	if err != nil {
		return nil, false, err
	}
	if _, err := owner.manifests.Open(ctx, manifestOwner, guards); err != nil {
		return nil, false, err
	}
	cursor, found, err := owner.manifests.Read(ctx, manifestOwner, 0)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, invalidVolumeManifestAuthority()
	}
	// Return one cursor's credit and terminal state, rather than pairing an
	// earlier grant with a later completion observed during reconnect.
	credit, err := cursor.Record.LastCredit()
	return credit, cursor.Record.Complete, err
}

func (owner *AgentWork) AcceptVolumeManifestFrame(ctx context.Context, authorization agentregistration.WorkAuthorization,
	attempt *agentpb.WorkAttemptIdentity,
	frame *agentpb.BackupVolumeManifestTransfer,
) (*agentpb.BackupVolumeManifestAckCredit, error) {
	if frame == nil {
		return nil, invalidVolumeManifestAuthority()
	}
	manifestOwner, guards, err := owner.volumeManifestOwner(ctx, authorization, attempt, frame.SourceOrdinal, frame.Direction)
	if err != nil {
		return nil, err
	}
	return owner.manifests.Accept(ctx, manifestOwner, frame, guards)
}

func (owner *AgentWork) ReadCompleteVolumeManifest(ctx context.Context, authorization agentregistration.WorkAuthorization,
	attempt *agentpb.WorkAttemptIdentity, ordinal uint32, direction agentpb.BackupVolumeManifestDirection, revision int64,
) (backupvolumemanifest.Complete, error) {
	manifestOwner, _, err := owner.volumeManifestOwner(ctx, authorization, attempt, ordinal, direction)
	if err != nil {
		return backupvolumemanifest.Complete{}, err
	}
	return owner.manifests.ReadComplete(ctx, manifestOwner, revision)
}

func (owner *AgentWork) volumeManifestOwner(ctx context.Context, authorization agentregistration.WorkAuthorization,
	wire *agentpb.WorkAttemptIdentity, ordinal uint32, direction agentpb.BackupVolumeManifestDirection,
) (backupvolumemanifest.Owner, []keyvalue.Condition, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return backupvolumemanifest.Owner{}, nil, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(wire, authorization.Identity())
	if err != nil {
		return backupvolumemanifest.Owner{}, nil, err
	}
	view, err := authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return backupvolumemanifest.Owner{}, nil, err
	}
	at := time.Now().UTC()
	live := view.Attempt.Attempt
	if !workexecution.SameAssignment(live, attempt) || !view.CurrentExecutor || !view.ActiveAttempt ||
		live.StartedAt == nil || live.FinishedAt != nil || !at.Before(live.Deadline) {
		return backupvolumemanifest.Owner{}, nil, invalidVolumeManifestAuthority()
	}
	var binding backupvolumetransfer.Binding
	var change workqueue.Change
	if live.Direction == workexecution.Recovery {
		if direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD {
			return backupvolumemanifest.Owner{}, nil, invalidVolumeManifestAuthority()
		}
		assignment, captured, loadErr := owner.inputs.LoadRecoveryAssignment(ctx, view)
		if loadErr != nil {
			return backupvolumemanifest.Owner{}, nil, loadErr
		}
		if assignment.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION {
			return backupvolumemanifest.Owner{}, nil, invalidVolumeManifestAuthority()
		}
		change = captured
		binding, err = backupvolumetransfer.BindRecoverySource(assignment, authorization.Identity(), ordinal, direction)
	} else {
		if view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked {
			return backupvolumemanifest.Owner{}, nil, invalidVolumeManifestAuthority()
		}
		assignment, captured, loadErr := owner.inputs.LoadAssignment(ctx, view)
		if loadErr != nil {
			return backupvolumemanifest.Owner{}, nil, loadErr
		}
		change = captured
		binding, err = backupvolumetransfer.Bind(assignment, authorization.Identity(), ordinal, direction)
	}
	if err != nil {
		return backupvolumemanifest.Owner{}, nil, err
	}
	guards = append(guards, change.Conditions...)
	guards = append(guards,
		keyvalue.Condition{Key: workqueue.TaskKey(live.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(live.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(live.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(live.Executor), ModRevision: view.ExecutorRevision})
	return backupvolumemanifest.Owner{Binding: binding}, guards, nil
}

func invalidVolumeManifestAuthority() error {
	return errs.New(errs.KindStateConflict, "Volume manifest execution ownership changed")
}
