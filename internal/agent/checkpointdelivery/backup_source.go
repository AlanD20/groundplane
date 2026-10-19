package checkpointdelivery

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupSource binds resource reports to one captured source. Callers supply
// typed evidence only; this owner obtains sequence and predecessor from durable
// acknowledgements, never from a local increment or an unacknowledged send.
type BackupSource struct {
	delivery *Backup
	recovery *BackupRestoreRecovery
	ordinal  uint32
}

func (delivery *Backup) Source(ordinal uint32) (*BackupSource, error) {
	if delivery == nil || uint64(ordinal) >= uint64(len(delivery.assignment.Sources)) {
		return nil, errs.New(errs.KindValidationFailed, "Backup source is not captured")
	}
	return &BackupSource{delivery: delivery, ordinal: ordinal}, nil
}

// Current returns the cumulative acknowledged cursor, including prepared
// artifact evidence retained through later upload checkpoints. It does not
// infer success from an unacknowledged command report.
func (source *BackupSource) Current(ctx context.Context) (*agentpb.BackupStepResume, error) {
	if source != nil && source.recovery != nil {
		return source.recovery.Current(ctx)
	}
	if source == nil || source.delivery == nil {
		return nil, errs.New(errs.KindStateConflict, "Backup source delivery is unavailable")
	}
	_, ack, err := source.delivery.Current(ctx, source.ordinal)
	if err != nil {
		return nil, err
	}
	if ack != nil {
		return proto.CloneOf(ack.Source), nil
	}
	return proto.CloneOf(source.delivery.assignment.Sources[source.ordinal]), nil
}

func (source *BackupSource) ArtifactPrepared(ctx context.Context, value *agentpb.BackupArtifactPrepared) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_ArtifactPrepared{ArtifactPrepared: value},
	})
}

func (source *BackupSource) UploadCompleted(ctx context.Context, value *agentpb.BackupUploadCompleted) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_UploadCompleted{UploadCompleted: value},
	})
}

func (source *BackupSource) UploadVerified(ctx context.Context, value *agentpb.BackupUploadVerified) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_UploadVerified{UploadVerified: value},
	})
}

func (source *BackupSource) SourceCleanupCompleted(
	ctx context.Context,
	value *agentpb.BackupSourceCleanupCompleted,
) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_SourceCleanupCompleted{SourceCleanupCompleted: value},
	})
}

func (source *BackupSource) Config(ctx context.Context, value *agentpb.BackupConfigCheckpoint) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_Config{Config: value},
	})
}

func (source *BackupSource) Volume(ctx context.Context, value *agentpb.BackupVolumeCheckpoint) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_Volume{Volume: value},
	})
}

func (source *BackupSource) RestoreArtifactValidated(
	ctx context.Context,
	value *agentpb.BackupRestoreArtifactValidated,
) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_RestoreArtifactValidated{RestoreArtifactValidated: value},
	})
}

func (source *BackupSource) publish(ctx context.Context, payload *agentpb.BackupWorkCheckpointRequest) error {
	if source != nil && source.recovery != nil {
		return source.recovery.publish(ctx, payload)
	}
	if source == nil || source.delivery == nil {
		return errs.New(errs.KindStateConflict, "Backup source delivery is unavailable")
	}
	delivery := source.delivery
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	attempt, record, err := delivery.journal.LoadBackup(ctx, delivery.assignment, delivery.attempt.Executor)
	if err != nil {
		return err
	}
	if record == nil || record.Completion != nil || record.FailureReason != "" || attempt.StartedAt == nil ||
		!attempt.StartedAt.Equal(*delivery.attempt.StartedAt) || uint64(source.ordinal) >= uint64(len(record.Sources)) {
		return errs.New(errs.KindStateConflict, "Backup source requires its active durable start")
	}
	var sequence uint64
	var fence *agentpb.CheckpointFence
	captured := delivery.assignment.Sources[source.ordinal]
	switch {
	case captured.GetCapture() != nil:
		sequence, fence = captured.GetCapture().CheckpointSequence, captured.GetCapture().PrecedingCheckpoint
	case captured.GetRestore() != nil:
		sequence, fence = captured.GetRestore().CheckpointSequence, captured.GetRestore().PrecedingCheckpoint
	case captured.GetPrune() != nil:
		sequence, fence = captured.GetPrune().CheckpointSequence, captured.GetPrune().PrecedingCheckpoint
	default:
		return errs.New(errs.KindStateConflict, "Backup source cursor is unavailable")
	}
	local := record.Sources[source.ordinal]
	if len(local.Request) != 0 {
		ack, err := local.Ack()
		if err != nil {
			return err
		}
		if ack == nil {
			// Resume must deliver the exact saved request before execution
			// continues. Do not replace it with another payload or guess a
			// predecessor because a network send may already have committed.
			return errs.New(errs.KindStateConflict, "Backup source has an unacknowledged report; resume delivery first")
		}
		sequence, fence = ack.CheckpointSequence, ack.Committed
	}
	if sequence == ^uint64(0) {
		return errs.New(errs.KindStateConflict, "Backup source checkpoint sequence is exhausted")
	}
	request := proto.CloneOf(payload)
	request.Attempt = proto.CloneOf(delivery.assignment.Attempt)
	request.SourceOrdinal = source.ordinal
	request.CheckpointSequence = sequence + 1
	request.PrecedingCheckpoint = proto.CloneOf(fence)
	next, err := delivery.journal.PrepareBackup(ctx, delivery.assignment, delivery.attempt, request, time.Now().UTC())
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, next)
}
