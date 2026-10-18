package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/s3compatible"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type SourceExchange interface {
	RequestBackupSecrets(context.Context, *agentpb.WorkAttemptIdentity, uint32) error
	BackupCheckpoint(context.Context, *agentpb.BackupWorkInput, *agentpb.BackupWorkCheckpointRequest) (*agentpb.BackupWorkCheckpointAck, error)
	ConsumeBackupSecret(context.Context, *agentpb.WorkAttemptIdentity, uint32, agentpb.BackupSecretSlotPurpose, func([]byte) error) error
}

// Prune executes captured sources under an already acknowledged start grant.
// It reports exact absence; the Controller owns Recovery Point settlement and
// terminal acknowledgement. Returning nil here is not a terminal Task receipt.
func Prune(ctx context.Context, assignment *agentpb.BackupWorkAssignment, attempt workexecution.Attempt,
	journal *agentworkjournal.Journal, exchange SourceExchange,
) error {
	owned, decoded, err := executionplan.OpenBackupWorkAssignment(assignment, attempt.Executor)
	if err != nil {
		return err
	}
	if ctx == nil || exchange == nil || !workexecution.SameAssignment(decoded, attempt) || attempt.StartedAt == nil ||
		attempt.FinishedAt != nil || owned.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		return errs.New(errs.KindStateConflict, "Backup prune requires its started native attempt")
	}
	delivery, err := checkpointdelivery.NewBackup(owned, attempt, journal, exchange.BackupCheckpoint)
	if err != nil {
		return err
	}
	if err := delivery.Resume(ctx); err != nil {
		return err
	}
	for ordinal, source := range owned.Sources {
		step := owned.Input.Steps[ordinal]
		prune, resume := step.GetPrune(), source.GetPrune()
		if prune == nil || resume == nil || len(prune.Objects) != 1 {
			return errs.New(errs.KindValidationFailed, "Backup prune source must identify one captured object")
		}
		object := prune.Objects[0]
		expected := &agentpb.BackupPruneObjectDeleted{Ordinal: object.Ordinal, PointId: object.PointId, Object: proto.CloneOf(object.Object)}
		if resume.CheckpointSequence == 1 {
			if !proto.Equal(resume.GetObjectDeleted(), expected) {
				return errs.New(errs.KindStateConflict, "Backup prune resume identifies another object")
			}
			continue
		}
		local, _, err := delivery.Current(ctx, uint32(ordinal))
		if err != nil {
			return err
		}
		if local != nil {
			if local.CheckpointSequence != 1 || !proto.Equal(local.GetPruneObjectDeleted(), expected) {
				return errs.New(errs.KindStateConflict, "Backup prune local receipt identifies another object")
			}
			continue
		}
		deadline := time.Unix(0, int64(step.StepDeadlineUnixNano))
		if attempt.Deadline.Before(deadline) {
			deadline = attempt.Deadline
		}
		sourceCtx, cancel := context.WithDeadline(ctx, deadline)
		err = pruneObject(sourceCtx, owned, uint32(ordinal), object, exchange)
		cancel()
		if err != nil {
			return err
		}
		request := &agentpb.BackupWorkCheckpointRequest{Attempt: proto.CloneOf(owned.Attempt), SourceOrdinal: uint32(ordinal),
			CheckpointSequence: 1, Checkpoint: &agentpb.BackupWorkCheckpointRequest_PruneObjectDeleted{PruneObjectDeleted: expected}}
		// Save/report an observed effect even if its execution deadline just
		// expired. The Controller decides settlement; never delete it again.
		reportCtx, reportCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		reportErr := delivery.Checkpoint(reportCtx, request)
		reportCancel()
		if reportErr != nil {
			return reportErr
		}
	}
	return nil
}

func pruneObject(ctx context.Context, assignment *agentpb.BackupWorkAssignment, ordinal uint32,
	object *agentpb.BackupPruneObject, exchange SourceExchange,
) error {
	authority, err := executionplan.BackupPruneObjectAuthority(assignment.Input.EnvironmentId, object)
	if err != nil {
		return err
	}
	connector := object.Object.Connector
	if err := exchange.RequestBackupSecrets(ctx, assignment.Attempt, ordinal); err != nil {
		return err
	}
	return exchange.ConsumeBackupSecret(ctx, assignment.Attempt, ordinal,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, func(accessKey []byte) error {
			return exchange.ConsumeBackupSecret(ctx, assignment.Attempt, ordinal,
				agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, func(secretKey []byte) error {
					adapter, err := s3compatible.New(s3compatible.Config{Endpoint: connector.CanonicalEndpointUrl,
						Bucket: object.Object.Bucket, Prefix: connector.Prefix, Region: connector.Region,
						PathStyle: connector.GetPathStyle(), AccessKey: string(accessKey), SecretKey: string(secretKey)})
					if err != nil {
						return err
					}
					// PruneExact first observes the exact version/ETag. Already
					// absent is success; a different object is never deleted.
					return adapter.PruneExact(ctx, authority)
				})
		})
}
