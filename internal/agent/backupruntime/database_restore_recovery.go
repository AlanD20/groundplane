package backupruntime

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (executor *Executor) continueDatabaseRestore(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, source executionplan.BackupWorkRecovery, ownership *agentownership.Lease,
	staging *Staging, publisher *checkpointdelivery.BackupSource,
) (*agentpb.BackupStepResume, error) {
	if staging == nil || publisher == nil || ownership == nil {
		return nil, invalidDatabaseStaging()
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return nil, err
	}
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return nil, err
	}
	step := assignment.Input.Steps[0]
	progress := cursor.GetRestore().GetDatabaseProgress()
	if progress == nil || (!progress.ConsumersOnly && progress.PostgresVerified == nil && progress.MysqlVerified == nil) {
		switch {
		case progress != nil && cursor.GetRestore().ApplyStart == nil && cursor.GetRestore().MysqlApplyStart == nil:
			err = publisher.DatabaseConsumerRecovery(ctx, step.GetRestore().PointId)
		case step.GetRestore().GetPostgres() != nil && cursor.GetRestore().ApplyStart != nil:
			err = provePostgresRecovery(ctx, assignment, cursor.GetRestore(), publisher)
		case step.GetRestore().GetMysql() != nil && cursor.GetRestore().MysqlApplyStart != nil:
			err = proveMySQLRecovery(ctx, assignment, cursor.GetRestore(), publisher)
		default:
			return nil, errs.New(errs.KindStateConflict, "Database Restore recovery requires the original apply receipt before verifying its effects")
		}
		if err != nil {
			return nil, err
		}
		cursor, err = publisher.Current(ctx)
		if err != nil {
			return nil, err
		}
	}
	host, err := NewBackupConsumerHost(executor.journal, executor.helper, attempt.Executor, executor.ownership)
	if err != nil {
		return nil, err
	}
	commands := databaseRecoveryCommands{
		resumeIntent: func(ctx context.Context, value *agentpb.BackupPostgresServiceProgress) error {
			var digest [sha256.Size]byte
			if len(value.ObservationSha256) != len(digest) {
				return invalidDatabaseStaging()
			}
			copy(digest[:], value.ObservationSha256)
			return publishDatabaseService(ctx, step, publisher, value.ServiceCursor, value.ServiceId, value.Phase, digest)
		},
		observe: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return host.RunRecovery(ctx, assignment, 0, serviceID, agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_UNSPECIFIED, true)
		},
		recover: func(ctx context.Context, serviceID string) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
			return host.RunRecovery(ctx, assignment, 0, serviceID, agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_RECOVER, false)
		},
	}
	if err := recoverDatabaseConsumerSet(ctx, assignment.Input, 0, cursor.GetRestore(), publisher, commands); err != nil {
		return nil, err
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return nil, err
	}
	if err := retireSettledSourceHelpers(ctx, assignment.Input, step, !cursor.GetRestore().GetDatabaseProgress().GetConsumersOnly()); err != nil {
		return nil, err
	}
	if err := staging.cleanupCapturedSource(ctx, backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: step.GetRestore().PointId}); err != nil {
		return nil, err
	}
	if err := publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{
		PointId: step.GetRestore().PointId, Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence),
	}); err != nil {
		return nil, err
	}
	return publisher.Current(ctx)
}
