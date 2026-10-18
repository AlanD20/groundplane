package checkpointdelivery

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (source *BackupSource) DatabaseConsumerRecovery(ctx context.Context, pointID string) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_DatabaseConsumerRecovery{
			DatabaseConsumerRecovery: &agentpb.BackupDatabaseConsumerRecovery{PointId: pointID}},
	})
}

// Database reports use the source's durable sequence and acknowledged
// predecessor. In particular, dump/apply start must be acknowledged before
// the helper runs; publishing a request alone is not execution permission.
func (source *BackupSource) PostgresContainerObserved(ctx context.Context, value *agentpb.BackupPostgresContainerObserved) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_PostgresContainerObserved{PostgresContainerObserved: value},
	})
}

func (source *BackupSource) PostgresDumpStart(ctx context.Context, value *agentpb.BackupPostgresDumpStart) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_PostgresDumpStart{PostgresDumpStart: value},
	})
}

func (source *BackupSource) PostgresRestoreApplyStart(ctx context.Context, value *agentpb.BackupPostgresRestoreApplyStartCheckpoint) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_PostgresRestoreApplyStart{PostgresRestoreApplyStart: value},
	})
}

func (source *BackupSource) PostgresRestoreVerified(ctx context.Context, value *agentpb.BackupPostgresRestoreVerified) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_PostgresRestoreVerified{PostgresRestoreVerified: value},
	})
}

func (source *BackupSource) PostgresServiceProgress(ctx context.Context, value *agentpb.BackupPostgresServiceProgress) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_PostgresServiceProgress{PostgresServiceProgress: value},
	})
}

func (source *BackupSource) MysqlContainerObserved(ctx context.Context, value *agentpb.BackupMySQLContainerObserved) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_MysqlContainerObserved{MysqlContainerObserved: value},
	})
}

func (source *BackupSource) MysqlDumpStart(ctx context.Context, value *agentpb.BackupMySQLDumpStart) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_MysqlDumpStart{MysqlDumpStart: value},
	})
}

func (source *BackupSource) MysqlRestoreApplyStart(ctx context.Context, value *agentpb.BackupMySQLRestoreApplyStartCheckpoint) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_MysqlRestoreApplyStart{MysqlRestoreApplyStart: value},
	})
}

func (source *BackupSource) MysqlRestoreVerified(ctx context.Context, value *agentpb.BackupMySQLRestoreVerified) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_MysqlRestoreVerified{MysqlRestoreVerified: value},
	})
}

func (source *BackupSource) MysqlServiceProgress(ctx context.Context, value *agentpb.BackupMySQLServiceProgress) error {
	return source.publish(ctx, &agentpb.BackupWorkCheckpointRequest{
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_MysqlServiceProgress{MysqlServiceProgress: value},
	})
}
