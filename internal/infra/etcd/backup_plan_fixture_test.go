package etcd

import "github.com/AlanD20/groundplane/proto/agentpb"

func backupFixtureAddressing(pathStyle bool) agentpb.BackupS3Addressing {
	if pathStyle {
		return agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE
	}
	return agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_VIRTUAL_HOSTED_STYLE
}
