package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Wire the native Backup owners and their captured-input transfer boundaries.
func (platform *Platform) newBackupWork(queue *workqueue.Repository, dependencies ExecutionDependencies) (*backup.AgentWork, *backupkey.Work, error) {
	inputs, err := backupwork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	manifests := backupvolumemanifest.NewRepository(platform.dependencies.Store, inputs.VolumeTransferGuard(queue))
	snapshots, err := backup.NewConfigSnapshotProducer(platform.dependencies.Store, platform.dependencies.Protector)
	if err != nil {
		return nil, nil, err
	}
	runtime, err := etcd.NewBackupRuntimeRepository(platform.dependencies.Store)
	if err != nil {
		return nil, nil, err
	}
	rotations, err := backupkey.NewWork(dependencies.Keys, queue)
	if err != nil {
		return nil, nil, err
	}
	restores, err := backup.NewConfigRestoreProducer(platform.dependencies.Store, runtime, platform.dependencies.Protector)
	if err != nil {
		return nil, nil, err
	}
	reports, err := backup.NewAgentWork(inputs, queue, manifests, snapshots, runtime, restores)
	if err != nil {
		return nil, nil, err
	}
	if err := platform.dependencies.Channel.ConfigureBackupWork(reports); err != nil {
		return nil, nil, err
	}
	credentials, err := backup.NewWorkSecretDelivery(dependencies.BackupSecrets, queue)
	if err != nil {
		return nil, nil, err
	}
	if err := platform.dependencies.Channel.ConfigureBackupWorkSecrets(credentials); err != nil {
		return nil, nil, err
	}
	return reports, rotations, nil
}
