package backup

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicefactauthority"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateBackupBackingCatalog(
	value *etcdstore.KeyValue,
	applied servicefactauthority.Applied,
	input backupplanning.BackupServiceFactInput,
) error {
	if value != nil {
		digest := sha256.Sum256(value.Value)
		record, err := backingpostgresrelease.Decode(value.Value)
		if err == nil && record.EnvironmentID == input.EnvironmentID && record.ServiceID == input.ServiceID &&
			hex.EncodeToString(digest[:]) == applied.ManagedReleaseSHA256 &&
			record.Release.Image == applied.Workload.ImageReference && record.Release.ContainsRuntimeImageID(applied.LocalImageID) {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "backup Backing catalog differs from its acknowledged runtime")
}
