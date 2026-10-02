package backupsecrets

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func backupSecretRecordMatches(value *etcdstore.KeyValue, authority *agentpb.RevisionDigest) bool {
	if value == nil || authority == nil || authority.ModRevision <= 0 || value.ModRevision != authority.ModRevision {
		return false
	}
	digest := sha256.Sum256(value.Value)
	return bytes.Equal(digest[:], authority.Sha256)
}

func backupCredentialSlot(
	authority *agentpb.BackupConnectorAuthority,
	name backupsecret.CredentialName,
) *agentpb.RevisionDigest {
	switch name {
	case backupsecret.CredentialAccessKey:
		return authority.GetAccessKeySlot()
	case backupsecret.CredentialSecretKey:
		return authority.GetSecretKeySlot()
	default:
		return nil
	}
}
