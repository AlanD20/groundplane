package backupsecrets

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

// BackupSecretValueEvidence is an encrypted Secret value selected by the
// project-first/platform-fallback lookup. Ciphertext is caller-owned and must
// be cleared after the controller has opened it.
type SecretValueEvidence struct {
	Name      backupsecret.CredentialName
	Reference string
	Value     secretrecord.EncryptedValue
}

// BackupSecretResolutionEvidence contains only the durable proof and
// encrypted values required by the Controller resolver. No plaintext is
// returned; the current age identity remains Controller-key-wrapped.
type Evidence struct {
	ReadRevision        int64
	Environment         hierarchyrecord.EnvironmentRecord
	EnvironmentRevision int64
	Project             hierarchyrecord.ProjectRecord
	ProjectRevision     int64
	Connector           connectorrecord.Record
	ConnectorRevision   int64
	ConnectorAuthority  *agentpb.BackupConnectorAuthority
	Run                 *backupruntime.BackupRunRecord
	Restore             *backupruntime.BackupRestoreRecord
	Dispatch            *backupruntime.BackupRecoveryPointPruneDispatchRecord
	DispatchRevision    int64
	Point               *backupruntime.BackupRecoveryPointRecord
	Prune               *backupruntime.BackupRecoveryPointPruneRecord
	Source              backuppolicy.BackupSourceRecord
	SourceRevision      int64
	Credentials         connectorrecord.EncryptedCredentials
	HasCredentials      bool
	SecretValues        []SecretValueEvidence
	CurrentAgeIdentity  *CurrentAgeIdentityEvidence
}

// Clear releases every encrypted buffer returned by the reader. It is safe
// to call more than once and is intentionally explicit for callers that take
// ownership before returning an error.
func (e *Evidence) Clear() {
	if e == nil {
		return
	}
	clear(e.Credentials.Ciphertext)
	e.Credentials.Ciphertext = nil
	for index := range e.SecretValues {
		clear(e.SecretValues[index].Value.Ciphertext)
		e.SecretValues[index].Value.Ciphertext = nil
	}
	e.SecretValues = nil
	if e.CurrentAgeIdentity != nil {
		clear(e.CurrentAgeIdentity.Encrypted.Ciphertext)
		e.CurrentAgeIdentity.Encrypted.Ciphertext = nil
		e.CurrentAgeIdentity = nil
	}
}

// BackupSecretResolutionReader reads the exact durable evidence needed by a
// backup capture or prune secret delivery. It is deliberately narrower than
// the runtime repositories so channel composition can depend on one read
// contract without acquiring lifecycle authority.
type Reader struct {
	store  backupSecretResolutionStore
	inputs *backupwork.InputRepository
	now    func() time.Time
}

type backupSecretResolutionStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func NewReader(store backupwork.InputReader) (*Reader, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup secret resolution store is required")
	}
	inputs, err := backupwork.NewInputRepository(store)
	if err != nil {
		return nil, err
	}
	return &Reader{store: store, inputs: inputs, now: time.Now}, nil
}
