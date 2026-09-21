package backuppolicymutations

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

// backupPolicyInitialKey is an application-sealed era-1 age identity. The
// repository treats the ciphertext as opaque and never receives plaintext.
type InitialKey struct {
	Record    backuppolicy.BackupKeyRecord
	Encrypted backuppolicy.BackupKeyEncryptedValue
}

// backupPolicySourceEvidence binds one ordered policy source to the exact
// durable target revision that was validated by the application.
type SourceEvidence struct {
	Source           etcdstore.Versioned[backuppolicy.BackupSourceRecord]
	EnvironmentIndex *etcdstore.KeyValue
	IdentityIndex    *etcdstore.KeyValue
	Attach           *etcdstore.Versioned[attachrecord.Record]
	Volume           *environmentqueries.BackupVolumeProjectionEvidence
	TargetOwnerIndex *etcdstore.KeyValue
}

// backupPolicyConnectorReferenceEvidence proves either the exact existing
// reverse reference or its absence before the replacement transaction.
type ConnectorReferenceEvidence struct {
	ConnectorID string
	Entry       *etcdstore.KeyValue
}

// backupPolicyReplacementCandidate contains the fully resolved, prevalidated
// durable evidence for one protected Environment-scoped replacement.
type ReplacementCandidate struct {
	Environment         etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Project             etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	MutationEpoch       etcdstore.Versioned[backupruntime.EnvironmentMutationEpochRecord]
	Coordination        etcdstore.Versioned[coordinationrecord.EnvironmentCoordinationRecord]
	NextCoordination    coordinationrecord.EnvironmentCoordinationRecord
	NextRunAt           time.Time
	ScheduleSealed      bool
	Current             *etcdstore.Versioned[backuppolicy.BackupPolicyRecord]
	Replacement         backuppolicy.BackupPolicyRecord
	Sources             []SourceEvidence
	Connector           *etcdstore.Versioned[connectorrecord.Record]
	ConnectorOwnerIndex *etcdstore.KeyValue
	ConnectorReferences []ConnectorReferenceEvidence
	ExistingKey         *backuppolicy.VersionedBackupKey
	InitialKey          *InitialKey
}

type BackupPolicyReplacementCompareKind uint8

const (
	BackupPolicyComparePolicy BackupPolicyReplacementCompareKind = iota + 1
	backupPolicyCompareEnvironment
	backupPolicyCompareProject
	BackupPolicyCompareCoordination
	backupPolicyCompareOperationLock
	backupPolicyCompareHierarchyTombstone
	BackupPolicyCompareSource
	BackupPolicyCompareSourceEnvironmentIndex
	BackupPolicyCompareSourceIdentityIndex
	BackupPolicyCompareAttach
	backupPolicyCompareVolume
	backupPolicyCompareVolumeRoot
	BackupPolicyCompareTargetOwnerIndex
	BackupPolicyCompareTargetTombstone
	BackupPolicyCompareConnector
	BackupPolicyCompareConnectorOwnerIndex
	BackupPolicyCompareConnectorTombstone
	BackupPolicyCompareConnectorReference
	BackupPolicyCompareKey
)

const backupPolicyReplacementRoute = "/environments/{id}/backup-policy"

type BackupPolicyReplacementCompare struct {
	Kind             BackupPolicyReplacementCompareKind
	ID               string
	ExpectedRevision int64
}

type backupPolicyReplacementPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	evidence   []BackupPolicyReplacementCompare
}

func (plan *backupPolicyReplacementPlan) compare(
	kind BackupPolicyReplacementCompareKind,
	id string,
	key string,
	revision int64,
) {
	plan.conditions = append(plan.conditions, etcdstore.Condition{Key: key, ModRevision: revision})
	plan.evidence = append(plan.evidence, BackupPolicyReplacementCompare{
		Kind: kind, ID: id, ExpectedRevision: revision,
	})
}
