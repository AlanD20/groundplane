package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// backupPolicyInitialKey is an application-sealed era-1 age identity. The
// repository treats the ciphertext as opaque and never receives plaintext.
type backupPolicyInitialKey struct {
	Record    backuppolicy.BackupKeyRecord
	Encrypted backuppolicy.BackupKeyEncryptedValue
}

// backupPolicySourceEvidence binds one ordered policy source to the exact
// durable target revision that was validated by the application.
type backupPolicySourceEvidence struct {
	Source           etcdstore.Versioned[backuppolicy.BackupSourceRecord]
	EnvironmentIndex *etcdstore.KeyValue
	IdentityIndex    *etcdstore.KeyValue
	Attach           *etcdstore.Versioned[attachrecord.Record]
	Volume           *backupVolumeProjectionEvidence
	TargetOwnerIndex *etcdstore.KeyValue
}

// backupPolicyConnectorReferenceEvidence proves either the exact existing
// reverse reference or its absence before the replacement transaction.
type backupPolicyConnectorReferenceEvidence struct {
	ConnectorID string
	Entry       *etcdstore.KeyValue
}

// backupPolicyReplacementCandidate contains the fully resolved, prevalidated
// durable evidence for one protected Environment-scoped replacement.
type backupPolicyReplacementCandidate struct {
	Environment         etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	Project             etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	MutationEpoch       etcdstore.Versioned[backupruntime.EnvironmentMutationEpochRecord]
	Coordination        etcdstore.Versioned[EnvironmentCoordinationRecord]
	NextCoordination    EnvironmentCoordinationRecord
	NextRunAt           time.Time
	ScheduleSealed      bool
	Current             *etcdstore.Versioned[backuppolicy.BackupPolicyRecord]
	Replacement         backuppolicy.BackupPolicyRecord
	Sources             []backupPolicySourceEvidence
	Connector           *etcdstore.Versioned[connectorrecord.Record]
	ConnectorOwnerIndex *etcdstore.KeyValue
	ConnectorReferences []backupPolicyConnectorReferenceEvidence
	ExistingKey         *backuppolicy.VersionedBackupKey
	InitialKey          *backupPolicyInitialKey
}

type backupPolicyReplacementCompareKind uint8

const (
	backupPolicyComparePolicy backupPolicyReplacementCompareKind = iota + 1
	backupPolicyCompareEnvironment
	backupPolicyCompareProject
	backupPolicyCompareCoordination
	backupPolicyCompareOperationLock
	backupPolicyCompareHierarchyTombstone
	backupPolicyCompareSource
	backupPolicyCompareSourceEnvironmentIndex
	backupPolicyCompareSourceIdentityIndex
	backupPolicyCompareAttach
	backupPolicyCompareVolume
	backupPolicyCompareVolumeRoot
	backupPolicyCompareTargetOwnerIndex
	backupPolicyCompareTargetTombstone
	backupPolicyCompareConnector
	backupPolicyCompareConnectorOwnerIndex
	backupPolicyCompareConnectorTombstone
	backupPolicyCompareConnectorReference
	backupPolicyCompareKey
)

const backupPolicyReplacementRoute = "/environments/{id}/backup-policy"

type backupPolicyReplacementCompare struct {
	Kind             backupPolicyReplacementCompareKind
	ID               string
	ExpectedRevision int64
}

type backupPolicyReplacementPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	evidence   []backupPolicyReplacementCompare
}

func (plan *backupPolicyReplacementPlan) compare(
	kind backupPolicyReplacementCompareKind,
	id string,
	key string,
	revision int64,
) {
	plan.conditions = append(plan.conditions, etcdstore.Condition{Key: key, ModRevision: revision})
	plan.evidence = append(plan.evidence, backupPolicyReplacementCompare{
		Kind: kind, ID: id, ExpectedRevision: revision,
	})
}

// replaceBackupPolicyProtected atomically replaces the Environment singleton,
// swaps Connector reverse references, creates an optional sealed era-1 key,
// and commits exact completed-direct replay evidence.
func (repository *BackupPolicyRepository) replaceBackupPolicyProtected(
	ctx context.Context,
	candidate backupPolicyReplacementCandidate,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateBackupPolicyReplacement(ctx, candidate, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := prepareBackupPolicyReplacement(candidate)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(plan.mutations)
	if backupPolicyReplacementOperationCount(plan, marker) > etcdstore.MaximumOperations {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup policy replacement exceeds the atomic transaction limit",
		)
	}
	mutationPlan, err := newIdempotencyMutationPlan(
		plan.conditions,
		plan.mutations,
		func(_ int64, values []*etcdstore.KeyValue) error {
			return classifyBackupPolicyReplacementConflict(values, plan.evidence)
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, mutationPlan)
}
