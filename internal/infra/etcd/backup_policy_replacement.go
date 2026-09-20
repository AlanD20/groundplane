package etcd

import (
	"context"
	"encoding/json"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
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
	MutationEpoch       etcdstore.Versioned[EnvironmentMutationEpochRecord]
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
	ExistingKey         *VersionedBackupKey
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

func validateBackupPolicyReplacement(
	ctx context.Context,
	candidate backupPolicyReplacementCandidate,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if err := validatebackupPolicyReplacementCandidate(ctx, candidate); err != nil {
		return err
	}
	return validateBackupPolicyReplacementMarker(candidate, marker)
}

func validateBackupPolicyReplacementMarker(
	candidate backupPolicyReplacementCandidate,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != candidate.Replacement.EnvironmentID ||
		marker.Locator.Method != http.MethodPut || marker.Locator.Route != backupPolicyReplacementRoute ||
		marker.ReplayTarget != nil || marker.Response.Status != http.StatusOK ||
		marker.Response.ContentKind != "application/json" || !json.Valid(marker.Response.Body) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || !marker.TerminalAt.Equal(marker.CreatedAt) ||
		!marker.RetainUntil.Equal(marker.TerminalAt.Add(idempotencyrecord.MarkerRetention)) {
		return errs.New(
			errs.KindValidationFailed,
			"backup policy marker must be the retained completed response for its exact put operation",
		)
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func validatebackupPolicyReplacementCandidate(
	ctx context.Context,
	candidate backupPolicyReplacementCandidate,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(candidate.Environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(candidate.Project.Record); err != nil {
		return err
	}
	if err := validateEnvironmentMutationEpochRecord(candidate.MutationEpoch.Record); err != nil {
		return err
	}
	if err := backuppolicy.ValidateBackupPolicyRecord(candidate.Replacement); err != nil {
		return err
	}
	if !validReplacementRevision(candidate.Environment.Revision, candidate.Environment.ReadRevision) ||
		!(validReplacementRevision(candidate.Coordination.Revision, candidate.Coordination.ReadRevision) ||
			(candidate.Coordination.Revision == 0 && candidate.Current == nil &&
				candidate.Coordination.ReadRevision > 0)) ||
		!candidate.ScheduleSealed ||
		!validReplacementRevision(candidate.Project.Revision, candidate.Project.ReadRevision) ||
		!validReplacementRevision(candidate.MutationEpoch.Revision, candidate.MutationEpoch.ReadRevision) ||
		candidate.Replacement.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.MutationEpoch.Record.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.Coordination.Record.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.NextCoordination.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.Environment.Record.ProjectID != candidate.Project.Record.ID ||
		candidate.Project.Record.Kind != hierarchyrecord.ProjectKindTenant || candidate.Project.Record.TenantID == "" {
		return errs.New(errs.KindValidationFailed, "backup policy hierarchy is invalid")
	}
	if candidate.Environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return errs.New(errs.KindStateConflict, "environment is not ready for backup policy replacement")
	}
	if !equalEnvironmentCoordinationRecord(candidate.NextCoordination, mustBackupPolicyScheduleTransition(candidate)) {
		return errs.New(errs.KindValidationFailed, "backup policy schedule transition is invalid")
	}
	if candidate.Replacement.Enabled {
		if err := validateBackupPolicyFrequency(candidate.Replacement.Frequency); err != nil {
			return err
		}
	}
	if candidate.Current != nil {
		if err := backuppolicy.ValidateBackupPolicyRecord(candidate.Current.Record); err != nil {
			return err
		}
		if !validReplacementRevision(candidate.Current.Revision, candidate.Current.ReadRevision) ||
			candidate.Current.Record.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "current backup policy evidence is invalid")
		}
	}
	if len(candidate.Sources) != len(candidate.Replacement.SourceIDs) {
		return errs.New(errs.KindValidationFailed, "backup policy source evidence is incomplete")
	}
	type sourceIdentity struct {
		kind     string
		targetID string
	}
	identities := make(map[sourceIdentity]struct{}, len(candidate.Sources))
	for index := range candidate.Sources {
		source := candidate.Sources[index].Source
		if err := backuppolicy.ValidateBackupSourceRecord(source.Record); err != nil {
			return err
		}
		if !validReplacementRevision(source.Revision, source.ReadRevision) ||
			source.Record.ID != candidate.Replacement.SourceIDs[index] ||
			source.Record.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "backup policy source evidence is invalid")
		}
		identity := sourceIdentity{kind: string(source.Record.Kind), targetID: source.Record.TargetID}
		if _, duplicate := identities[identity]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup policy source identities must be unique")
		}
		identities[identity] = struct{}{}
	}
	for index := range candidate.Sources {
		if err := validatebackupPolicySourceEvidence(
			candidate.Replacement.EnvironmentID,
			candidate.Replacement.SourceIDs[index],
			candidate.Sources[index],
		); err != nil {
			return err
		}
	}
	if candidate.Replacement.Enabled {
		if candidate.Connector == nil {
			return errs.New(errs.KindValidationFailed, "enabled backup policy requires connector evidence")
		}
		if err := connectorrecord.ValidateRecord(candidate.Connector.Record); err != nil {
			return err
		}
		if !validReplacementRevision(candidate.Connector.Revision, candidate.Connector.ReadRevision) ||
			candidate.Connector.Record.Connector.ID != candidate.Replacement.ConnectorID ||
			candidate.Connector.Record.Connector.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "backup policy connector evidence is invalid")
		}
		if !validBackupPolicyIndex(
			candidate.ConnectorOwnerIndex,
			connectorEnvironmentKey(
				candidate.Replacement.EnvironmentID,
				candidate.Replacement.ConnectorID,
			),
			candidate.Replacement.ConnectorID,
		) {
			return recordcodec.CorruptRecord()
		}
	} else if candidate.Connector != nil || candidate.ConnectorOwnerIndex != nil {
		return errs.New(errs.KindValidationFailed, "disabled backup policy cannot carry connector evidence")
	}
	if err := validateBackupPolicyKeyEvidence(candidate); err != nil {
		return err
	}
	return validateBackupPolicyConnectorReferences(candidate)
}

func mustBackupPolicyScheduleTransition(
	candidate backupPolicyReplacementCandidate,
) EnvironmentCoordinationRecord {
	next, _, err := replaceEnvironmentCoordinationSchedule(
		candidate.Coordination.Record, candidate.Replacement, candidate.Replacement.UpdatedAt,
	)
	if err != nil {
		return EnvironmentCoordinationRecord{}
	}
	return next
}

func validateBackupPolicyFrequency(frequency string) error {
	calendar := frequency
	if len(frequency) == 18 {
		if frequency[3] != ' ' || !validBackupPolicyWeekday(frequency[:3]) {
			return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
		}
		calendar = frequency[4:]
	}
	if len(calendar) != 14 || calendar[:6] != "*-*-* " {
		return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
	}
	clock := calendar[6:]
	if clock[2] != ':' || clock[5] != ':' ||
		!twoASCIIDigits(clock[0], clock[1], 23) ||
		!twoASCIIDigits(clock[3], clock[4], 59) ||
		!twoASCIIDigits(clock[6], clock[7], 59) {
		return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
	}
	return nil
}

func validBackupPolicyWeekday(value string) bool {
	switch value {
	case "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun":
		return true
	default:
		return false
	}
}

func twoASCIIDigits(tens byte, ones byte, maximum int) bool {
	if tens < '0' || tens > '9' || ones < '0' || ones > '9' {
		return false
	}
	return int(tens-'0')*10+int(ones-'0') <= maximum
}

func validReplacementRevision(revision int64, readRevision int64) bool {
	return revision > 0 && readRevision >= revision
}

func validatebackupPolicySourceEvidence(
	environmentID string,
	wantSourceID string,
	evidence backupPolicySourceEvidence,
) error {
	if err := backuppolicy.ValidateBackupSourceRecord(evidence.Source.Record); err != nil {
		return err
	}
	if !validReplacementRevision(evidence.Source.Revision, evidence.Source.ReadRevision) ||
		evidence.Source.Record.ID != wantSourceID || evidence.Source.Record.EnvironmentID != environmentID {
		return errs.New(errs.KindValidationFailed, "backup policy source evidence is invalid")
	}
	if !validBackupPolicyIndex(
		evidence.EnvironmentIndex,
		backuppolicy.BackupSourceEnvironmentKey(environmentID, evidence.Source.Record.ID),
		evidence.Source.Record.ID,
	) || !validBackupPolicyIndex(
		evidence.IdentityIndex,
		backuppolicy.BackupSourceIdentityKey(environmentID, evidence.Source.Record.Kind, evidence.Source.Record.TargetID),
		evidence.Source.Record.ID,
	) {
		return recordcodec.CorruptRecord()
	}
	switch string(evidence.Source.Record.Kind) {
	case "config":
		if evidence.Source.Record.TargetID != environmentID || evidence.Attach != nil ||
			evidence.Volume != nil || evidence.TargetOwnerIndex != nil {
			return errs.New(errs.KindValidationFailed, "config backup source cannot carry target evidence")
		}
	case "attach":
		if evidence.Attach == nil || evidence.Volume != nil ||
			attachrecord.ValidateAttachRecord(evidence.Attach.Record) != nil ||
			!validReplacementRevision(evidence.Attach.Revision, evidence.Attach.ReadRevision) ||
			evidence.Attach.Record.ID != evidence.Source.Record.TargetID ||
			!evidence.Attach.Record.OwnsCredential() ||
			evidence.Attach.Record.EnvironmentID != environmentID ||
			!validBackupPolicyIndex(
				evidence.TargetOwnerIndex,
				attachrecord.AttachOwnerKey(environmentID, evidence.Attach.Record.ID),
				evidence.Attach.Record.ID,
			) {
			return errs.New(errs.KindValidationFailed, "attach backup source evidence is invalid")
		}
	case "volume":
		if evidence.Volume == nil || evidence.Attach != nil ||
			evidence.TargetOwnerIndex != nil ||
			!validReplacementRevision(evidence.Volume.Projection.Revision, evidence.Volume.Projection.ReadRevision) ||
			evidence.Volume.ProjectionRoot <= 0 ||
			evidence.Volume.Volume.ID != evidence.Source.Record.TargetID ||
			evidence.Volume.Projection.Record.EnvironmentID != environmentID ||
			evidence.Volume.Projection.Record.RevisionID == "" ||
			!recordcodec.ValidSHA256(evidence.Volume.DependencyDigest) {
			return errs.New(errs.KindValidationFailed, "volume backup source evidence is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup policy source kind is invalid")
	}
	return nil
}

func validBackupPolicyIndex(entry *etcdstore.KeyValue, key string, value string) bool {
	return entry != nil && entry.Key == key && entry.ModRevision > 0 && string(entry.Value) == value
}

func validateBackupPolicyKeyEvidence(candidate backupPolicyReplacementCandidate) error {
	if candidate.ExistingKey != nil {
		if err := validateVersionedBackupKey(*candidate.ExistingKey); err != nil {
			return err
		}
		if candidate.ExistingKey.Record.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "backup policy key owner is invalid")
		}
	}
	wantsInitialKey := candidate.Replacement.Enabled && candidate.Replacement.Encryption == "age" &&
		candidate.ExistingKey == nil
	if wantsInitialKey != (candidate.InitialKey != nil) {
		return errs.New(errs.KindValidationFailed, "backup policy initial key evidence is invalid")
	}
	if candidate.InitialKey == nil {
		return nil
	}
	if err := backuppolicy.ValidateBackupKeyRecord(candidate.InitialKey.Record); err != nil {
		return err
	}
	if err := backuppolicy.ValidateBackupKeyEncryptedValue(candidate.InitialKey.Encrypted); err != nil {
		return err
	}
	if candidate.InitialKey.Record.EnvironmentID != candidate.Replacement.EnvironmentID ||
		candidate.InitialKey.Encrypted.EnvironmentID != candidate.Replacement.EnvironmentID ||
		candidate.InitialKey.Record.KeyEra != 1 || candidate.InitialKey.Encrypted.KeyEra != 1 ||
		!candidate.InitialKey.Record.CreatedAt.Equal(candidate.InitialKey.Record.RotatedAt) {
		return errs.New(errs.KindValidationFailed, "backup policy initial key must be one sealed era-1 pair")
	}
	return nil
}

func validateBackupPolicyConnectorReferences(candidate backupPolicyReplacementCandidate) error {
	environmentID := candidate.Replacement.EnvironmentID
	oldConnectorID := ""
	if candidate.Current != nil && candidate.Current.Record.Enabled {
		oldConnectorID = candidate.Current.Record.ConnectorID
	}
	newConnectorID := ""
	if candidate.Replacement.Enabled {
		newConnectorID = candidate.Replacement.ConnectorID
	}
	expected := make([]struct {
		connectorID string
		present     bool
	}, 0, 2)
	if oldConnectorID != "" {
		expected = append(expected, struct {
			connectorID string
			present     bool
		}{connectorID: oldConnectorID, present: true})
	}
	if newConnectorID != "" && newConnectorID != oldConnectorID {
		expected = append(expected, struct {
			connectorID string
			present     bool
		}{connectorID: newConnectorID})
	}
	if len(candidate.ConnectorReferences) != len(expected) {
		return errs.New(errs.KindValidationFailed, "backup policy connector reference evidence is incomplete")
	}
	for index, want := range expected {
		evidence := candidate.ConnectorReferences[index]
		if evidence.ConnectorID != want.connectorID {
			return errs.New(errs.KindValidationFailed, "backup policy connector reference order is invalid")
		}
		if want.present {
			if evidence.Entry == nil || evidence.Entry.Key != backuppolicy.BackupPolicyConnectorReferenceKey(
				want.connectorID,
				environmentID,
			) || evidence.Entry.ModRevision <= 0 || string(evidence.Entry.Value) != environmentID {
				return recordcodec.CorruptRecord()
			}
		} else if evidence.Entry != nil {
			return recordcodec.CorruptRecord()
		}
	}
	return nil
}

func prepareBackupPolicyReplacement(
	candidate backupPolicyReplacementCandidate,
) (backupPolicyReplacementPlan, error) {
	policyValue, err := backuppolicy.EncodeBackupPolicyRecord(candidate.Replacement)
	if err != nil {
		return backupPolicyReplacementPlan{}, err
	}
	coordinationValue, err := encodeEnvironmentCoordinationRecord(candidate.NextCoordination)
	if err != nil {
		clear(policyValue)
		return backupPolicyReplacementPlan{}, err
	}
	plan := backupPolicyReplacementPlan{
		conditions: make([]etcdstore.Condition, 0, 18+len(candidate.Sources)*3),
		mutations: []etcdstore.Mutation{
			{
				Type: etcdstore.MutationPut, Key: backuppolicy.BackupPolicyKey(candidate.Replacement.EnvironmentID), Value: policyValue,
			},
			{
				Type: etcdstore.MutationPut, Key: environmentCoordinationKey(candidate.Replacement.EnvironmentID),
				Value: coordinationValue,
			},
		},
		evidence: make([]backupPolicyReplacementCompare, 0, 18+len(candidate.Sources)*3),
	}
	policyRevision := int64(0)
	if candidate.Current != nil {
		policyRevision = candidate.Current.Revision
	}
	plan.compare(
		backupPolicyComparePolicy,
		candidate.Replacement.EnvironmentID,
		backuppolicy.BackupPolicyKey(candidate.Replacement.EnvironmentID),
		policyRevision,
	)
	plan.compare(
		backupPolicyCompareEnvironment,
		candidate.Environment.Record.ID,
		hierarchyrecord.EnvironmentKey(candidate.Environment.Record.ID),
		candidate.Environment.Revision,
	)
	plan.compare(
		backupPolicyCompareProject,
		candidate.Project.Record.ID,
		hierarchyrecord.ProjectKey(candidate.Project.Record.ID),
		candidate.Project.Revision,
	)
	plan.compare(
		backupPolicyCompareCoordination,
		candidate.Replacement.EnvironmentID,
		environmentCoordinationKey(candidate.Replacement.EnvironmentID),
		candidate.Coordination.Revision,
	)
	plan.compare(
		backupPolicyCompareOperationLock,
		candidate.Replacement.EnvironmentID,
		hierarchyrecord.EnvironmentOperationLockKey(candidate.Replacement.EnvironmentID),
		0,
	)
	for _, fence := range []struct {
		kind DeletionTargetKind
		id   string
	}{
		{kind: DeletionTargetEnvironment, id: candidate.Environment.Record.ID},
		{kind: DeletionTargetProject, id: candidate.Project.Record.ID},
		{kind: DeletionTargetTenant, id: candidate.Project.Record.TenantID},
	} {
		plan.compare(
			backupPolicyCompareHierarchyTombstone,
			fence.id,
			deletionTombstoneKey(string(fence.kind), fence.id),
			0,
		)
	}
	for _, source := range candidate.Sources {
		plan.compare(
			backupPolicyCompareSource,
			source.Source.Record.ID,
			backuppolicy.BackupSourceKey(source.Source.Record.ID),
			source.Source.Revision,
		)
		plan.compare(
			backupPolicyCompareSourceEnvironmentIndex,
			source.Source.Record.ID,
			source.EnvironmentIndex.Key,
			source.EnvironmentIndex.ModRevision,
		)
		plan.compare(
			backupPolicyCompareSourceIdentityIndex,
			source.Source.Record.ID,
			source.IdentityIndex.Key,
			source.IdentityIndex.ModRevision,
		)
		switch string(source.Source.Record.Kind) {
		case "attach":
			plan.compare(
				backupPolicyCompareAttach,
				source.Attach.Record.ID,
				attachrecord.AttachKey(source.Attach.Record.ID),
				source.Attach.Revision,
			)
			plan.compare(
				backupPolicyCompareTargetOwnerIndex,
				source.Attach.Record.ID,
				source.TargetOwnerIndex.Key,
				source.TargetOwnerIndex.ModRevision,
			)
			plan.compare(
				backupPolicyCompareTargetTombstone,
				source.Attach.Record.ID,
				deletionTombstoneKey("attach", source.Attach.Record.ID),
				0,
			)
		case "volume":
			plan.compare(
				backupPolicyCompareVolume,
				source.Volume.Volume.ID,
				environmentBlueprintHeadKey(source.Source.Record.EnvironmentID),
				source.Volume.Projection.Revision,
			)
			plan.compare(
				backupPolicyCompareVolumeRoot,
				source.Volume.Volume.ID,
				environmentBlueprintRootKey(
					source.Source.Record.EnvironmentID,
					source.Volume.Projection.Record.RevisionID,
				),
				source.Volume.ProjectionRoot,
			)
		}
	}
	if candidate.Connector != nil {
		connectorID := candidate.Connector.Record.Connector.ID
		plan.compare(
			backupPolicyCompareConnector,
			connectorID,
			connectorrecord.RecordKey(connectorID),
			candidate.Connector.Revision,
		)
		plan.compare(
			backupPolicyCompareConnectorOwnerIndex,
			connectorID,
			candidate.ConnectorOwnerIndex.Key,
			candidate.ConnectorOwnerIndex.ModRevision,
		)
		plan.compare(
			backupPolicyCompareConnectorTombstone,
			connectorID,
			deletionTombstoneKey(string(DeletionTargetConnector), connectorID),
			0,
		)
	}
	for _, reference := range candidate.ConnectorReferences {
		revision := int64(0)
		if reference.Entry != nil {
			revision = reference.Entry.ModRevision
		}
		plan.compare(
			backupPolicyCompareConnectorReference,
			reference.ConnectorID,
			backuppolicy.BackupPolicyConnectorReferenceKey(reference.ConnectorID, candidate.Replacement.EnvironmentID),
			revision,
		)
	}
	oldConnectorID := ""
	if candidate.Current != nil && candidate.Current.Record.Enabled {
		oldConnectorID = candidate.Current.Record.ConnectorID
	}
	newConnectorID := ""
	if candidate.Replacement.Enabled {
		newConnectorID = candidate.Replacement.ConnectorID
	}
	if oldConnectorID != "" && oldConnectorID != newConnectorID {
		plan.mutations = append(plan.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key:  backuppolicy.BackupPolicyConnectorReferenceKey(oldConnectorID, candidate.Replacement.EnvironmentID),
		})
	}
	if newConnectorID != "" && newConnectorID != oldConnectorID {
		plan.mutations = append(plan.mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   backuppolicy.BackupPolicyConnectorReferenceKey(newConnectorID, candidate.Replacement.EnvironmentID),
			Value: []byte(candidate.Replacement.EnvironmentID),
		})
	}
	keyRecordRevision := int64(0)
	keyValueRevision := int64(0)
	if candidate.ExistingKey != nil {
		keyRecordRevision = candidate.ExistingKey.RecordRevision
		keyValueRevision = candidate.ExistingKey.EncryptedRevision
	}
	plan.compare(
		backupPolicyCompareKey,
		candidate.Replacement.EnvironmentID,
		backuppolicy.BackupKeyKey(candidate.Replacement.EnvironmentID),
		keyRecordRevision,
	)
	plan.compare(
		backupPolicyCompareKey,
		candidate.Replacement.EnvironmentID,
		backuppolicy.BackupKeyValueKey(candidate.Replacement.EnvironmentID),
		keyValueRevision,
	)
	if candidate.InitialKey != nil {
		initial := backupPolicyInitialKey{
			Record:    candidate.InitialKey.Record,
			Encrypted: candidate.InitialKey.Encrypted,
		}
		initial.Encrypted.Ciphertext = append([]byte(nil), candidate.InitialKey.Encrypted.Ciphertext...)
		defer clear(initial.Encrypted.Ciphertext)
		recordValue, encodeErr := backuppolicy.EncodeBackupKeyRecord(initial.Record)
		if encodeErr != nil {
			clearMutationValues(plan.mutations)
			return backupPolicyReplacementPlan{}, encodeErr
		}
		encryptedValue, encodeErr := backuppolicy.EncodeBackupKeyEncryptedValue(initial.Encrypted)
		if encodeErr != nil {
			clear(recordValue)
			clearMutationValues(plan.mutations)
			return backupPolicyReplacementPlan{}, encodeErr
		}
		plan.mutations = append(
			plan.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backuppolicy.BackupKeyKey(candidate.Replacement.EnvironmentID), Value: recordValue},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   backuppolicy.BackupKeyValueKey(candidate.Replacement.EnvironmentID),
				Value: encryptedValue,
			},
		)
	}
	return plan, nil
}

func backupPolicyReplacementOperationCount(
	plan backupPolicyReplacementPlan,
	marker idempotencyrecord.IdempotencyMarker,
) int {
	count := len(plan.conditions) + len(plan.mutations) + 2
	if marker.ReplayTarget != nil {
		count += 2
	}
	if !marker.RetainUntil.IsZero() {
		count++
	}
	return count
}

func classifyBackupPolicyReplacementConflict(
	values []*etcdstore.KeyValue,
	evidence []backupPolicyReplacementCompare,
) error {
	if len(values) != len(evidence) {
		return errs.New(errs.KindInternal, "backup policy replacement compare evidence is incomplete")
	}
	for index, comparison := range evidence {
		value := values[index]
		if (comparison.ExpectedRevision == 0 && value == nil) ||
			(comparison.ExpectedRevision > 0 && value != nil && value.ModRevision == comparison.ExpectedRevision) {
			continue
		}
		switch comparison.Kind {
		case backupPolicyCompareEnvironment:
			if value == nil {
				return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
			}
			return stateConflict("environment", comparison.ID)
		case backupPolicyCompareProject:
			if value == nil {
				return errs.New(errs.KindProjectNotFound, "project was not found")
			}
			return stateConflict("project", comparison.ID)
		case backupPolicyCompareCoordination:
			if value == nil {
				return errs.New(errs.KindInternal, "environment coordination is missing")
			}
			coordination, err := decodeEnvironmentCoordinationRecord(value.Value)
			if err != nil || coordination.EnvironmentID != comparison.ID {
				return errs.New(errs.KindInternal, "environment coordination is corrupt")
			}
			return stateConflict("environment coordination", comparison.ID)
		case backupPolicyCompareOperationLock:
			return errs.New(errs.KindResourceInUse, "environment persistence operation is in progress")
		case backupPolicyCompareSource:
			if value == nil {
				return errs.New(errs.KindBackupSourceNotFound, "backup source was not found")
			}
			return stateConflict("backup source", comparison.ID)
		case backupPolicyCompareAttach:
			if value == nil {
				return errs.New(errs.KindAttachNotFound, "attach was not found")
			}
			return stateConflict("attach", comparison.ID)
		case backupPolicyCompareVolume:
			if value == nil {
				return errs.New(errs.KindVolumeNotFound, "volume was not found")
			}
			return stateConflict("volume", comparison.ID)
		case backupPolicyCompareConnector:
			if value == nil {
				return errs.New(errs.KindConnectorNotFound, "connector was not found")
			}
			return stateConflict("connector", comparison.ID)
		case backupPolicyCompareSourceEnvironmentIndex,
			backupPolicyCompareSourceIdentityIndex,
			backupPolicyCompareTargetOwnerIndex,
			backupPolicyCompareConnectorOwnerIndex:
			return recordcodec.CorruptRecord()
		case backupPolicyCompareHierarchyTombstone,
			backupPolicyCompareTargetTombstone,
			backupPolicyCompareConnectorTombstone:
			return errs.New(errs.KindResourceInUse, "backup policy dependency deletion is in progress")
		case backupPolicyCompareConnectorReference:
			return stateConflict("backup policy connector reference", comparison.ID)
		case backupPolicyCompareKey:
			return stateConflict("backup key", comparison.ID)
		case backupPolicyComparePolicy:
			return stateConflict("backup policy", comparison.ID)
		default:
			return errs.New(errs.KindInternal, "backup policy replacement compare kind is invalid")
		}
	}
	return errs.New(errs.KindStateConflict, "backup policy replacement state changed")
}
