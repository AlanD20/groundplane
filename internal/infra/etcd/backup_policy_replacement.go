package etcd

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// backupPolicyInitialKey is an application-sealed era-1 age identity. The
// repository treats the ciphertext as opaque and never receives plaintext.
type backupPolicyInitialKey struct {
	Record    BackupKeyRecord
	Encrypted BackupKeyEncryptedValue
}

// backupPolicySourceEvidence binds one ordered policy source to the exact
// durable target revision that was validated by the application.
type backupPolicySourceEvidence struct {
	Source           Versioned[BackupSourceRecord]
	EnvironmentIndex *KeyValue
	IdentityIndex    *KeyValue
	Attach           *Versioned[AttachRecord]
	Volume           *backupVolumeProjectionEvidence
	TargetOwnerIndex *KeyValue
}

// backupPolicyConnectorReferenceEvidence proves either the exact existing
// reverse reference or its absence before the replacement transaction.
type backupPolicyConnectorReferenceEvidence struct {
	ConnectorID string
	Entry       *KeyValue
}

// backupPolicyReplacementCandidate contains the fully resolved, prevalidated
// durable evidence for one protected Environment-scoped replacement.
type backupPolicyReplacementCandidate struct {
	Environment         Versioned[EnvironmentRecord]
	Project             Versioned[ProjectRecord]
	MutationEpoch       Versioned[EnvironmentMutationEpochRecord]
	Coordination        Versioned[EnvironmentCoordinationRecord]
	NextCoordination    EnvironmentCoordinationRecord
	NextRunAt           time.Time
	ScheduleSealed      bool
	Current             *Versioned[BackupPolicyRecord]
	Replacement         BackupPolicyRecord
	Sources             []backupPolicySourceEvidence
	Connector           *Versioned[ConnectorRecord]
	ConnectorOwnerIndex *KeyValue
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
	conditions []Condition
	mutations  []Mutation
	evidence   []backupPolicyReplacementCompare
}

func (plan *backupPolicyReplacementPlan) compare(
	kind backupPolicyReplacementCompareKind,
	id string,
	key string,
	revision int64,
) {
	plan.conditions = append(plan.conditions, Condition{Key: key, ModRevision: revision})
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
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateBackupPolicyReplacement(ctx, candidate, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := prepareBackupPolicyReplacement(candidate)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(plan.mutations)
	if backupPolicyReplacementOperationCount(plan, marker) > maximumTransactionOperations {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup policy replacement exceeds the atomic transaction limit",
		)
	}
	mutationPlan, err := newIdempotencyMutationPlan(
		plan.conditions,
		plan.mutations,
		func(_ int64, values []*KeyValue) error {
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
	marker IdempotencyMarker,
) error {
	if err := validatebackupPolicyReplacementCandidate(ctx, candidate); err != nil {
		return err
	}
	return validateBackupPolicyReplacementMarker(candidate, marker)
}

func validateBackupPolicyReplacementMarker(
	candidate backupPolicyReplacementCandidate,
	marker IdempotencyMarker,
) error {
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != candidate.Replacement.EnvironmentID ||
		marker.Locator.Method != http.MethodPut || marker.Locator.Route != backupPolicyReplacementRoute ||
		marker.ReplayTarget != nil || marker.Response.Status != http.StatusOK ||
		marker.Response.ContentKind != "application/json" || !json.Valid(marker.Response.Body) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || !marker.TerminalAt.Equal(marker.CreatedAt) ||
		!marker.RetainUntil.Equal(marker.TerminalAt.Add(markerRetention)) {
		return errs.New(
			errs.KindValidationFailed,
			"backup policy marker must be the retained completed response for its exact put operation",
		)
	}
	return validateIdempotencyMarker(marker)
}

func validatebackupPolicyReplacementCandidate(
	ctx context.Context,
	candidate backupPolicyReplacementCandidate,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEnvironment(candidate.Environment.Record); err != nil {
		return err
	}
	if err := validateProject(candidate.Project.Record); err != nil {
		return err
	}
	if err := validateEnvironmentMutationEpochRecord(candidate.MutationEpoch.Record); err != nil {
		return err
	}
	if err := validateBackupPolicyRecord(candidate.Replacement); err != nil {
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
		candidate.Project.Record.Kind != ProjectKindTenant || candidate.Project.Record.TenantID == "" {
		return errs.New(errs.KindValidationFailed, "backup policy hierarchy is invalid")
	}
	if candidate.Environment.Record.ProvisioningState != EnvironmentProvisioningReady {
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
		if err := validateBackupPolicyRecord(candidate.Current.Record); err != nil {
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
		if err := validateBackupSourceRecord(source.Record); err != nil {
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
		if err := validateConnectorRecord(candidate.Connector.Record); err != nil {
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
			return corruptRecord()
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
	if err := validateBackupSourceRecord(evidence.Source.Record); err != nil {
		return err
	}
	if !validReplacementRevision(evidence.Source.Revision, evidence.Source.ReadRevision) ||
		evidence.Source.Record.ID != wantSourceID || evidence.Source.Record.EnvironmentID != environmentID {
		return errs.New(errs.KindValidationFailed, "backup policy source evidence is invalid")
	}
	if !validBackupPolicyIndex(
		evidence.EnvironmentIndex,
		backupSourceEnvironmentKey(environmentID, evidence.Source.Record.ID),
		evidence.Source.Record.ID,
	) || !validBackupPolicyIndex(
		evidence.IdentityIndex,
		backupSourceIdentityKey(environmentID, evidence.Source.Record.Kind, evidence.Source.Record.TargetID),
		evidence.Source.Record.ID,
	) {
		return corruptRecord()
	}
	switch string(evidence.Source.Record.Kind) {
	case "config":
		if evidence.Source.Record.TargetID != environmentID || evidence.Attach != nil ||
			evidence.Volume != nil || evidence.TargetOwnerIndex != nil {
			return errs.New(errs.KindValidationFailed, "config backup source cannot carry target evidence")
		}
	case "attach":
		if evidence.Attach == nil || evidence.Volume != nil ||
			validateAttachRecord(evidence.Attach.Record) != nil ||
			!validReplacementRevision(evidence.Attach.Revision, evidence.Attach.ReadRevision) ||
			evidence.Attach.Record.ID != evidence.Source.Record.TargetID ||
			!evidence.Attach.Record.OwnsCredential() ||
			evidence.Attach.Record.EnvironmentID != environmentID ||
			!validBackupPolicyIndex(
				evidence.TargetOwnerIndex,
				attachOwnerKey(environmentID, evidence.Attach.Record.ID),
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
			!validSHA256(evidence.Volume.DependencyDigest) {
			return errs.New(errs.KindValidationFailed, "volume backup source evidence is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup policy source kind is invalid")
	}
	return nil
}

func validBackupPolicyIndex(entry *KeyValue, key string, value string) bool {
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
	if err := validateBackupKeyRecord(candidate.InitialKey.Record); err != nil {
		return err
	}
	if err := validateBackupKeyEncryptedValue(candidate.InitialKey.Encrypted); err != nil {
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
			if evidence.Entry == nil || evidence.Entry.Key != backupPolicyConnectorReferenceKey(
				want.connectorID,
				environmentID,
			) || evidence.Entry.ModRevision <= 0 || string(evidence.Entry.Value) != environmentID {
				return corruptRecord()
			}
		} else if evidence.Entry != nil {
			return corruptRecord()
		}
	}
	return nil
}

func prepareBackupPolicyReplacement(
	candidate backupPolicyReplacementCandidate,
) (backupPolicyReplacementPlan, error) {
	policyValue, err := encodeBackupPolicyRecord(candidate.Replacement)
	if err != nil {
		return backupPolicyReplacementPlan{}, err
	}
	coordinationValue, err := encodeEnvironmentCoordinationRecord(candidate.NextCoordination)
	if err != nil {
		clear(policyValue)
		return backupPolicyReplacementPlan{}, err
	}
	plan := backupPolicyReplacementPlan{
		conditions: make([]Condition, 0, 18+len(candidate.Sources)*3),
		mutations: []Mutation{
			{
				Type: MutationPut, Key: backupPolicyKey(candidate.Replacement.EnvironmentID), Value: policyValue,
			},
			{
				Type: MutationPut, Key: environmentCoordinationKey(candidate.Replacement.EnvironmentID),
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
		backupPolicyKey(candidate.Replacement.EnvironmentID),
		policyRevision,
	)
	plan.compare(
		backupPolicyCompareEnvironment,
		candidate.Environment.Record.ID,
		environmentKey(candidate.Environment.Record.ID),
		candidate.Environment.Revision,
	)
	plan.compare(
		backupPolicyCompareProject,
		candidate.Project.Record.ID,
		projectKey(candidate.Project.Record.ID),
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
		environmentOperationLockKey(candidate.Replacement.EnvironmentID),
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
			backupSourceKey(source.Source.Record.ID),
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
				attachKey(source.Attach.Record.ID),
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
			connectorRecordKey(connectorID),
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
			backupPolicyConnectorReferenceKey(reference.ConnectorID, candidate.Replacement.EnvironmentID),
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
		plan.mutations = append(plan.mutations, Mutation{
			Type: MutationDelete,
			Key:  backupPolicyConnectorReferenceKey(oldConnectorID, candidate.Replacement.EnvironmentID),
		})
	}
	if newConnectorID != "" && newConnectorID != oldConnectorID {
		plan.mutations = append(plan.mutations, Mutation{
			Type:  MutationPut,
			Key:   backupPolicyConnectorReferenceKey(newConnectorID, candidate.Replacement.EnvironmentID),
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
		backupKeyKey(candidate.Replacement.EnvironmentID),
		keyRecordRevision,
	)
	plan.compare(
		backupPolicyCompareKey,
		candidate.Replacement.EnvironmentID,
		backupKeyValueKey(candidate.Replacement.EnvironmentID),
		keyValueRevision,
	)
	if candidate.InitialKey != nil {
		initial := backupPolicyInitialKey{
			Record:    candidate.InitialKey.Record,
			Encrypted: candidate.InitialKey.Encrypted,
		}
		initial.Encrypted.Ciphertext = append([]byte(nil), candidate.InitialKey.Encrypted.Ciphertext...)
		defer clear(initial.Encrypted.Ciphertext)
		recordValue, encodeErr := encodeBackupKeyRecord(initial.Record)
		if encodeErr != nil {
			clearMutationValues(plan.mutations)
			return backupPolicyReplacementPlan{}, encodeErr
		}
		encryptedValue, encodeErr := encodeBackupKeyEncryptedValue(initial.Encrypted)
		if encodeErr != nil {
			clear(recordValue)
			clearMutationValues(plan.mutations)
			return backupPolicyReplacementPlan{}, encodeErr
		}
		plan.mutations = append(
			plan.mutations,
			Mutation{Type: MutationPut, Key: backupKeyKey(candidate.Replacement.EnvironmentID), Value: recordValue},
			Mutation{
				Type:  MutationPut,
				Key:   backupKeyValueKey(candidate.Replacement.EnvironmentID),
				Value: encryptedValue,
			},
		)
	}
	return plan, nil
}

func backupPolicyReplacementOperationCount(
	plan backupPolicyReplacementPlan,
	marker IdempotencyMarker,
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
	values []*KeyValue,
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
			return corruptRecord()
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
