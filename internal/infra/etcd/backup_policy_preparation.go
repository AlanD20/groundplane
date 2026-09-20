package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// MaximumBackupPolicySources is the largest selection whose worst-case
// protected replacement fits the fixed 96-operation etcd transaction budget.
// The worst case is an enabled Connector move that creates era 1 and selects
// only Attach/Volume sources: 24 fixed operations plus 6 per source.
const (
	MaximumBackupPolicySources       = 12
	MaximumBackupPolicyKeep    int64 = 9_007_199_254_740_991
)

// BackupPolicySourceSelection is the stable-id form accepted by the human API
// after label resolution. It carries no persistence revisions or indexes.
type BackupPolicySourceSelection struct {
	Kind     core.BackupSourceKind
	TargetID string
}

// BackupPolicyReplacementInput is one complete desired singleton document.
type BackupPolicyReplacementInput struct {
	EnvironmentID string
	Enabled       bool
	Frequency     string
	Keep          int64
	Encryption    string
	ConnectorID   string
	Sources       []BackupPolicySourceSelection
}

// BackupPolicyInitialKeyMaterial is cryptographic output supplied by the
// application. Persistence owns the era, owner, and lifecycle timestamps.
type BackupPolicyInitialKeyMaterial struct {
	Recipient  string
	Ciphertext []byte
}

// PreparedBackupPolicyReplacement keeps raw compare evidence opaque while
// exposing the exact typed projection the application serializes into the
// protected 200 response.
type PreparedBackupPolicyReplacement struct {
	candidate          backupPolicyReplacementCandidate
	projection         BackupPolicyProjection
	requiresInitialKey bool
}

func (prepared PreparedBackupPolicyReplacement) Projection() BackupPolicyProjection {
	return cloneBackupPolicyProjection(prepared.projection)
}

func (prepared PreparedBackupPolicyReplacement) RequiresInitialKey() bool {
	return prepared.requiresInitialKey
}

// FinalizeSchedule chooses the replacement logical boundary after any initial
// key preparation and immediately before the protected response is sealed.
func (prepared PreparedBackupPolicyReplacement) FinalizeSchedule(
	now time.Time,
) (PreparedBackupPolicyReplacement, error) {
	if prepared.requiresInitialKey || (prepared.candidate.InitialKey == nil &&
		prepared.candidate.Replacement.Enabled &&
		prepared.candidate.Replacement.Encryption == "age" &&
		prepared.candidate.ExistingKey == nil) {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindValidationFailed, "backup policy initial key is not prepared",
		)
	}
	if err := sealBackupPolicyCandidateSchedule(&prepared.candidate, now); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if err := validatebackupPolicyReplacementCandidate(context.Background(), prepared.candidate); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	prepared.projection = backupPolicyProjectionFromCandidate(prepared.candidate)
	return prepared, nil
}

// Destroy clears private key ciphertext retained by an abandoned or completed
// prepared replacement. The prepared value must not be reused afterward.
func (prepared *PreparedBackupPolicyReplacement) Destroy() {
	if prepared == nil {
		return
	}
	if prepared.candidate.ExistingKey != nil {
		clear(prepared.candidate.ExistingKey.Encrypted.Ciphertext)
	}
	if prepared.candidate.InitialKey != nil {
		clear(prepared.candidate.InitialKey.Encrypted.Ciphertext)
	}
	*prepared = PreparedBackupPolicyReplacement{}
}

// PrepareBackupPolicyReplacement resolves stable source catalog identities and
// captures every hierarchy, owner-index, Connector, key, and reverse-reference
// compare inside the etcd adapter. Callers never construct KeyValue evidence.
func (repository *BackupPolicyRepository) PrepareBackupPolicyReplacement(
	ctx context.Context,
	input BackupPolicyReplacementInput,
) (PreparedBackupPolicyReplacement, error) {
	if err := validateBackupPolicyReplacementInput(ctx, input); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	hierarchy, err := newHierarchyRepository(repository.store)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	environment, err := hierarchy.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	project, err := hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if project.Record.Kind != ProjectKindTenant || project.Record.TenantID == "" {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindValidationFailed,
			"backing environments cannot own backup policies",
		)
	}
	if environment.Record.ProvisioningState != EnvironmentProvisioningReady {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindStateConflict,
			"environment is not ready for backup policy replacement",
		)
	}

	resolved := make([]Versioned[BackupSourceRecord], len(input.Sources))
	for index, source := range input.Sources {
		if err := repository.validateBackupPolicySelectionTarget(ctx, input.EnvironmentID, source); err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
		resolved[index], err = repository.EnsureBackupSource(
			ctx,
			environment,
			project,
			source.Kind,
			source.TargetID,
		)
		if err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
	}

	now := repository.now().UTC()
	candidate, keyFound, err := repository.loadBackupPolicyReplacementBase(
		ctx,
		input,
		environment.Record.ProjectID,
		project.Record.TenantID,
		now,
	)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	keepCandidate := false
	defer func() {
		if candidate.ExistingKey != nil && !keepCandidate {
			clear(candidate.ExistingKey.Encrypted.Ciphertext)
		}
	}()
	for index, source := range resolved {
		candidate.Replacement.SourceIDs[index] = source.Record.ID
		candidate.Sources[index], err = repository.loadbackupPolicySourceEvidence(
			ctx,
			source,
			candidate.MutationEpoch.ReadRevision,
		)
		if err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
	}
	if input.Enabled {
		candidate.Connector, candidate.ConnectorOwnerIndex, err = repository.loadBackupPolicyConnectorEvidence(
			ctx,
			input.EnvironmentID,
			input.ConnectorID,
			candidate.MutationEpoch.ReadRevision,
		)
		if err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
	}
	candidate.ConnectorReferences, err = repository.loadBackupPolicyConnectorReferences(
		ctx,
		candidate,
		candidate.MutationEpoch.ReadRevision,
	)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if err := sealBackupPolicyCandidateSchedule(&candidate, now); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	requiresInitialKey := input.Enabled && input.Encryption == "age" && !keyFound
	projection := BackupPolicyProjection{}
	if !requiresInitialKey {
		if err := validatebackupPolicyReplacementCandidate(ctx, candidate); err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
		projection = backupPolicyProjectionFromCandidate(candidate)
	}
	keepCandidate = true
	return PreparedBackupPolicyReplacement{
		candidate: candidate, projection: projection, requiresInitialKey: requiresInitialKey,
	}, nil
}

func (repository *BackupPolicyRepository) loadBackupPolicyReplacementBase(
	ctx context.Context,
	input BackupPolicyReplacementInput,
	projectID string,
	tenantID string,
	now time.Time,
) (backupPolicyReplacementCandidate, bool, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		environmentKey(input.EnvironmentID),
		projectKey(projectID),
		backupPolicyKey(input.EnvironmentID),
		environmentMutationEpochKey(input.EnvironmentID),
		environmentOperationLockKey(input.EnvironmentID),
		backupKeyKey(input.EnvironmentID),
		backupKeyValueKey(input.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), input.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetProject), projectID),
		deletionTombstoneKey(string(DeletionTargetTenant), tenantID),
		environmentCoordinationKey(input.EnvironmentID),
	}})
	if err != nil {
		return backupPolicyReplacementCandidate{}, false, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != 11 {
		return backupPolicyReplacementCandidate{}, false, errs.New(
			errs.KindInternal,
			"backup policy replacement base read is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	if result.Values[0] == nil {
		return backupPolicyReplacementCandidate{}, false, errs.New(
			errs.KindEnvironmentNotFound,
			"environment was not found",
		)
	}
	environment, err := decodeEnvironment(result.Values[0].Value)
	if err != nil || environment.ID != input.EnvironmentID || environment.ProjectID != projectID {
		return backupPolicyReplacementCandidate{}, false, corruptRecord()
	}
	if environment.ProvisioningState != EnvironmentProvisioningReady {
		return backupPolicyReplacementCandidate{}, false, errs.New(
			errs.KindStateConflict,
			"environment is not ready for backup policy replacement",
		)
	}
	if result.Values[1] == nil {
		return backupPolicyReplacementCandidate{}, false, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	project, err := decodeProject(result.Values[1].Value)
	if err != nil || project.ID != projectID || project.TenantID != tenantID || project.Kind != ProjectKindTenant {
		return backupPolicyReplacementCandidate{}, false, corruptRecord()
	}
	for _, index := range []int{7, 8, 9} {
		if result.Values[index] != nil {
			return backupPolicyReplacementCandidate{}, false, errs.New(
				errs.KindResourceInUse,
				"backup policy hierarchy deletion is in progress",
			)
		}
	}
	if result.Values[4] != nil {
		return backupPolicyReplacementCandidate{}, false, errs.New(
			errs.KindResourceInUse,
			"environment persistence operation is in progress",
		)
	}
	if result.Values[3] == nil {
		return backupPolicyReplacementCandidate{}, false, errs.New(
			errs.KindInternal,
			"environment mutation epoch is missing",
		)
	}
	epoch, err := decodeEnvironmentMutationEpochRecord(result.Values[3].Value)
	if err != nil || epoch.EnvironmentID != input.EnvironmentID {
		return backupPolicyReplacementCandidate{}, false, errs.New(
			errs.KindInternal,
			"environment mutation epoch is corrupt",
		)
	}
	candidate := backupPolicyReplacementCandidate{
		Environment: Versioned[EnvironmentRecord]{
			Record: environment, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
		},
		Project: Versioned[ProjectRecord]{
			Record: project, Revision: result.Values[1].ModRevision, ReadRevision: result.ReadRevision,
		},
		MutationEpoch: Versioned[EnvironmentMutationEpochRecord]{
			Record: epoch, Revision: result.Values[3].ModRevision, ReadRevision: result.ReadRevision,
		},
		Replacement: BackupPolicyRecord{
			EnvironmentID: input.EnvironmentID,
			Enabled:       input.Enabled,
			Frequency:     input.Frequency,
			Keep:          input.Keep,
			Encryption:    input.Encryption,
			ConnectorID:   input.ConnectorID,
			SourceIDs:     make([]string, len(input.Sources)),
			UpdatedAt:     now,
		},
		Sources: make([]backupPolicySourceEvidence, len(input.Sources)),
	}
	if result.Values[2] != nil {
		current, decodeErr := decodeBackupPolicyRecord(result.Values[2].Value)
		if decodeErr != nil || current.EnvironmentID != input.EnvironmentID {
			return backupPolicyReplacementCandidate{}, false, corruptRecord()
		}
		candidate.Current = &Versioned[BackupPolicyRecord]{
			Record: current, Revision: result.Values[2].ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	coordination := EnvironmentCoordinationRecord{
		EnvironmentID: input.EnvironmentID, ScheduleClockFloor: now,
	}
	coordinationRevision := int64(0)
	if result.Values[10] != nil {
		decoded, coordinationErr := decodeEnvironmentCoordinationRecord(result.Values[10].Value)
		if coordinationErr != nil || decoded.EnvironmentID != input.EnvironmentID {
			return backupPolicyReplacementCandidate{}, false, corruptEnvironmentCoordination()
		}
		coordination = decoded
		coordinationRevision = result.Values[10].ModRevision
	} else if candidate.Current != nil {
		return backupPolicyReplacementCandidate{}, false, corruptEnvironmentCoordination()
	}
	candidate.Coordination = Versioned[EnvironmentCoordinationRecord]{
		Record: coordination, Revision: coordinationRevision, ReadRevision: result.ReadRevision,
	}
	if candidate.Current == nil {
		if coordination.CurrentBackupScheduleState != nil {
			return backupPolicyReplacementCandidate{}, false, corruptEnvironmentCoordination()
		}
	} else if candidate.Current.Record.Enabled {
		digest, digestErr := backupPolicyScheduleDigest(candidate.Current.Record)
		state := coordination.CurrentBackupScheduleState
		if digestErr != nil || state == nil || state.PolicyDigest != digest ||
			state.Frequency != candidate.Current.Record.Frequency {
			return backupPolicyReplacementCandidate{}, false, corruptEnvironmentCoordination()
		}
	} else if coordination.CurrentBackupScheduleState != nil {
		return backupPolicyReplacementCandidate{}, false, corruptEnvironmentCoordination()
	}
	if (result.Values[5] == nil) != (result.Values[6] == nil) {
		return backupPolicyReplacementCandidate{}, false, corruptBackupKey()
	}
	keyFound := result.Values[5] != nil
	if keyFound {
		record, decodeErr := decodeBackupKeyRecord(result.Values[5].Value)
		if decodeErr != nil {
			return backupPolicyReplacementCandidate{}, false, corruptBackupKey()
		}
		encrypted, decodeErr := decodeBackupKeyEncryptedValue(result.Values[6].Value)
		if decodeErr != nil || record.EnvironmentID != input.EnvironmentID ||
			encrypted.EnvironmentID != input.EnvironmentID || record.KeyEra != encrypted.KeyEra {
			clear(encrypted.Ciphertext)
			return backupPolicyReplacementCandidate{}, false, corruptBackupKey()
		}
		candidate.ExistingKey = &VersionedBackupKey{
			Record: record, Encrypted: encrypted,
			RecordRevision: result.Values[5].ModRevision, EncryptedRevision: result.Values[6].ModRevision,
			ReadRevision: result.ReadRevision,
		}
	}
	return candidate, keyFound, nil
}

func (repository *BackupPolicyRepository) SupplyBackupPolicyInitialKey(
	ctx context.Context,
	prepared PreparedBackupPolicyReplacement,
	material BackupPolicyInitialKeyMaterial,
) (PreparedBackupPolicyReplacement, error) {
	if err := validateContext(ctx); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if !prepared.requiresInitialKey || prepared.candidate.InitialKey != nil {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindValidationFailed, "backup policy preparation does not require initial key material",
		)
	}
	initial, err := newbackupPolicyInitialKey(
		prepared.candidate.Replacement.EnvironmentID,
		prepared.candidate.Replacement.UpdatedAt,
		&material,
	)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	prepared.candidate.InitialKey = initial
	if err := validatebackupPolicyReplacementCandidate(ctx, prepared.candidate); err != nil {
		clear(initial.Encrypted.Ciphertext)
		return PreparedBackupPolicyReplacement{}, err
	}
	prepared.requiresInitialKey = false
	prepared.projection = backupPolicyProjectionFromCandidate(prepared.candidate)
	return prepared, nil
}

// ReplaceBackupPolicyProtected commits an opaque prepared replacement and the
// exact completed-direct response marker in one transaction.
func (repository *BackupPolicyRepository) ReplaceBackupPolicyProtected(
	ctx context.Context,
	prepared PreparedBackupPolicyReplacement,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.replaceBackupPolicyProtected(ctx, prepared.candidate, marker)
}

func sealBackupPolicyCandidateSchedule(
	candidate *backupPolicyReplacementCandidate,
	now time.Time,
) error {
	if candidate == nil || !validEnvironmentCoordinationInstant(now.UTC()) {
		return errs.New(errs.KindValidationFailed, "backup policy schedule boundary is invalid")
	}
	candidate.Replacement.UpdatedAt = now.UTC()
	next, nextRunAt, err := replaceEnvironmentCoordinationSchedule(
		candidate.Coordination.Record, candidate.Replacement, now.UTC(),
	)
	if err != nil {
		return err
	}
	candidate.NextCoordination = next
	candidate.NextRunAt = nextRunAt
	candidate.ScheduleSealed = true
	return nil
}

func validateBackupPolicyReplacementInput(
	ctx context.Context,
	input BackupPolicyReplacementInput,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateID(ids.KindEnvironment, input.EnvironmentID); err != nil {
		return err
	}
	if len(input.Sources) > MaximumBackupPolicySources {
		return errs.New(
			errs.KindValidationFailed,
			"backup policy may select at most 12 sources",
		)
	}
	configured := input.Frequency != "" || input.Keep != 0 || input.Encryption != "" ||
		input.ConnectorID != "" || len(input.Sources) != 0
	if input.Enabled || configured {
		if input.Frequency == "" || input.Keep <= 0 || input.Keep > MaximumBackupPolicyKeep ||
			(input.Encryption != "age" && input.Encryption != "none") ||
			input.Enabled && (len(input.Sources) == 0 || input.ConnectorID == "") {
			return errs.New(errs.KindValidationFailed, "configured backup policy is incomplete")
		}
		if err := validateBackupPolicyFrequency(input.Frequency); err != nil {
			return err
		}
		if err := validateID(ids.KindConnector, input.ConnectorID); err != nil {
			return err
		}
	}
	seen := make(map[struct {
		kind     core.BackupSourceKind
		targetID string
	}]struct{}, len(input.Sources))
	for _, source := range input.Sources {
		probe := BackupSourceRecord{
			ID:            ids.New(ids.KindBackupSource),
			EnvironmentID: input.EnvironmentID,
			Kind:          source.Kind,
			TargetID:      source.TargetID,
			CreatedAt:     time.Now().UTC(),
		}
		if err := validateBackupSourceRecord(probe); err != nil {
			return err
		}
		identity := struct {
			kind     core.BackupSourceKind
			targetID string
		}{kind: source.Kind, targetID: source.TargetID}
		if _, duplicate := seen[identity]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup policy source identities must be unique")
		}
		seen[identity] = struct{}{}
		if source.Kind == core.BackupSourceConfig && input.Encryption != "age" {
			return errs.New(errs.KindValidationFailed, "config backup source requires age encryption")
		}
	}
	return nil
}

func (repository *BackupPolicyRepository) validateBackupPolicySelectionTarget(
	ctx context.Context,
	environmentID string,
	selection BackupPolicySourceSelection,
) error {
	if selection.Kind == core.BackupSourceConfig {
		return nil
	}
	if selection.Kind == core.BackupSourceVolume {
		evidence, err := loadBackupVolumeProjectionEvidence(
			ctx, repository.store, environmentID, selection.TargetID, 0,
		)
		if err != nil {
			return err
		}
		if evidence.Projection.Record.EnvironmentID != environmentID {
			return errs.New(errs.KindScopeUnauthorized, "backup policy source belongs to another environment")
		}
		return nil
	}
	primaryKey := ""
	ownerKey := ""
	notFound := errs.KindAttachNotFound
	if selection.Kind == core.BackupSourceAttach {
		primaryKey = attachKey(selection.TargetID)
		ownerKey = attachOwnerKey(environmentID, selection.TargetID)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		primaryKey,
		ownerKey,
		deletionTombstoneKey(string(selection.Kind), selection.TargetID),
	}})
	if err != nil {
		return err
	}
	if result == nil || len(result.Values) != 3 {
		return errs.New(errs.KindInternal, "backup policy source target read is incomplete")
	}
	if result.Values[0] == nil {
		return errs.New(notFound, string(selection.Kind)+" was not found")
	}
	ownerEnvironmentID := ""
	if selection.Kind == core.BackupSourceAttach {
		record, decodeErr := decodeAttachRecord(result.Values[0].Value)
		if decodeErr != nil || record.ID != selection.TargetID {
			return corruptRecord()
		}
		if !record.OwnsCredential() {
			return errs.New(errs.KindValidationFailed, "backup policy requires a credential-owning Attach")
		}
		ownerEnvironmentID = record.EnvironmentID
	}
	if ownerEnvironmentID != environmentID {
		return errs.New(errs.KindScopeUnauthorized, "backup policy source belongs to another environment")
	}
	if result.Values[1] == nil || result.Values[1].Key != ownerKey ||
		string(result.Values[1].Value) != selection.TargetID {
		return corruptRecord()
	}
	if result.Values[2] != nil {
		return errs.New(errs.KindResourceInUse, "backup policy source deletion is in progress")
	}
	return nil
}

func (repository *BackupPolicyRepository) loadbackupPolicySourceEvidence(
	ctx context.Context,
	expected Versioned[BackupSourceRecord],
	revision int64,
) (backupPolicySourceEvidence, error) {
	record := expected.Record
	keys := []string{
		backupSourceKey(record.ID),
		backupSourceEnvironmentKey(record.EnvironmentID, record.ID),
		backupSourceIdentityKey(record.EnvironmentID, record.Kind, record.TargetID),
	}
	if record.Kind == core.BackupSourceAttach {
		keys = append(keys, attachKey(record.TargetID), attachOwnerKey(record.EnvironmentID, record.TargetID))
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return backupPolicySourceEvidence{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) || result.Values[0] == nil ||
		result.Values[1] == nil || result.Values[2] == nil {
		return backupPolicySourceEvidence{}, corruptRecord()
	}
	current, err := decodeBackupSourceRecord(result.Values[0].Value)
	if err != nil || current.ID != record.ID || current.EnvironmentID != record.EnvironmentID ||
		current.Kind != record.Kind || current.TargetID != record.TargetID ||
		string(result.Values[1].Value) != record.ID || string(result.Values[2].Value) != record.ID {
		return backupPolicySourceEvidence{}, corruptRecord()
	}
	evidence := backupPolicySourceEvidence{
		Source: Versioned[BackupSourceRecord]{
			Record: current, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
		},
		EnvironmentIndex: cloneBackupPolicyEvidenceKeyValue(result.Values[1]),
		IdentityIndex:    cloneBackupPolicyEvidenceKeyValue(result.Values[2]),
	}
	if record.Kind == core.BackupSourceConfig {
		return evidence, nil
	}
	if record.Kind == core.BackupSourceVolume {
		volume, volumeErr := loadBackupVolumeProjectionEvidence(
			ctx, repository.store, record.EnvironmentID, record.TargetID, revision,
		)
		if volumeErr != nil {
			return backupPolicySourceEvidence{}, volumeErr
		}
		evidence.Volume = &volume
		return evidence, nil
	}
	if result.Values[3] == nil || result.Values[4] == nil ||
		string(result.Values[4].Value) != record.TargetID {
		return backupPolicySourceEvidence{}, corruptRecord()
	}
	evidence.TargetOwnerIndex = cloneBackupPolicyEvidenceKeyValue(result.Values[4])
	if record.Kind == core.BackupSourceAttach {
		attach, decodeErr := decodeAttachRecord(result.Values[3].Value)
		if decodeErr != nil || attach.ID != record.TargetID || attach.EnvironmentID != record.EnvironmentID {
			return backupPolicySourceEvidence{}, corruptRecord()
		}
		evidence.Attach = &Versioned[AttachRecord]{
			Record: attach, Revision: result.Values[3].ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	return evidence, nil
}

func (repository *BackupPolicyRepository) loadBackupPolicyConnectorEvidence(
	ctx context.Context,
	environmentID string,
	connectorID string,
	revision int64,
) (*Versioned[ConnectorRecord], *etcdstore.KeyValue, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		connectorRecordKey(connectorID),
		connectorEnvironmentKey(environmentID, connectorID),
	}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 2 {
		return nil, nil, errs.New(errs.KindInternal, "backup policy connector read is incomplete")
	}
	if result.Values[0] == nil {
		return nil, nil, errs.New(errs.KindConnectorNotFound, "connector was not found")
	}
	record, err := decodeConnectorRecord(result.Values[0].Value)
	if err != nil || record.Connector.ID != connectorID {
		return nil, nil, corruptConnectorRecord()
	}
	if record.Connector.EnvironmentID != environmentID {
		return nil, nil, errs.New(errs.KindScopeUnauthorized, "connector belongs to another environment")
	}
	if result.Values[1] == nil || string(result.Values[1].Value) != connectorID {
		return nil, nil, corruptConnectorRecord()
	}
	return &Versioned[ConnectorRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, cloneBackupPolicyEvidenceKeyValue(result.Values[1]), nil
}

func (repository *BackupPolicyRepository) loadBackupPolicyConnectorReferences(
	ctx context.Context,
	candidate backupPolicyReplacementCandidate,
	revision int64,
) ([]backupPolicyConnectorReferenceEvidence, error) {
	oldConnectorID := ""
	if candidate.Current != nil && candidate.Current.Record.Enabled {
		oldConnectorID = candidate.Current.Record.ConnectorID
	}
	newConnectorID := ""
	if candidate.Replacement.Enabled {
		newConnectorID = candidate.Replacement.ConnectorID
	}
	connectorIDs := make([]string, 0, 2)
	if oldConnectorID != "" {
		connectorIDs = append(connectorIDs, oldConnectorID)
	}
	if newConnectorID != "" && newConnectorID != oldConnectorID {
		connectorIDs = append(connectorIDs, newConnectorID)
	}
	evidence := make([]backupPolicyConnectorReferenceEvidence, len(connectorIDs))
	for index, connectorID := range connectorIDs {
		result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				backupPolicyConnectorReferenceKey(connectorID, candidate.Replacement.EnvironmentID),
			},
			Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if result == nil || result.ReadRevision != revision || len(result.Values) != 1 {
			return nil, errs.New(errs.KindInternal, "backup policy connector reference read is empty")
		}
		evidence[index] = backupPolicyConnectorReferenceEvidence{
			ConnectorID: connectorID,
			Entry:       cloneBackupPolicyEvidenceKeyValue(result.Values[0]),
		}
	}
	return evidence, nil
}

func newbackupPolicyInitialKey(
	environmentID string,
	now time.Time,
	material *BackupPolicyInitialKeyMaterial,
) (*backupPolicyInitialKey, error) {
	if material == nil {
		return nil, errs.New(errs.KindValidationFailed, "initial age key material is required")
	}
	initial := &backupPolicyInitialKey{
		Record: BackupKeyRecord{
			EnvironmentID: environmentID,
			Recipient:     material.Recipient,
			KeyEra:        1,
			CreatedAt:     now,
			RotatedAt:     now,
		},
		Encrypted: BackupKeyEncryptedValue{
			EnvironmentID: environmentID,
			KeyEra:        1,
			Ciphertext:    append([]byte(nil), material.Ciphertext...),
		},
	}
	if err := validateBackupKeyRecord(initial.Record); err != nil {
		clear(initial.Encrypted.Ciphertext)
		return nil, err
	}
	if err := validateBackupKeyEncryptedValue(initial.Encrypted); err != nil {
		clear(initial.Encrypted.Ciphertext)
		return nil, err
	}
	return initial, nil
}

func backupPolicyProjectionFromCandidate(candidate backupPolicyReplacementCandidate) BackupPolicyProjection {
	projection := BackupPolicyProjection{
		EnvironmentID: candidate.Replacement.EnvironmentID,
		Enabled:       candidate.Replacement.Enabled,
		Frequency:     candidate.Replacement.Frequency,
		Keep:          candidate.Replacement.Keep,
		Encryption:    candidate.Replacement.Encryption,
		ConnectorID:   candidate.Replacement.ConnectorID,
		Sources:       make([]BackupPolicySourceProjection, len(candidate.Sources)),
		NextRunAt:     backupPolicyNextRunAt(candidate.NextRunAt),
	}
	for index, source := range candidate.Sources {
		projection.Sources[index] = BackupPolicySourceProjection{
			ID: source.Source.Record.ID, Kind: source.Source.Record.Kind,
			TargetID: source.Source.Record.TargetID,
		}
	}
	if candidate.ExistingKey != nil {
		projection.AgeRecipient = candidate.ExistingKey.Record.Recipient
		projection.KeyEra = candidate.ExistingKey.Record.KeyEra
		projection.KeyCreatedAt = candidate.ExistingKey.Record.CreatedAt
		projection.KeyRotatedAt = candidate.ExistingKey.Record.RotatedAt
	} else if candidate.InitialKey != nil {
		projection.AgeRecipient = candidate.InitialKey.Record.Recipient
		projection.KeyEra = candidate.InitialKey.Record.KeyEra
		projection.KeyCreatedAt = candidate.InitialKey.Record.CreatedAt
		projection.KeyRotatedAt = candidate.InitialKey.Record.RotatedAt
	}
	return projection
}

func backupPolicyNextRunAt(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}

func cloneBackupPolicyEvidenceKeyValue(value *etcdstore.KeyValue) *etcdstore.KeyValue {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Value = append([]byte(nil), value.Value...)
	return &clone
}
