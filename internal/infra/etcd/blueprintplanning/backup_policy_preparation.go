package blueprintplanning

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintBackupPolicySourceEvidence struct {
	record           backuppolicy.BackupSourceRecord
	primary          *etcdstore.KeyValue
	environmentIndex *etcdstore.KeyValue
	identityIndex    *etcdstore.KeyValue
	attach           *etcdstore.Versioned[attachrecord.Record]
	attachOwner      *etcdstore.KeyValue
	candidateAttach  bool
}

type blueprintBackupPolicyPreparationState struct {
	mu                  sync.Mutex
	consumed            bool
	environmentID       string
	taskID              string
	createdAt           time.Time
	desired             *projectionrecord.EnvironmentBlueprintBackupPolicy
	candidate           backuppolicymutations.ReplacementCandidate
	sources             []blueprintBackupPolicySourceEvidence
	connectorNameIndex  *etcdstore.KeyValue
	connectorTombstone  *etcdstore.KeyValue
	retainedConnectorID string
	retain              bool
	requiresInitialKey  bool
}
type BlueprintBackupPolicyPreparation struct {
	state *blueprintBackupPolicyPreparationState
}

func (prepared BlueprintBackupPolicyPreparation) IsZero() bool { return prepared.state == nil }
func (prepared BlueprintBackupPolicyPreparation) Projection() *projectionrecord.EnvironmentBlueprintBackupPolicy {
	if prepared.state == nil {
		return nil
	}
	prepared.state.mu.Lock()
	defer prepared.state.mu.Unlock()
	return projectionrecord.CloneEnvironmentBlueprintBackupPolicy(prepared.state.desired)
}
func (prepared BlueprintBackupPolicyPreparation) RequiresInitialKey() bool {
	if prepared.state == nil {
		return false
	}
	prepared.state.mu.Lock()
	defer prepared.state.mu.Unlock()
	return prepared.state.requiresInitialKey
}
func (prepared *BlueprintBackupPolicyPreparation) SupplyInitialKey(
	material backuppolicymutations.BackupPolicyInitialKeyMaterial,
) error {
	if prepared == nil || prepared.state == nil {
		return errs.New(errs.KindInternal, "Blueprint Backup preparation is required")
	}
	state := prepared.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.consumed || !state.requiresInitialKey || state.candidate.InitialKey != nil {
		return errs.New(errs.KindStateConflict, "Blueprint Backup initial key is not expected")
	}
	initial, err := backuppolicymutations.NewInitialKey(state.environmentID, state.createdAt, &material)
	if err != nil {
		return err
	}
	state.candidate.InitialKey = initial
	return nil
}
func (prepared *BlueprintBackupPolicyPreparation) Clear() {
	if prepared == nil || prepared.state == nil {
		return
	}
	state := prepared.state
	state.mu.Lock()
	defer state.mu.Unlock()
	clearBlueprintBackupPolicyPreparationState(state)
}

func clearBlueprintBackupPolicyPreparationState(state *blueprintBackupPolicyPreparationState) {
	if state.candidate.ExistingKey != nil {
		clear(state.candidate.ExistingKey.Encrypted.Ciphertext)
		state.candidate.ExistingKey.Encrypted.Ciphertext = nil
	}
	if state.candidate.InitialKey != nil {
		clear(state.candidate.InitialKey.Encrypted.Ciphertext)
		state.candidate.InitialKey.Encrypted.Ciphertext = nil
	}
}

func (repository *BackupPolicyPlanner) PrepareEnvironmentBlueprintBackupPolicy(
	ctx context.Context,
	input EnvironmentBlueprintBackupPolicyInput,
) (BlueprintBackupPolicyPreparation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return BlueprintBackupPolicyPreparation{}, err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, input.TaskID) != nil || input.ReadRevision <= 0 ||
		input.CreatedAt.IsZero() || input.CreatedAt != input.CreatedAt.UTC() ||
		input.Projection.EnvironmentID != input.EnvironmentID || input.Projection.RevisionID != input.TaskID {
		return BlueprintBackupPolicyPreparation{}, errs.New(
			errs.KindValidationFailed, "Blueprint Backup preparation identity is invalid",
		)
	}
	if len(input.Sources) > backuppolicy.MaximumBackupPolicySources {
		return BlueprintBackupPolicyPreparation{}, errs.New(
			errs.KindValidationFailed, "backup policy may select at most 12 sources",
		)
	}
	if input.Retain && (input.Enabled || input.Frequency != "" || input.Keep != 0 || input.Encryption != "" ||
		input.ConnectorName != "" || len(input.Sources) != 0) {
		return BlueprintBackupPolicyPreparation{}, errs.New(
			errs.KindValidationFailed, "retained Blueprint Backup input cannot replace policy decisions",
		)
	}
	if err := validateBlueprintAttachTaskPreparation(input.AttachPreparation); err != nil &&
		!BlueprintAttachTaskPreparationIsZero(input.AttachPreparation) {
		return BlueprintBackupPolicyPreparation{}, err
	}
	current, coordination, key, err := repository.loadBlueprintBackupBase(
		ctx, input.EnvironmentID, input.ReadRevision, input.CreatedAt,
	)
	if err != nil {
		return BlueprintBackupPolicyPreparation{}, err
	}
	state := &blueprintBackupPolicyPreparationState{
		environmentID: input.EnvironmentID, taskID: input.TaskID, createdAt: input.CreatedAt,
		candidate: backuppolicymutations.ReplacementCandidate{
			Current: current, Coordination: coordination, ExistingKey: key,
		},
	}
	if input.Retain {
		state.retain = true
		if err := repository.prepareRetainedBlueprintBackupPolicy(ctx, state, input.ReadRevision); err != nil {
			clearBlueprintBackupPolicyPreparationState(state)
			return BlueprintBackupPolicyPreparation{}, err
		}
		return BlueprintBackupPolicyPreparation{state: state}, nil
	}
	connectorID, connector, connectorOwner, connectorName, err := repository.resolveBlueprintBackupConnector(
		ctx, input.EnvironmentID, input.ConnectorName, input.ReadRevision,
	)
	if err != nil {
		clearBlueprintBackupPolicyPreparationState(state)
		return BlueprintBackupPolicyPreparation{}, err
	}
	selections := make([]backuppolicy.BackupPolicySourceSelection, len(input.Sources))
	for index, source := range input.Sources {
		selections[index] = backuppolicy.BackupPolicySourceSelection{Kind: source.Kind, TargetID: source.TargetID}
	}
	if err := backuppolicy.ValidateReplacementInput(ctx, backuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: input.EnvironmentID, Enabled: input.Enabled, Frequency: input.Frequency,
		Keep: input.Keep, Encryption: input.Encryption, ConnectorID: connectorID, Sources: selections,
	}); err != nil {
		clearBlueprintBackupPolicyPreparationState(state)
		return BlueprintBackupPolicyPreparation{}, err
	}
	state.candidate.Connector = connector
	state.candidate.ConnectorOwnerIndex = connectorOwner
	state.connectorNameIndex = connectorName
	state.sources, err = repository.loadBlueprintBackupSources(ctx, input, input.ReadRevision)
	if err != nil {
		clearBlueprintBackupPolicyPreparationState(state)
		return BlueprintBackupPolicyPreparation{}, err
	}
	sourceIDs := make([]string, len(state.sources))
	desiredSources := make([]projectionrecord.EnvironmentBlueprintBackupPolicySource, len(state.sources))
	for index, source := range state.sources {
		sourceIDs[index] = source.record.ID
		desiredSources[index] = projectionrecord.EnvironmentBlueprintBackupPolicySource{
			ID: source.record.ID, Kind: source.record.Kind, TargetID: source.record.TargetID,
		}
	}
	state.candidate.Replacement = backuppolicy.BackupPolicyRecord{
		EnvironmentID: input.EnvironmentID, Enabled: input.Enabled, Frequency: input.Frequency,
		Keep: input.Keep, Encryption: input.Encryption, ConnectorID: connectorID,
		SourceIDs: sourceIDs, UpdatedAt: input.CreatedAt,
	}
	state.desired = &projectionrecord.EnvironmentBlueprintBackupPolicy{
		Enabled: input.Enabled, Frequency: input.Frequency, Keep: input.Keep,
		Encryption: input.Encryption, ConnectorID: connectorID, Sources: desiredSources,
	}
	if err := projectionrecord.ValidateEnvironmentBlueprintBackupPolicy(input.EnvironmentID, state.desired); err != nil {
		clearBlueprintBackupPolicyPreparationState(state)
		return BlueprintBackupPolicyPreparation{}, err
	}
	if err := backuppolicymutations.SealBackupPolicyCandidateSchedule(&state.candidate, input.CreatedAt); err != nil {
		clearBlueprintBackupPolicyPreparationState(state)
		return BlueprintBackupPolicyPreparation{}, err
	}
	state.candidate.ConnectorReferences, err = repository.policy.LoadBackupPolicyConnectorReferences(
		ctx, state.candidate, input.ReadRevision,
	)
	if err != nil {
		clearBlueprintBackupPolicyPreparationState(state)
		return BlueprintBackupPolicyPreparation{}, err
	}
	state.requiresInitialKey = input.Enabled && input.Encryption == "age" && key == nil
	return BlueprintBackupPolicyPreparation{state: state}, nil
}

func (repository *BackupPolicyPlanner) ValidateEnvironmentBlueprintBackupPolicy(
	ctx context.Context,
	input EnvironmentBlueprintBackupPolicyInput,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil || input.ReadRevision <= 0 || input.Retain {
		return errs.New(errs.KindValidationFailed, "Blueprint Backup validation identity is invalid")
	}
	connectorID, _, _, _, err := repository.resolveBlueprintBackupConnector(
		ctx, input.EnvironmentID, input.ConnectorName, input.ReadRevision,
	)
	if err != nil {
		return err
	}
	selections := make([]backuppolicy.BackupPolicySourceSelection, len(input.Sources))
	for index, source := range input.Sources {
		selections[index] = backuppolicy.BackupPolicySourceSelection{Kind: source.Kind, TargetID: source.TargetID}
	}
	return backuppolicy.ValidateReplacementInput(ctx, backuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: input.EnvironmentID, Enabled: input.Enabled, Frequency: input.Frequency,
		Keep: input.Keep, Encryption: input.Encryption, ConnectorID: connectorID, Sources: selections,
	})
}

func (repository *BackupPolicyPlanner) resolveBlueprintBackupConnector(
	ctx context.Context,
	environmentID string,
	name string,
	revision int64,
) (string, *etcdstore.Versioned[connectorrecord.Record], *etcdstore.KeyValue, *etcdstore.KeyValue, error) {
	if name == "" {
		return "", nil, nil, nil, nil
	}
	nameRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{connectorrecord.ConnectorNameKey(environmentID, name)}, Revision: revision,
	})
	if err != nil {
		return "", nil, nil, nil, err
	}
	if nameRead == nil || nameRead.ReadRevision != revision || len(nameRead.Values) != 1 {
		return "", nil, nil, nil, errs.New(errs.KindInternal, "Blueprint Backup Connector name read is incomplete")
	}
	if nameRead.Values[0] == nil {
		return "", nil, nil, nil, errs.New(errs.KindConnectorNotFound, "connector was not found")
	}
	connectorID := string(nameRead.Values[0].Value)
	if ids.Validate(ids.KindConnector, connectorID) != nil {
		return "", nil, nil, nil, connectorrecord.CorruptRecord()
	}
	connector, owner, err := repository.policy.LoadBackupPolicyConnectorEvidence(ctx, environmentID, connectorID, revision)
	if err != nil {
		return "", nil, nil, nil, err
	}
	if connector.Record.Connector.Name != name {
		return "", nil, nil, nil, connectorrecord.CorruptRecord()
	}
	tombstone, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connectorID)}, Revision: revision,
	})
	if err != nil {
		return "", nil, nil, nil, err
	}
	if tombstone == nil || tombstone.ReadRevision != revision || len(tombstone.Values) != 1 {
		return "", nil, nil, nil, errs.New(errs.KindInternal, "Blueprint Backup Connector fence read is incomplete")
	}
	if tombstone.Values[0] != nil {
		return "", nil, nil, nil, errs.New(errs.KindResourceInUse, "connector deletion is in progress")
	}
	return connectorID, connector, owner, backuppolicymutations.CloneBackupPolicyEvidenceKeyValue(nameRead.Values[0]), nil
}
