package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VolumeRemovalBackupPolicyPreparation is immutable fixed-revision evidence.
// Only the desired publisher may turn it into a policy replacement; preparation
// neither changes selection nor deletes immutable historical source records.
type VolumeRemovalBackupPolicyPreparation struct {
	state *volumeRemovalBackupPolicyState
}

type volumeRemovalBackupPolicyState struct {
	environmentID string
	volumeID      string
	policy        *backuppolicy.BackupPolicyRecord
	coordination  EnvironmentCoordinationRecord
	sources       []backuppolicy.BackupSourceRecord
	conditions    []etcdstore.Condition
}

// Projection supplies the replacement decisions for ADR0051 staging. It does
// not capture policy_now; the final publisher supplies that scheduling boundary.
func (prepared VolumeRemovalBackupPolicyPreparation) Projection() *projectionrecord.EnvironmentBlueprintBackupPolicy {
	if prepared.state == nil || prepared.state.policy == nil {
		return nil
	}
	_, projection := volumeRemovalPolicyReplacement(prepared.state)
	return projection
}

func (repository *BackupPolicyRepository) PrepareVolumeRemovalBackupPolicy(
	ctx context.Context,
	environmentID, volumeID string,
	readRevision int64,
) (VolumeRemovalBackupPolicyPreparation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return VolumeRemovalBackupPolicyPreparation{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindVolume, volumeID) != nil || readRevision <= 0 {
		return VolumeRemovalBackupPolicyPreparation{}, errs.New(
			errs.KindValidationFailed,
			"Volume policy removal identity is invalid",
		)
	}
	keys := []string{
		backuppolicy.BackupPolicyKey(environmentID),
		environmentCoordinationKey(environmentID),
		hierarchyrecord.EnvironmentMutationEpochKey(environmentID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return VolumeRemovalBackupPolicyPreparation{}, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) || read.Values[2] == nil {
		return VolumeRemovalBackupPolicyPreparation{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer clearKeyValues(read.Values)
	state := &volumeRemovalBackupPolicyState{environmentID: environmentID, volumeID: volumeID}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(read.Values[2].Value)
	if err != nil || epoch.EnvironmentID != environmentID {
		return VolumeRemovalBackupPolicyPreparation{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	state.conditions = []etcdstore.Condition{
		{Key: keys[0]}, {Key: keys[1]},
		{Key: keys[2], ModRevision: read.Values[2].ModRevision},
	}
	if read.Values[1] != nil {
		state.coordination, err = decodeEnvironmentCoordinationRecord(read.Values[1].Value)
		if err != nil || state.coordination.EnvironmentID != environmentID {
			return VolumeRemovalBackupPolicyPreparation{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		state.conditions[1].ModRevision = read.Values[1].ModRevision
	}
	if read.Values[0] == nil {
		return VolumeRemovalBackupPolicyPreparation{state: state}, nil
	}
	if read.Values[1] == nil {
		return VolumeRemovalBackupPolicyPreparation{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	policy, err := backuppolicy.DecodeBackupPolicyRecord(read.Values[0].Value)
	if err != nil || policy.EnvironmentID != environmentID || len(policy.SourceIDs) > backuppolicy.MaximumBackupPolicySources {
		return VolumeRemovalBackupPolicyPreparation{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	state.policy = &policy
	state.conditions[0].ModRevision = read.Values[0].ModRevision
	if err := repository.loadVolumeRemovalPolicySources(ctx, state, readRevision); err != nil {
		return VolumeRemovalBackupPolicyPreparation{}, err
	}
	return VolumeRemovalBackupPolicyPreparation{state: state}, nil
}

type volumeRemovalBackupPolicyPublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	projection *projectionrecord.EnvironmentBlueprintBackupPolicy
}

func prepareVolumeRemovalBackupPolicyPublication(
	prepared VolumeRemovalBackupPolicyPreparation,
	policyNow time.Time,
) (volumeRemovalBackupPolicyPublication, error) {
	state := prepared.state
	if state == nil || !backuppolicy.ValidUTCInstant(policyNow) {
		return volumeRemovalBackupPolicyPublication{}, errs.New(
			errs.KindValidationFailed,
			"Volume policy preparation is required",
		)
	}
	publication := volumeRemovalBackupPolicyPublication{conditions: append([]etcdstore.Condition(nil), state.conditions...)}
	if state.policy == nil {
		return publication, nil
	}
	replacement, projection := volumeRemovalPolicyReplacement(state)
	replacement.UpdatedAt = policyNow
	coordination, _, err := replaceEnvironmentCoordinationSchedule(state.coordination, replacement, policyNow)
	if err != nil {
		return volumeRemovalBackupPolicyPublication{}, err
	}
	policyValue, err := backuppolicy.EncodeBackupPolicyRecord(replacement)
	if err != nil {
		return volumeRemovalBackupPolicyPublication{}, err
	}
	coordinationValue, err := encodeEnvironmentCoordinationRecord(coordination)
	if err != nil {
		clear(policyValue)
		return volumeRemovalBackupPolicyPublication{}, err
	}
	publication.projection = projection
	publication.mutations = []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: backuppolicy.BackupPolicyKey(state.environmentID), Value: policyValue},
		{Type: etcdstore.MutationPut, Key: environmentCoordinationKey(state.environmentID), Value: coordinationValue},
	}
	if state.policy.Enabled && !replacement.Enabled {
		publication.mutations = append(publication.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: backuppolicy.BackupPolicyConnectorReferenceKey(state.policy.ConnectorID, state.environmentID),
		})
	}
	return publication, nil
}

func volumeRemovalPolicyReplacement(
	state *volumeRemovalBackupPolicyState,
) (backuppolicy.BackupPolicyRecord, *projectionrecord.EnvironmentBlueprintBackupPolicy) {
	replacement := *state.policy
	replacement.SourceIDs = nil
	projection := &projectionrecord.EnvironmentBlueprintBackupPolicy{
		Enabled: replacement.Enabled, Frequency: replacement.Frequency, Keep: replacement.Keep,
		Encryption: replacement.Encryption, ConnectorID: replacement.ConnectorID,
	}
	for _, source := range state.sources {
		if source.Kind == core.BackupSourceVolume && source.TargetID == state.volumeID {
			continue
		}
		replacement.SourceIDs = append(replacement.SourceIDs, source.ID)
		projection.Sources = append(projection.Sources, projectionrecord.EnvironmentBlueprintBackupPolicySource{
			ID: source.ID, Kind: source.Kind, TargetID: source.TargetID,
		})
	}
	if len(replacement.SourceIDs) == 0 {
		replacement.Enabled, projection.Enabled = false, false
	}
	return replacement, projection
}

func (prepared VolumeRemovalBackupPolicyPreparation) validateDesiredPublication(
	claim EnvironmentBlueprintStageClaim,
	projection projectionrecord.EnvironmentComposeProjection,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	removedVolumeID string,
) error {
	state := prepared.state
	if state == nil || claim.SourceKind != EnvironmentBlueprintSourceMutation ||
		state.environmentID != claim.EnvironmentID || state.volumeID != removedVolumeID ||
		task.Type != TaskRemove || task.Target != state.volumeID ||
		task.Params[TaskResourceKindParam] != TaskResourceVolume ||
		marker.Locator.Method != "DELETE" || marker.Locator.Route != "/volumes/{id}" ||
		!equalEnvironmentBlueprintBackupPolicy(prepared.Projection(), projection.Backup) {
		return errs.New(errs.KindValidationFailed, "Volume policy preparation does not match desired removal")
	}
	return nil
}

// The shared publisher already compares the Environment mutation epoch.
// Equal fences collapse; different revisions reject rather than losing either
// owner's fixed-revision authority to fit the publication budget.
func (publication volumeRemovalBackupPolicyPublication) withExistingComparisons(
	existing []etcdstore.Condition,
) (volumeRemovalBackupPolicyPublication, error) {
	seen := make(map[string]etcdstore.Condition, len(existing)+len(publication.conditions))
	for _, condition := range existing {
		seen[condition.Key] = condition
	}
	remaining := make([]etcdstore.Condition, 0, len(publication.conditions))
	for _, condition := range publication.conditions {
		if previous, found := seen[condition.Key]; found {
			if previous != condition {
				return volumeRemovalBackupPolicyPublication{}, errs.New(
					errs.KindStateConflict, "Volume policy publication source revisions disagree",
				)
			}
			continue
		}
		seen[condition.Key] = condition
		remaining = append(remaining, condition)
	}
	publication.conditions = remaining
	return publication, nil
}

func (publication volumeRemovalBackupPolicyPublication) classifyConflict(
	baseCount int,
	previous idempotencyPlanClassifier,
) idempotencyPlanClassifier {
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseCount+len(publication.conditions) {
			return errs.New(errs.KindInternal, "Volume policy publication compare evidence is incomplete")
		}
		if err := previous(revision, values[:baseCount]); err != nil {
			return err
		}
		for index, condition := range publication.conditions {
			value := values[baseCount+index]
			if value != nil && value.Key != condition.Key {
				return errs.New(errs.KindInternal, "Volume policy publication compare evidence is corrupt")
			}
			if (condition.ModRevision == 0 && value != nil) || (condition.ModRevision > 0 &&
				(value == nil || value.ModRevision != condition.ModRevision)) {
				return errs.New(errs.KindStateConflict, "Volume policy publication source changed")
			}
		}
		return nil
	}
}
