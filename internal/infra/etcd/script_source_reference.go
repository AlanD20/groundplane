package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	scriptSourcePreparationPrefix      = ref.PreparationPrefix
	scriptSourceRootPrefix             = ref.RootPrefix
	scriptSourceForwardReferencePrefix = ref.ForwardReferencePrefix
	scriptSourceCountPrefix            = ref.CountPrefix
	scriptSourcePlatformOwner          = "platform/-"
	scriptSourceMaterializationPrefix  = "/v1/records/materialization-proofs/"
)

type ScriptSourceKind = ref.SourceKind

const (
	ScriptSourceBody            = ref.SourceBody
	ScriptSourceRunnerSnapshot  = ref.SourceRunnerSnapshot
	ScriptSourceService         = ref.SourceService
	ScriptSourceRelease         = ref.SourceRelease
	ScriptSourceNetwork         = ref.SourceNetwork
	ScriptSourceVolume          = ref.SourceVolume
	ScriptSourceEntryValue      = ref.SourceEntryValue
	ScriptSourceSecretValue     = ref.SourceSecretValue
	ScriptSourceMaterialization = ref.SourceMaterialization
)

type ScriptSourceIdentity = ref.SourceIdentity
type ScriptSourceReference = ref.Reference
type ScriptSourceCount = ref.Count
type ScriptSourcePreparation = ref.Preparation
type ScriptOperationSourceRoot = ref.OperationSourceRoot
type ScriptSourcePreparationPhase = ref.PreparationPhase

const (
	ScriptSourcePreparationPreparing  = ref.PreparationPreparing
	ScriptSourcePreparationSealed     = ref.PreparationSealed
	ScriptSourcePreparationAbandoning = ref.PreparationAbandoning
	ScriptOperationSourceActive       = "active"
	ScriptOperationSourceReleasing    = "releasing"
	ScriptSourceReleaseAbsent         = "absent"
)

type ScriptExistingSourceEvidence struct{ SourceKey string }

type ScriptCandidateSourceStage struct {
	EnvironmentID        string
	RevisionID           string
	RenderGeneration     uint64
	FixedReadRevision    int64
	CanonicalValueSHA256 [sha256.Size]byte
}

type ScriptStagedSourceEvidence struct {
	SourceKey string
	Stage     ScriptCandidateSourceStage
	Value     []byte
}

// ScriptSourceEvidence is a closed existing-vs-staged union. Only exact candidate
// Blueprint mutations admitted by scriptSourceKindMayBeBlueprintStaged may be staged.
type ScriptSourceEvidence struct {
	Existing *ScriptExistingSourceEvidence
	Staged   *ScriptStagedSourceEvidence
}

type ScriptSourcePreparationMember struct {
	Reference ScriptSourceReference
	Evidence  ScriptSourceEvidence
}

type PreparedSourceSet struct {
	prepared           ref.Prepared
	descriptorRevision int64
	membershipCount    uint64
	membershipSHA256   string
}

func (prepared PreparedSourceSet) IsZero() bool { return prepared.prepared.IsZero() }

type ScriptStagedSourceRequirement struct {
	Source        ScriptSourceIdentity
	SourceKey     string
	SourceOwnerID string
	SourceDigest  string
	Stage         ScriptCandidateSourceStage
	value         []byte
}

func (requirement ScriptStagedSourceRequirement) Matches(mutation Mutation) bool {
	return mutation.Type == MutationPut && !mutation.Prefix && mutation.Key == requirement.SourceKey &&
		bytes.Equal(mutation.Value, requirement.value)
}

type ScriptSourcePublicationFragment struct {
	conditions []Condition
	mutations  []Mutation
	staged     []ScriptStagedSourceRequirement
}

func (fragment ScriptSourcePublicationFragment) StagedRequirements() []ScriptStagedSourceRequirement {
	result := make([]ScriptStagedSourceRequirement, len(fragment.staged))
	for index, requirement := range fragment.staged {
		result[index] = requirement
		result[index].value = append([]byte(nil), requirement.value...)
	}
	return result
}

func (fragment ScriptSourcePublicationFragment) ValidateStagedMutations(
	stage EnvironmentBlueprintStageClaim,
	mutations []Mutation,
) error {
	matched := make([]bool, len(fragment.staged))
	for _, requirement := range fragment.staged {
		if requirement.Stage.EnvironmentID != stage.EnvironmentID ||
			requirement.Stage.RevisionID != stage.RevisionID ||
			requirement.Stage.RenderGeneration != stage.RenderGeneration {
			return errs.New(errs.KindValidationFailed, "staged Script source candidate identity changed")
		}
	}
	for _, mutation := range mutations {
		for index, requirement := range fragment.staged {
			if mutation.Key != requirement.SourceKey {
				continue
			}
			if !requirement.Matches(mutation) {
				return errs.New(errs.KindValidationFailed, "staged Script source mutation changed")
			}
			if matched[index] {
				return errs.New(errs.KindValidationFailed, "staged Script source mutation is duplicated")
			}
			matched[index] = true
		}
	}
	for _, found := range matched {
		if !found {
			return errs.New(errs.KindValidationFailed, "staged Script source mutation is missing or changed")
		}
	}
	return nil
}

func (fragment *ScriptSourcePublicationFragment) Clear() {
	if fragment == nil {
		return
	}
	clearMutationValues(fragment.mutations)
	for index := range fragment.staged {
		clear(fragment.staged[index].value)
	}
	*fragment = ScriptSourcePublicationFragment{}
}

type ScriptSourceReferenceAuthority struct {
	store      hierarchyStore
	repository *ref.Repository
}

func newScriptSourceReferenceAuthority(store hierarchyStore) (*ScriptSourceReferenceAuthority, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Script source reference store is required")
	}
	adapter := &scriptSourceReferenceStore{store: store}
	repository, err := ref.NewRepository(adapter, adapter)
	if err != nil {
		return nil, mapScriptSourceReferenceError(err)
	}
	return &ScriptSourceReferenceAuthority{store: store, repository: repository}, nil
}

func (authority *ScriptSourceReferenceAuthority) Prepare(
	ctx context.Context,
	operationID string,
	members []ScriptSourcePreparationMember,
) (PreparedSourceSet, error) {
	converted, err := authority.validateMembers(ctx, operationID, members, true)
	if err != nil {
		return PreparedSourceSet{}, err
	}
	prepared, err := authority.repository.Prepare(ctx, operationID, converted)
	if err != nil {
		return PreparedSourceSet{}, mapScriptSourceReferenceError(err)
	}
	return PreparedSourceSet{
		prepared: prepared, descriptorRevision: prepared.DescriptorRevision(),
		membershipCount: prepared.MembershipCount(), membershipSHA256: prepared.MembershipSHA256(),
	}, nil
}

// Abandon releases a matching preparation's exact committed membership prefix.
// Current source records are deliberately not re-adopted during cleanup.
func (authority *ScriptSourceReferenceAuthority) Abandon(
	ctx context.Context,
	operationID string,
	members []ScriptSourcePreparationMember,
) error {
	converted, err := authority.validateMembers(ctx, operationID, members, false)
	if err != nil {
		return err
	}
	return mapScriptSourceReferenceError(authority.repository.Abandon(ctx, operationID, converted))
}

func (authority *ScriptSourceReferenceAuthority) FinalPublicationFragment(
	ctx context.Context,
	prepared PreparedSourceSet,
) (ScriptSourcePublicationFragment, error) {
	fragment, err := authority.repository.FinalPublicationFragment(ctx, prepared.prepared)
	if err != nil {
		return ScriptSourcePublicationFragment{}, mapScriptSourceReferenceError(err)
	}
	result := ScriptSourcePublicationFragment{
		conditions: convertScriptSourceConditions(fragment.Conditions),
		mutations:  convertScriptSourceMutations(fragment.Mutations),
		staged:     make([]ScriptStagedSourceRequirement, len(fragment.StagedRequirements)),
	}
	for index, requirement := range fragment.StagedRequirements {
		result.staged[index] = ScriptStagedSourceRequirement{
			Source: requirement.Source, SourceKey: requirement.SourceKey,
			SourceOwnerID: requirement.SourceOwnerID, SourceDigest: requirement.SourceDigest,
			Stage: scriptCandidateSourceStageFromReference(requirement.Stage),
			value: append([]byte(nil), requirement.Value...),
		}
	}
	fragment.Clear()
	return result, nil
}

func (authority *ScriptSourceReferenceAuthority) validateMembers(
	ctx context.Context,
	operationID string,
	members []ScriptSourcePreparationMember,
	readRecords bool,
) ([]ref.Member, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if authority == nil || authority.store == nil || authority.repository == nil || len(members) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Script source preparation is invalid")
	}
	converted := make([]ref.Member, len(members))
	var candidateStage *ScriptCandidateSourceStage
	for index, member := range members {
		if member.Reference.OperationID != operationID || validateScriptSourceReference(member.Reference) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script source preparation member is invalid")
		}
		existing, staged := member.Evidence.Existing, member.Evidence.Staged
		if (existing == nil) == (staged == nil) {
			return nil, errs.New(errs.KindValidationFailed, "Script source evidence union is invalid")
		}
		converted[index].Reference = member.Reference
		if existing != nil {
			converted[index].SourceKey, converted[index].Mode = existing.SourceKey, ref.EvidenceExisting
			if member.Reference.SourceModRevision <= 0 || existing.SourceKey == "" {
				return nil, errs.New(errs.KindValidationFailed, "existing Script source evidence is invalid")
			}
			if readRecords {
				if err := authority.validateExistingSource(ctx, converted[index]); err != nil {
					return nil, err
				}
			}
			continue
		}
		converted[index].SourceKey, converted[index].Mode = staged.SourceKey, ref.EvidenceStaged
		converted[index].Stage = scriptCandidateSourceStageToReference(staged.Stage)
		converted[index].StagedValue = append([]byte(nil), staged.Value...)
		if member.Reference.SourceModRevision != 0 ||
			!scriptSourceKindMayBeBlueprintStaged(member.Reference.Source.Kind) {
			return nil, errs.New(errs.KindValidationFailed, "staged Script source kind is invalid")
		}
		if err := validateScriptCandidateSourceStage(staged.Stage, staged.Value, member.Reference.SourceOwnerID); err != nil {
			return nil, err
		}
		if candidateStage != nil && !sameScriptCandidateStage(*candidateStage, staged.Stage) {
			return nil, errs.New(errs.KindValidationFailed, "staged Script source candidate identity conflicts")
		}
		stageCopy := staged.Stage
		candidateStage = &stageCopy
		if err := validateScriptSourceRecord(staged.SourceKey, staged.Value, member.Reference); err != nil {
			return nil, err
		}
		if readRecords {
			read, readErr := authority.store.GetMany(ctx, GetManyRequest{
				Keys: []string{staged.SourceKey}, Revision: staged.Stage.FixedReadRevision,
			})
			if readErr != nil {
				return nil, readErr
			}
			if read == nil || read.ReadRevision != staged.Stage.FixedReadRevision ||
				len(read.Values) != 1 || read.Values[0] != nil {
				return nil, errs.New(errs.KindStateConflict, "staged Script source key is already occupied")
			}
		}
	}
	return converted, nil
}

func sameScriptCandidateStage(left, right ScriptCandidateSourceStage) bool {
	return left.EnvironmentID == right.EnvironmentID && left.RevisionID == right.RevisionID &&
		left.RenderGeneration == right.RenderGeneration && left.FixedReadRevision == right.FixedReadRevision
}

func scriptSourceKindMayBeBlueprintStaged(kind ScriptSourceKind) bool {
	switch kind {
	case ScriptSourceRunnerSnapshot, ScriptSourceService, ScriptSourceRelease,
		ScriptSourceNetwork, ScriptSourceVolume, ScriptSourceEntryValue:
		return true
	default:
		return false
	}
}

func validateScriptCandidateSourceStage(
	stage ScriptCandidateSourceStage,
	value []byte,
	ownerID string,
) error {
	digest := sha256.Sum256(value)
	if ids.Validate(ids.KindEnvironment, stage.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, stage.RevisionID) != nil || stage.EnvironmentID != ownerID ||
		stage.RenderGeneration == 0 || stage.FixedReadRevision <= 0 ||
		!bytes.Equal(stage.CanonicalValueSHA256[:], digest[:]) {
		return errs.New(errs.KindValidationFailed, "staged Script source authority is invalid")
	}
	return nil
}

func scriptCandidateSourceStageToReference(stage ScriptCandidateSourceStage) ref.StageIdentity {
	return ref.StageIdentity{
		EnvironmentID: stage.EnvironmentID, RevisionID: stage.RevisionID,
		RenderGeneration: stage.RenderGeneration, FixedReadRevision: stage.FixedReadRevision,
		CanonicalValueSHA256: hex.EncodeToString(stage.CanonicalValueSHA256[:]),
	}
}

func scriptCandidateSourceStageFromReference(stage ref.StageIdentity) ScriptCandidateSourceStage {
	result := ScriptCandidateSourceStage{
		EnvironmentID: stage.EnvironmentID, RevisionID: stage.RevisionID,
		RenderGeneration: stage.RenderGeneration, FixedReadRevision: stage.FixedReadRevision,
	}
	decoded, _ := hex.DecodeString(stage.CanonicalValueSHA256)
	copy(result.CanonicalValueSHA256[:], decoded)
	clear(decoded)
	return result
}

func (authority *ScriptSourceReferenceAuthority) validateExistingSource(ctx context.Context, member ref.Member) error {
	read, err := authority.store.GetMany(ctx, GetManyRequest{
		Keys: []string{member.SourceKey}, Revision: member.Reference.SourceModRevision,
	})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].ModRevision != member.Reference.SourceModRevision {
		return errs.New(errs.KindStateConflict, "existing Script source revision is unavailable")
	}
	if err := validateScriptSourceRecord(member.SourceKey, read.Values[0].Value, member.Reference); err != nil {
		return err
	}
	if member.Reference.Source.Kind != ScriptSourceSecretValue {
		return nil
	}
	metadata, err := authority.store.GetMany(ctx, GetManyRequest{
		Keys: []string{secretRecordKey(member.Reference.Source.SecretID)}, Revision: member.Reference.SourceModRevision,
	})
	if err != nil {
		return err
	}
	if metadata == nil || len(metadata.Values) != 1 || metadata.Values[0] == nil {
		return errs.New(errs.KindValidationFailed, "Script Secret source owner evidence is missing")
	}
	record, err := decodeSecretRecord(metadata.Values[0].Value)
	if err != nil {
		return errs.New(errs.KindValidationFailed, "Script Secret source owner evidence is invalid")
	}
	ownerID := scriptSourcePlatformOwner
	if record.Secret.ProjectID != "" {
		ownerID = record.Secret.ProjectID
	}
	if ownerID != member.Reference.SourceOwnerID {
		return errs.New(errs.KindValidationFailed, "Script Secret source owner evidence is invalid")
	}
	return nil
}
