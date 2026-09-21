package scriptsourceevidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ScriptSourcePlatformOwner         = "platform/-"
	scriptSourceMaterializationPrefix = "/v1/records/materialization-proofs/"
)

const (
	ScriptOperationSourceActive    = "active"
	ScriptOperationSourceReleasing = "releasing"
	ScriptSourceReleaseAbsent      = "absent"
	ScriptSourceReleaseNormal      = "normal_completion"
	ScriptSourceReleaseRetryExpiry = "retry_expiry"
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
	Reference ref.Reference
	Evidence  ScriptSourceEvidence
}

func SameScriptCandidateStage(left, right ScriptCandidateSourceStage) bool {
	return left.EnvironmentID == right.EnvironmentID && left.RevisionID == right.RevisionID &&
		left.RenderGeneration == right.RenderGeneration && left.FixedReadRevision == right.FixedReadRevision
}

func ScriptSourceKindMayBeBlueprintStaged(kind ref.SourceKind) bool {
	switch kind {
	case ref.SourceRunnerSnapshot, ref.SourceService, ref.SourceRelease,
		ref.SourceEntryValue:
		return true
	default:
		return false
	}
}

func ValidateScriptCandidateSourceStage(
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

func ScriptCandidateSourceStageToReference(stage ScriptCandidateSourceStage) ref.StageIdentity {
	return ref.StageIdentity{
		EnvironmentID: stage.EnvironmentID, RevisionID: stage.RevisionID,
		RenderGeneration: stage.RenderGeneration, FixedReadRevision: stage.FixedReadRevision,
		CanonicalValueSHA256: hex.EncodeToString(stage.CanonicalValueSHA256[:]),
	}
}

func ScriptCandidateSourceStageFromReference(stage ref.StageIdentity) ScriptCandidateSourceStage {
	result := ScriptCandidateSourceStage{
		EnvironmentID: stage.EnvironmentID, RevisionID: stage.RevisionID,
		RenderGeneration: stage.RenderGeneration, FixedReadRevision: stage.FixedReadRevision,
	}
	decoded, _ := hex.DecodeString(stage.CanonicalValueSHA256)
	copy(result.CanonicalValueSHA256[:], decoded)
	clear(decoded)
	return result
}
