package blueprints

import (
	"bytes"
	"crypto/sha256"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

// ConfigRestoreRevision uses the same immutable desired revision encoding as
// ordinary mutations. Its owner is an already-published Restore Task, not a
// second public idempotency claim or a different desired-state selector.
type ConfigRestoreRevision struct {
	Seal         EnvironmentBlueprintSeal
	Audit        []byte
	DesiredInput []byte
}

func BuildConfigRestoreRevision(input projectionrecord.EnvironmentDesiredInput,
	audit EnvironmentConfigRestoreAudit, baselineHeadRevision int64, dependencyDigest [sha256.Size]byte,
) (ConfigRestoreRevision, error) {
	var result ConfigRestoreRevision
	encodedAudit, err := encodeEnvironmentDesiredMutationAudit(EnvironmentDesiredMutationAudit{ConfigRestore: &audit})
	if err != nil {
		return result, err
	}
	encodedInput, resources, err := encodeDesiredRevisionInput(input)
	if err != nil {
		clear(encodedAudit)
		return result, err
	}
	seal := EnvironmentBlueprintSeal{EnvironmentID: input.EnvironmentID, RevisionID: input.RevisionID,
		SourceKind: EnvironmentBlueprintSourceMutation, RenderGeneration: input.RenderGeneration,
		ProjectionSchema: EnvironmentDesiredInputSchema, BaselineHeadRevision: baselineHeadRevision,
		AuditChunks: ChunkCount32(
			len(encodedAudit),
		), AuditBytes: uint64(len(encodedAudit)), AuditSHA256: sha256.Sum256(encodedAudit),
		ProjectionChunks: ChunkCount32(len(encodedInput)), ProjectionBytes: uint64(len(encodedInput)),
		ProjectionSHA256: sha256.Sum256(
			encodedInput,
		), ProjectionResources: resources, DependencyDigest: dependencyDigest}
	if baselineHeadRevision <= 0 || resources > 512 || validateEnvironmentBlueprintSeal(seal) != nil {
		clear(encodedAudit)
		clear(encodedInput)
		return result, CorruptEnvironmentBlueprintStage()
	}
	return ConfigRestoreRevision{Seal: seal, Audit: encodedAudit, DesiredInput: encodedInput}, nil
}

func (revision *ConfigRestoreRevision) Clear() {
	clear(revision.Audit)
	clear(revision.DesiredInput)
	revision.Audit, revision.DesiredInput = nil, nil
}

// Validate binds both canonical streams to the native Restore's allocated
// identity. Staging reuses ordinary revision keys and readers, not a second
// desired-state format or a new public idempotency claim.
func (revision ConfigRestoreRevision) Validate(environmentID, taskID string, renderGeneration uint64,
	baselineHeadRevision int64, entryCount uint32, audit EnvironmentConfigRestoreAudit,
) error {
	seal := revision.Seal
	if validateEnvironmentBlueprintSeal(seal) != nil || seal.EnvironmentID != environmentID ||
		seal.RevisionID != taskID || seal.RenderGeneration != renderGeneration ||
		seal.SourceKind != EnvironmentBlueprintSourceMutation || seal.BaselineHeadRevision != baselineHeadRevision ||
		uint64(len(revision.Audit)) != seal.AuditBytes || uint64(len(revision.DesiredInput)) != seal.ProjectionBytes ||
		sha256.Sum256(
			revision.Audit,
		) != seal.AuditSHA256 || sha256.Sum256(revision.DesiredInput) != seal.ProjectionSHA256 {
		return CorruptEnvironmentBlueprintStage()
	}
	expectedAudit, err := encodeEnvironmentDesiredMutationAudit(EnvironmentDesiredMutationAudit{ConfigRestore: &audit})
	if err != nil {
		return err
	}
	defer clear(expectedAudit)
	if !bytes.Equal(expectedAudit, revision.Audit) {
		return CorruptEnvironmentBlueprintStage()
	}
	input, err := projectionrecord.DecodeEnvironmentDesiredInputStorage(revision.DesiredInput)
	if err != nil || input.EnvironmentID != environmentID || input.RevisionID != taskID ||
		input.RenderGeneration != renderGeneration || uint32(len(input.Input.Entries)) != entryCount {
		return CorruptEnvironmentBlueprintStage()
	}
	canonical, resources, err := encodeDesiredRevisionInput(input)
	defer clear(canonical)
	if err != nil || resources > 512 || resources != seal.ProjectionResources ||
		!bytes.Equal(canonical, revision.DesiredInput) {
		return CorruptEnvironmentBlueprintStage()
	}
	return nil
}
