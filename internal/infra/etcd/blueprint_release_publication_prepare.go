package etcd

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintReleasePublication seals the executed artifact, release marker,
// indexes, Script checkpoints, and prepared source root into one opaque fragment.
func (ledger *ReleaseLedger) PrepareBlueprintReleasePublication(
	ctx context.Context,
	authority *ScriptSourceReferenceAuthority,
	evidence BlueprintReleasePublicationEvidence,
) (BlueprintReleasePublication, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || evidence.Manifest.Revision <= 0 ||
		evidence.Manifest.Record.OperationID != evidence.Task.OperationID ||
		evidence.Manifest.Record.PublicationID != evidence.Task.Params[TaskReleasePublicationParam] ||
		evidence.Task.Owner.EnvironmentID != evidence.EnvironmentID || evidence.Task.Type != TaskUpdate ||
		evidence.PublishedAt.IsZero() || evidence.PublishedAt.Location() != time.UTC {
		return BlueprintReleasePublication{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Release publication evidence is invalid",
		)
	}
	if _, err := validateReleaseCandidateDescriptor(evidence.CandidateReleaseDescriptor, evidence.Task, evidence.Manifest.Record); err != nil {
		return BlueprintReleasePublication{}, err
	}
	artifact, err := blueprintExecutedArtifact(evidence)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	runtimes, err := executionplan.PrepareBlueprintRuntimeInputs(
		evidence.Plan,
		evidence.Task.Params[TaskComposeArtifactParam],
	)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	conditions := []Condition{
		{
			Key:         releaseManifestStagingKey(evidence.Manifest.Record.PublicationID),
			ModRevision: evidence.Manifest.Revision,
		},
		{Key: releasePublicationKey(evidence.Manifest.Record.PublicationID)},
	}
	nativeConditions, err := ledger.blueprintNativePredecessorConditions(ctx, evidence)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	conditions = append(conditions, nativeConditions...)
	primaryConditions, err := ledger.blueprintScriptPrimaryConditions(ctx, evidence.Hooks)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	conditions = append(conditions, primaryConditions...)
	nativeReferences, err := blueprintNativePredecessorReferences(evidence.NativePredecessors)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	publicationValue, err := encodeReleaseRecord("release-publication", ReleasePublicationMarker{
		PublicationID:              evidence.Manifest.Record.PublicationID,
		OperationID:                evidence.Manifest.Record.OperationID,
		ManifestDigest:             evidence.Manifest.Record.Digest,
		CandidateReleaseDescriptor: executionplan.CloneCandidateReleaseDescriptor(evidence.CandidateReleaseDescriptor),
		ExecutedComposeArtifact:    artifact,
		BlueprintRuntimes:          runtimes,
		NativePredecessors:         nativeReferences,
		PublishedAt:                evidence.PublishedAt,
	})
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	mutations := []Mutation{
		{
			Type:  MutationPut,
			Key:   releasePublicationKey(evidence.Manifest.Record.PublicationID),
			Value: publicationValue,
		},
	}
	for _, member := range evidence.Manifest.Record.Members {
		environmentValue, encodeErr := json.Marshal(releaseEnvironmentIndexValue{
			Schema: 1, ServiceID: member.ServiceID, PublicationID: evidence.Manifest.Record.PublicationID,
		})
		if encodeErr != nil {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		serviceValue, encodeErr := json.Marshal(
			releaseServiceIndexValue{Schema: 1, PublicationID: evidence.Manifest.Record.PublicationID},
		)
		if encodeErr != nil {
			clear(environmentValue)
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		mutations = append(
			mutations,
			Mutation{
				Type:  MutationPut,
				Key:   releaseEnvironmentIndexKey(evidence.EnvironmentID, member.ReleaseID),
				Value: environmentValue,
			},
			Mutation{
				Type:  MutationPut,
				Key:   releaseServiceIndexKey(evidence.EnvironmentID, member.ServiceID, member.ReleaseID),
				Value: serviceValue,
			},
		)
	}
	if len(evidence.Hooks) != 0 {
		if evidence.HookPrepared.key != blueprintReleaseHookStageKey(evidence.Task.OperationID) ||
			evidence.HookPrepared.revision <= 0 {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint hook preparation is incomplete",
			)
		}
		conditions = append(
			conditions,
			Condition{Key: evidence.HookPrepared.key, ModRevision: evidence.HookPrepared.revision},
		)
		for _, hook := range evidence.Hooks {
			conditions = append(conditions, scriptAttachSourceConditions(hook.Sources.AttachSources)...)
		}
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: evidence.HookPrepared.key})
	}
	var sourceFragment ScriptSourcePublicationFragment
	if !evidence.SourcePrepared.IsZero() {
		if authority == nil || len(evidence.SourceMembers) == 0 {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script source publication is incomplete",
			)
		}
		sourceFragment, err = authority.FinalPublicationFragment(ctx, evidence.SourcePrepared)
		if err != nil {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, err
		}
	}
	publication, err := newBlueprintReleasePublication(blueprintReleasePublicationInput{
		EnvironmentID: evidence.EnvironmentID, OperationID: evidence.Task.OperationID,
		Conditions: conditions, Mutations: mutations, SourceFragment: sourceFragment,
		SourceAuthority: authority, SourceMembers: evidence.SourceMembers,
	})
	clearMutations(mutations)
	if err != nil {
		sourceFragment.Clear()
		return BlueprintReleasePublication{}, err
	}
	return publication, nil
}
