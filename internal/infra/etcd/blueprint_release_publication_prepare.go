package etcd

import (
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
		evidence.Manifest.Record.PublicationID != evidence.Task.Params[releaserender.TaskReleasePublicationParam] ||
		evidence.Task.Owner.EnvironmentID != evidence.EnvironmentID || evidence.Task.Type != taskjournal.TaskUpdate ||
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
		evidence.Task.Params[taskjournal.TaskComposeArtifactParam],
	)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	conditions := []etcdstore.Condition{
		{
			Key:         releases.ReleaseManifestStagingKey(evidence.Manifest.Record.PublicationID),
			ModRevision: evidence.Manifest.Revision,
		},
		{Key: releases.ReleasePublicationKey(evidence.Manifest.Record.PublicationID)},
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
	publicationValue, err := releases.EncodeReleaseRecord("release-publication", releases.ReleasePublicationMarker{
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
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   releases.ReleasePublicationKey(evidence.Manifest.Record.PublicationID),
			Value: publicationValue,
		},
	}
	for _, member := range evidence.Manifest.Record.Members {
		environmentValue, encodeErr := json.Marshal(releases.ReleaseEnvironmentIndexValue{
			Schema: 1, ServiceID: member.ServiceID, PublicationID: evidence.Manifest.Record.PublicationID,
		})
		if encodeErr != nil {
			etcdstore.ZeroMutationBytes(mutations)
			return BlueprintReleasePublication{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		serviceValue, encodeErr := json.Marshal(
			releases.ReleaseServiceIndexValue{Schema: 1, PublicationID: evidence.Manifest.Record.PublicationID},
		)
		if encodeErr != nil {
			clear(environmentValue)
			etcdstore.ZeroMutationBytes(mutations)
			return BlueprintReleasePublication{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   releases.ReleaseEnvironmentIndexKey(evidence.EnvironmentID, member.ReleaseID),
				Value: environmentValue,
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   releases.ReleaseServiceIndexKey(evidence.EnvironmentID, member.ServiceID, member.ReleaseID),
				Value: serviceValue,
			},
		)
	}
	if len(evidence.Hooks) != 0 {
		if evidence.HookPrepared.key != blueprintReleaseHookStageKey(evidence.Task.OperationID) ||
			evidence.HookPrepared.revision <= 0 {
			etcdstore.ZeroMutationBytes(mutations)
			return BlueprintReleasePublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint hook preparation is incomplete",
			)
		}
		conditions = append(
			conditions,
			etcdstore.Condition{Key: evidence.HookPrepared.key, ModRevision: evidence.HookPrepared.revision},
		)
		for _, hook := range evidence.Hooks {
			conditions = append(conditions, scriptAttachSourceConditions(hook.Sources.AttachSources)...)
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: evidence.HookPrepared.key})
	}
	var sourceFragment ScriptSourcePublicationFragment
	if !evidence.SourcePrepared.IsZero() {
		if authority == nil || len(evidence.SourceMembers) == 0 {
			etcdstore.ZeroMutationBytes(mutations)
			return BlueprintReleasePublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script source publication is incomplete",
			)
		}
		sourceFragment, err = authority.FinalPublicationFragment(ctx, evidence.SourcePrepared)
		if err != nil {
			etcdstore.ZeroMutationBytes(mutations)
			return BlueprintReleasePublication{}, err
		}
	}
	publication, err := newBlueprintReleasePublication(blueprintReleasePublicationInput{
		EnvironmentID: evidence.EnvironmentID, OperationID: evidence.Task.OperationID,
		Conditions: conditions, Mutations: mutations, SourceFragment: sourceFragment,
		SourceAuthority: authority, SourceMembers: evidence.SourceMembers,
	})
	etcdstore.ZeroMutationBytes(mutations)
	if err != nil {
		sourceFragment.Clear()
		return BlueprintReleasePublication{}, err
	}
	return publication, nil
}
