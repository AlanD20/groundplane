package etcd

import (
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareReleaseWorkPublication seals the executed artifact, release marker,
// indexes, Script checkpoints, and prepared source root into one opaque fragment.
func (ledger *ReleaseLedger) PrepareReleaseWorkPublication(
	ctx context.Context,
	authority *scriptsourcepublication.Authority,
	evidence ReleaseWorkPublicationEvidence,
) (ReleaseWorkPublication, error) {
	work, prepared := evidence.Work, evidence.PreparedInput
	if ctx == nil || ledger == nil || ledger.store == nil || evidence.Manifest.Revision <= 0 ||
		evidence.ServiceRollout == nil ||
		evidence.Manifest.Record.PublicationID != evidence.ServiceRollout.Record.PublicationID ||
		evidence.Manifest.Record.OperationID != evidence.ServiceRollout.Record.OperationID ||
		work.Task.Task.Scope.EnvironmentID != evidence.EnvironmentID || !releaserender.IsReleaseAction(work.Task.Task.Action) ||
		prepared.Item.ID != work.Item.Item.ID || prepared.Item.Input == nil || *prepared.Item.Input != prepared.Input ||
		prepared.Item.Operation != workexecution.Release || len(prepared.Change.Mutations) == 0 ||
		evidence.PublishedAt.IsZero() || evidence.PublishedAt.Location() != time.UTC {
		return ReleaseWorkPublication{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Release publication evidence is invalid",
		)
	}
	if len(evidence.Manifest.Record.Members) != 1 ||
		evidence.Manifest.Record.Members[0].ServiceID != prepared.Item.TargetID ||
		evidence.Manifest.Record.Members[0].ReleaseID != evidence.ServiceRollout.Record.Render.ReleaseID {
		return ReleaseWorkPublication{}, errs.New(errs.KindValidationFailed, "Blueprint Release manifest differs from its work item")
	}
	conditions := []etcdstore.Condition{
		{
			Key:         releases.ReleaseManifestStagingKey(evidence.Manifest.Record.PublicationID),
			ModRevision: evidence.Manifest.Revision,
		},
		{Key: releases.ReleasePublicationKey(evidence.Manifest.Record.PublicationID)},
	}
	conditions = append(conditions, prepared.Change.Conditions...)
	conditions = append(conditions, evidence.ProxyAddresses.Conditions()...)
	conditions = append(conditions, evidence.StorageConditions...)
	nativeConditions, err := ledger.blueprintNativePredecessorConditions(ctx, evidence)
	if err != nil {
		return ReleaseWorkPublication{}, err
	}
	conditions = append(conditions, nativeConditions...)
	primaryConditions, err := ledger.blueprintScriptPrimaryConditions(ctx, evidence.HookWork.Hooks)
	if err != nil {
		return ReleaseWorkPublication{}, err
	}
	conditions = append(conditions, primaryConditions...)
	nativeReferences, err := blueprintNativePredecessorReferences(evidence.NativePredecessors)
	if err != nil {
		return ReleaseWorkPublication{}, err
	}
	publicationValue, err := releases.EncodeWorkPublication(releases.WorkPublication{
		PublicationID:  evidence.Manifest.Record.PublicationID,
		OperationID:    evidence.Manifest.Record.OperationID,
		ManifestDigest: evidence.Manifest.Record.Digest,
		TaskID:         work.Task.Task.ID, WorkItemID: prepared.Item.ID,
		EnvironmentID: evidence.EnvironmentID, ServiceID: prepared.Item.TargetID, Input: prepared.Input,
		NativePredecessors: nativeReferences,
		PublishedAt:        evidence.PublishedAt,
	})
	if err != nil {
		return ReleaseWorkPublication{}, err
	}
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   releases.ReleasePublicationKey(evidence.Manifest.Record.PublicationID),
			Value: publicationValue,
		},
	}
	mutations = append(mutations, cloneBlueprintReleaseMutations(prepared.Change.Mutations)...)
	mutations = append(mutations, evidence.ProxyAddresses.Mutations()...)
	for _, member := range evidence.Manifest.Record.Members {
		environmentValue, encodeErr := json.Marshal(releases.ReleaseEnvironmentIndexValue{
			Schema: 1, ServiceID: member.ServiceID, PublicationID: evidence.Manifest.Record.PublicationID,
		})
		if encodeErr != nil {
			etcdstore.ZeroMutationBytes(mutations)
			return ReleaseWorkPublication{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		serviceValue, encodeErr := json.Marshal(
			releases.ReleaseServiceIndexValue{Schema: 1, PublicationID: evidence.Manifest.Record.PublicationID},
		)
		if encodeErr != nil {
			clear(environmentValue)
			etcdstore.ZeroMutationBytes(mutations)
			return ReleaseWorkPublication{}, errs.Wrap(errs.KindInternal, encodeErr)
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
	if len(evidence.HookWork.Hooks) != 0 {
		if evidence.HookWork.OperationID != evidence.Manifest.Record.OperationID ||
			evidence.HookWork.Task.Scope.EnvironmentID != evidence.EnvironmentID ||
			evidence.SourcePrepared.IsZero() {
			etcdstore.ZeroMutationBytes(mutations)
			return ReleaseWorkPublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint hook source publication is incomplete",
			)
		}
		for _, hook := range evidence.HookWork.Hooks {
			if hook.Execution.SourceMembershipCount != evidence.SourcePrepared.MembershipCount() ||
				hook.Execution.SourceMembershipSHA256 != evidence.SourcePrepared.MembershipSHA256() {
				etcdstore.ZeroMutationBytes(mutations)
				return ReleaseWorkPublication{}, errs.New(errs.KindValidationFailed, "Blueprint hook retained source membership changed")
			}
			conditions = append(
				conditions,
				scriptsourcequeries.ScriptAttachSourceConditions(hook.Sources.AttachSources)...)
		}
		hookChange, err := evidence.HookPrepared.Publication(evidence.HookWork)
		if err != nil {
			etcdstore.ZeroMutationBytes(mutations)
			return ReleaseWorkPublication{}, err
		}
		conditions = append(conditions, hookChange.Conditions...)
		mutations = append(mutations, hookChange.Mutations...)
	}
	var sourceFragment scriptsourcepublication.ScriptSourcePublicationFragment
	if !evidence.SourcePrepared.IsZero() {
		if authority == nil || len(evidence.SourceMembers) == 0 {
			etcdstore.ZeroMutationBytes(mutations)
			return ReleaseWorkPublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script source publication is incomplete",
			)
		}
		sourceFragment, err = authority.FinalPublicationFragment(ctx, evidence.SourcePrepared)
		if err != nil {
			etcdstore.ZeroMutationBytes(mutations)
			return ReleaseWorkPublication{}, err
		}
	}
	publication, err := newReleaseWorkPublication(releaseWorkPublicationInput{
		Work: work, Input: prepared.Input,
		EnvironmentID: evidence.EnvironmentID, OperationID: evidence.Manifest.Record.OperationID,
		Conditions: conditions, Mutations: mutations, SourceFragment: sourceFragment,
		SourceAuthority: authority, SourceMembers: evidence.SourceMembers,
	})
	etcdstore.ZeroMutationBytes(mutations)
	if err != nil {
		sourceFragment.Clear()
		return ReleaseWorkPublication{}, err
	}
	return publication, nil
}
