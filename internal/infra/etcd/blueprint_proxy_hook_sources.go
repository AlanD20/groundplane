package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintProxyHookSourceMembers reserves only existing immutable values for
// future hooks. Candidate Service/projection records are not promoted, and no
// Script execution is published before the owning Service Task is sealed.
func (ledger *ReleaseLedger) BlueprintProxyHookSourceMembers(ctx context.Context,
	parent workexecution.Task, reference componentplanning.ProxyServiceRolloutReference,
) ([]scriptsourceevidence.ScriptSourcePreparationMember, error) {
	operationID, err := componentplanning.ProxySourceOperationID(parent)
	if err != nil {
		return nil, err
	}
	preparation, capture, err := ledger.ReadBlueprintProxyServiceRollout(
		ctx,
		parent.Scope.EnvironmentID,
		parent.ID,
		reference,
	)
	if err != nil {
		return nil, err
	}
	render, err := releaserender.OpenServiceRolloutCapture(capture.Record)
	if err != nil {
		return nil, err
	}
	var members []scriptsourceevidence.ScriptSourcePreparationMember
	for _, hook := range render.Hooks {
		snapshot := new(agentpb.ResolvedRunnerSnapshot)
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(hook.RunnerSnapshot, snapshot); err != nil ||
			snapshot.GetScriptExecutionId() != hook.ScriptExecutionID ||
			snapshot.GetSnapshotId() != hook.RunnerSnapshotID ||
			snapshot.GetServiceId() != render.ServiceID ||
			snapshot.GetReleaseId() != render.ReleaseID {
			return nil, errs.New(errs.KindStateConflict, "Captured rollout hook snapshot does not match its Release")
		}
		sources, found := preparation.Hooks[snapshot.GetScriptExecutionId()]
		if !found || sources.Script.Record.ScriptSetGeneration != parent.ID ||
			scriptsourcequeries.ValidateScriptContextSources(sources, snapshot) != nil {
			return nil, errs.New(errs.KindStateConflict, "pending proxy hook source authority is incomplete")
		}
		base := sourceref.Reference{OperationID: operationID,
			ScriptExecutionID: snapshot.GetScriptExecutionId(), SourceOwnerID: parent.Scope.EnvironmentID}
		body := base
		body.Source = sourceref.SourceIdentity{Kind: sourceref.SourceBody,
			EnvironmentID: parent.Scope.EnvironmentID, ScriptSetGeneration: parent.ID,
			ScriptID: sources.Script.Record.Desired.ID, BodyGeneration: sources.BodyGeneration.Record.Generation}
		body.SourceModRevision = sources.BodyGeneration.Revision
		body.SourceDigest = sources.BodyGeneration.Record.BodySHA256
		members = append(members, existingProxyHookSource(body, scriptrecord.ScriptSetBodyGenerationKey(
			parent.Scope.EnvironmentID, parent.ID, body.Source.ScriptID, body.Source.BodyGeneration)))
		for _, binding := range snapshot.GetEntryBindings() {
			key := entryvalues.PlainKey(binding.GetEntryId(), binding.GetValueGenerationId())
			if binding.GetSecret() {
				key = entryvalues.SecretKey(binding.GetEntryId(), binding.GetValueGenerationId())
			}
			value, err := scriptexecutions.ScriptExecutionValueAt(
				ctx,
				ledger.store,
				key,
				preparation.Manifest.ReadRevision,
			)
			if err != nil {
				return nil, err
			}
			entry := base
			entry.Source = sourceref.SourceIdentity{Kind: sourceref.SourceEntryValue,
				EntryID: binding.GetEntryId(), ValueGenerationID: binding.GetValueGenerationId()}
			entry.SourceModRevision = value.ModRevision
			if binding.GetSecret() {
				generation, err := entryvalues.DecodeSecret(value.Value)
				clear(value.Value)
				if err != nil {
					return nil, err
				}
				entry.SourceDigest = generation.CiphertextSHA256
				clear(generation.Ciphertext)
			} else {
				generation, err := entryvalues.DecodePlain(value.Value)
				clear(value.Value)
				if err != nil {
					return nil, err
				}
				entry.SourceDigest = generation.PlaintextSHA256
				clear(generation.Content)
			}
			members = append(members, existingProxyHookSource(entry, key))
		}
		for _, secret := range snapshot.GetSecretValues() {
			key := secretrecord.ValueKey(secret.GetValueGenerationId())
			value, err := scriptexecutions.ScriptExecutionValueAt(
				ctx,
				ledger.store,
				key,
				preparation.Manifest.ReadRevision,
			)
			if err != nil {
				return nil, err
			}
			clear(value.Value)
			member := base
			member.Source = sourceref.SourceIdentity{Kind: sourceref.SourceSecretValue,
				SecretID: secret.GetValueGenerationId(), ValueGenerationID: secret.GetValueGenerationId()}
			member.SourceOwnerID = secret.GetOwnerId()
			member.SourceModRevision = value.ModRevision
			member.SourceDigest = hex.EncodeToString(secret.GetDigest())
			members = append(members, existingProxyHookSource(member, key))
		}
	}
	return members, nil
}

func existingProxyHookSource(
	reference sourceref.Reference,
	key string,
) scriptsourceevidence.ScriptSourcePreparationMember {
	return scriptsourceevidence.ScriptSourcePreparationMember{Reference: reference,
		Evidence: scriptsourceevidence.ScriptSourceEvidence{
			Existing: &scriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: key}}}
}
