package taskassignment

import (
	"bytes"
	"crypto/sha256"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidateAndCopyScriptArtifacts(
	plan *agentpb.ExecutionPlan,
	artifacts *agentpb.ScriptAssignmentArtifacts,
) (*agentpb.ScriptAssignmentArtifacts, error) {
	hasScripts := plan != nil && len(plan.ScriptBodyArtifacts) != 0
	if !hasScripts {
		if artifacts != nil &&
			(len(artifacts.Bodies) != 0 || len(artifacts.Secrets) != 0 || len(artifacts.Entries) != 0) {
			return nil, errs.New(errs.KindInternal, "agent: non-Script assignment contains Script artifacts")
		}
		return nil, nil
	}
	if artifacts == nil || len(artifacts.Bodies) != len(plan.ScriptBodyArtifacts) || len(artifacts.Secrets) != 0 ||
		len(plan.ScriptRunnerSnapshots) != len(plan.ScriptBodyArtifacts) {
		return nil, errs.New(errs.KindInternal, "agent: Script assignment artifacts are incomplete")
	}
	if err := executionplan.RejectUnknown(artifacts); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	metadataByExecution := make(map[string]*agentpb.ScriptBodyArtifactMetadata, len(plan.ScriptBodyArtifacts))
	for _, metadata := range plan.ScriptBodyArtifacts {
		if metadata == nil || metadata.ScriptExecutionId == "" ||
			metadataByExecution[metadata.ScriptExecutionId] != nil {
			return nil, errs.New(errs.KindInternal, "agent: Script body artifact metadata is invalid")
		}
		metadataByExecution[metadata.ScriptExecutionId] = metadata
	}
	owned := &agentpb.ScriptAssignmentArtifacts{}
	seenBodies := make(map[string]struct{}, len(artifacts.Bodies))
	for _, body := range artifacts.Bodies {
		if body == nil || body.Metadata == nil {
			ClearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script body artifact is invalid")
		}
		metadata := metadataByExecution[body.Metadata.ScriptExecutionId]
		if _, duplicate := seenBodies[body.Metadata.ScriptExecutionId]; duplicate ||
			metadata == nil || !proto.Equal(body.Metadata, metadata) || len(body.Body) != int(metadata.Size) ||
			len(body.Body) == 0 || len(body.Body) > executionplan.MaximumScriptBodyBytes ||
			!utf8.Valid(body.Body) || bytes.IndexByte(body.Body, 0) >= 0 {
			ClearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script body artifact is invalid")
		}
		seenBodies[body.Metadata.ScriptExecutionId] = struct{}{}
		digest := sha256.Sum256(body.Body)
		if !bytes.Equal(digest[:], metadata.Sha256) {
			ClearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script body artifact digest does not match")
		}
		owned.Bodies = append(owned.Bodies, &agentpb.ScriptBodyArtifact{
			Metadata: proto.Clone(metadata).(*agentpb.ScriptBodyArtifactMetadata),
			Body:     append([]byte(nil), body.Body...),
		})
	}
	bindingByIdentity := make(map[string]*agentpb.ScriptRunnerEntryBinding)
	for _, snapshot := range plan.ScriptRunnerSnapshots {
		if snapshot == nil {
			ClearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script runner snapshot is invalid")
		}
		for _, binding := range snapshot.EntryBindings {
			if binding == nil {
				ClearScriptArtifacts(owned)
				return nil, errs.New(errs.KindInternal, "agent: Script Entry binding is invalid")
			}
			identity := binding.EntryId + "\x00" + binding.ValueGenerationId
			if previous, exists := bindingByIdentity[identity]; exists && !proto.Equal(previous, binding) {
				ClearScriptArtifacts(owned)
				return nil, errs.New(errs.KindInternal, "agent: Script Entry bindings disagree")
			}
			bindingByIdentity[identity] = binding
		}
	}
	if len(artifacts.Entries) != len(bindingByIdentity) {
		ClearScriptArtifacts(owned)
		return nil, errs.New(errs.KindInternal, "agent: Script Entry artifacts are incomplete")
	}
	seenEntries := make(map[string]struct{}, len(artifacts.Entries))
	for _, entry := range artifacts.Entries {
		if entry == nil || entry.Binding == nil {
			ClearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script Entry artifact is invalid")
		}
		identity := entry.Binding.EntryId + "\x00" + entry.Binding.ValueGenerationId
		binding := bindingByIdentity[identity]
		if _, duplicate := seenEntries[identity]; duplicate || !proto.Equal(entry.Binding, binding) ||
			len(entry.Value) > 256<<10 {
			ClearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script Entry artifact is invalid")
		}
		seenEntries[identity] = struct{}{}
		digest := sha256.Sum256(entry.Value)
		if !bytes.Equal(digest[:], binding.Sha256) ||
			(binding.Kind == agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV &&
				(!utf8.Valid(entry.Value) || bytes.IndexByte(entry.Value, 0) >= 0)) {
			ClearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script Entry artifact digest does not match")
		}
		owned.Entries = append(owned.Entries, &agentpb.ScriptEntryArtifact{
			Binding: proto.Clone(binding).(*agentpb.ScriptRunnerEntryBinding),
			Value:   append([]byte(nil), entry.Value...),
		})
	}
	return owned, nil
}

func ClearScriptArtifacts(artifacts *agentpb.ScriptAssignmentArtifacts) {
	if artifacts == nil {
		return
	}
	for _, body := range artifacts.Bodies {
		if body != nil {
			clear(body.Body)
			body.Body = nil
		}
	}
	for _, secret := range artifacts.Secrets {
		if secret != nil {
			clear(secret.Value)
			secret.Value = nil
		}
	}
	for _, entry := range artifacts.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}
