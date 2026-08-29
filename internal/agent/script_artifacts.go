package agent

import (
	"bytes"
	"crypto/sha256"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateAndCopyScriptArtifacts(
	plan *agentpb.ExecutionPlan,
	artifacts *agentpb.ScriptAssignmentArtifacts,
) (*agentpb.ScriptAssignmentArtifacts, error) {
	isScript := plan != nil && plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_SCRIPT
	if !isScript {
		if artifacts != nil && (len(artifacts.Bodies) != 0 || len(artifacts.Secrets) != 0 || len(artifacts.Entries) != 0) {
			return nil, errs.New(errs.KindInternal, "agent: non-Script assignment contains Script artifacts")
		}
		return nil, nil
	}
	if artifacts == nil || len(artifacts.Bodies) != 1 || len(artifacts.Secrets) != 0 ||
		len(plan.ScriptBodyArtifacts) != 1 || len(plan.ScriptRunnerSnapshots) != 1 ||
		len(artifacts.Entries) != len(plan.ScriptRunnerSnapshots[0].EntryBindings) {
		return nil, errs.New(errs.KindInternal, "agent: Script assignment artifacts are incomplete")
	}
	if err := executionplan.RejectUnknown(artifacts); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	body := artifacts.Bodies[0]
	metadata := plan.ScriptBodyArtifacts[0]
	if body == nil || body.Metadata == nil || !proto.Equal(body.Metadata, metadata) ||
		len(body.Body) != int(metadata.Size) || len(body.Body) == 0 ||
		len(body.Body) > executionplan.MaximumScriptBodyBytes || !utf8.Valid(body.Body) ||
		bytes.IndexByte(body.Body, 0) >= 0 {
		return nil, errs.New(errs.KindInternal, "agent: Script body artifact is invalid")
	}
	digest := sha256.Sum256(body.Body)
	if !bytes.Equal(digest[:], metadata.Sha256) {
		return nil, errs.New(errs.KindInternal, "agent: Script body artifact digest does not match")
	}
	owned := &agentpb.ScriptAssignmentArtifacts{Bodies: []*agentpb.ScriptBodyArtifact{{
		Metadata: proto.Clone(metadata).(*agentpb.ScriptBodyArtifactMetadata),
		Body:     append([]byte(nil), body.Body...),
	}}}
	for index, entry := range artifacts.Entries {
		binding := plan.ScriptRunnerSnapshots[0].EntryBindings[index]
		if entry == nil || entry.Binding == nil || !proto.Equal(entry.Binding, binding) ||
			len(entry.Value) > 256<<10 {
			clearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script Entry artifact is invalid")
		}
		digest := sha256.Sum256(entry.Value)
		if !bytes.Equal(digest[:], binding.Sha256) ||
			(binding.Kind == agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV &&
				(!utf8.Valid(entry.Value) || bytes.IndexByte(entry.Value, 0) >= 0)) {
			clearScriptArtifacts(owned)
			return nil, errs.New(errs.KindInternal, "agent: Script Entry artifact digest does not match")
		}
		owned.Entries = append(owned.Entries, &agentpb.ScriptEntryArtifact{
			Binding: proto.Clone(binding).(*agentpb.ScriptRunnerEntryBinding),
			Value:   append([]byte(nil), entry.Value...),
		})
	}
	return owned, nil
}

func clearScriptArtifacts(artifacts *agentpb.ScriptAssignmentArtifacts) {
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
