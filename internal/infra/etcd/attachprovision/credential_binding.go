package attachprovision

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// CredentialConditions binds a connection to the exact acknowledged credential
// owner. New owners must have a native resource receipt in this same Apply.
// Runtime projection uses the same comparisons as connection publication.
func (repository *InputRepository) CredentialConditions(ctx context.Context, generation blueprintattachinputs.Generation, revision int64) ([]keyvalue.Condition, error) {
	if generation.Provision == nil || generation.CredentialOwnerID == generation.AttachID {
		return nil, nil
	}
	reference := generation.Provision.Credential
	keys := []string{attachments.AttachKey(reference.Record.ID), attachments.AttachFactsKey(reference.Record.ID)}
	if reference.Revision == 0 {
		keys = append(keys, blueprintunits.AppliedKey(generation.EnvironmentID, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: reference.Record.ID}))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil {
		return nil, checkpointConflict("Attach credential owner has not settled")
	}
	guards := make([]keyvalue.Condition, 0, len(keys)+2)
	for index, key := range keys {
		value := read.Values[index]
		if value != nil && (value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision) {
			return nil, checkpointConflict("Attach credential source has an invalid revision")
		}
		guards = append(guards, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)})
	}
	current, err := attachments.DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	expected := attachments.CloneAttachRecord(reference.Record)
	expected.Status = core.AttachReady
	if !attachments.SameBlueprintAttachCandidateRecord(expected, current) ||
		reference.Revision > 0 && read.Values[0].ModRevision != reference.Revision {
		return nil, checkpointConflict("Attach credential owner changed after capture")
	}
	guards = append(guards, keyvalue.Condition{Key: deletions.TombstoneKey("attach", current.ID)})
	var applied blueprintunits.AppliedRecord
	if reference.Revision == 0 {
		if read.Values[2] == nil {
			return nil, checkpointConflict("Attach credential owner has no applied receipt")
		}
		applied, err = blueprintunits.DecodeApplied(read.Values[2].Value)
		if err != nil {
			return nil, err
		}
		if applied.State != blueprintunits.Applied || applied.ParentTaskID != generation.ParentTaskID ||
			applied.EnvironmentID != generation.EnvironmentID || applied.Target.ID != current.ID || applied.Target.Kind != ids.KindAttach ||
			applied.Execution == nil && applied.ControllerAttachInputRevision == 0 {
			return nil, checkpointConflict("Attach credential receipt belongs to another execution")
		}
	}
	if len(generation.FactSets) == 0 && !current.HookBundle {
		if read.Values[1] != nil {
			return nil, checkpointConflict("Factless Attach credential owner acquired unexpected facts")
		}
		return guards, nil
	}
	if read.Values[1] == nil || reference.Revision > 0 && read.Values[1].ModRevision != generation.Provision.FactsRevision {
		return nil, checkpointConflict("Attach credential facts changed or are unavailable")
	}
	facts, err := attachments.DecodeAttachEncryptedFacts(read.Values[1].Value)
	if err != nil {
		return nil, err
	}
	defer clear(facts.Ciphertext)
	if generation.Provision.HookOwnerOperationID == "" {
		if !sameEncryptedFacts(facts, generation.GeneratedInputs) {
			return nil, checkpointConflict("Attach credential facts differ from captured values")
		}
		return guards, nil
	}
	if applied.Execution == nil || applied.Execution.TaskID != generation.ParentTaskID || applied.Execution.Validate() != nil {
		return nil, checkpointConflict("Attach hook facts have no acknowledged native execution")
	}
	key := blueprintattachoutputs.Key(current.ID, applied.Execution.AttemptID)
	outputRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if outputRead != nil {
		defer keyvalue.ClearValues(outputRead.Values)
	}
	if err != nil {
		return nil, err
	}
	if outputRead == nil || outputRead.ReadRevision != revision || len(outputRead.Values) != 1 || outputRead.Values[0] == nil ||
		outputRead.Values[0].Key != key || outputRead.Values[0].ModRevision <= 0 || outputRead.Values[0].ModRevision > revision {
		return nil, checkpointConflict("Attach hook output is unavailable")
	}
	output, err := blueprintattachoutputs.Decode(outputRead.Values[0].Value)
	if err != nil {
		return nil, err
	}
	defer blueprintattachoutputs.Clear(&output)
	if output.TaskID != applied.Execution.TaskID || output.WorkItemID != applied.Execution.WorkItemID || output.AttemptID != applied.Execution.AttemptID ||
		output.Executor != applied.Execution.Executor || output.Input != applied.Execution.Input || output.EnvironmentID != generation.EnvironmentID ||
		output.AttachID != current.ID || output.OperationID != generation.Provision.HookOwnerOperationID || !sameEncryptedFacts(facts, output.Facts) {
		return nil, checkpointConflict("Attach hook output differs from its applied credential owner")
	}
	return append(guards, keyvalue.Condition{Key: key, ModRevision: outputRead.Values[0].ModRevision}), nil
}

func sameEncryptedFacts(left, right attachments.EncryptedFacts) bool {
	return left.AttachID == right.AttachID && left.EnvelopeVersion == right.EnvelopeVersion && left.Cipher == right.Cipher &&
		left.DigestAlgorithm == right.DigestAlgorithm && left.CiphertextSHA256 == right.CiphertextSHA256 && bytes.Equal(left.Ciphertext, right.Ciphertext)
}
