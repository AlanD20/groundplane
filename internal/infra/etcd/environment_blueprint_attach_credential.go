package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	attachoutputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Reuse needs an acknowledged owner, not merely a declared dependency. The
// exact owner, receipt and encrypted facts are fenced together at connection
// publication and again when the Environment runtime projection is sealed.
func (repository *EnvironmentBlueprintRepository) blueprintAttachCredentialConditions(
	ctx context.Context, generation attachinputs.Generation, revision int64,
) ([]etcdstore.Condition, error) {
	if generation.Provision == nil || generation.CredentialOwnerID == generation.AttachID {
		return nil, nil
	}
	reference := generation.Provision.Credential
	keys := []string{attachrecord.AttachKey(reference.Record.ID),
		blueprintunits.AppliedKey(
			generation.EnvironmentID,
			blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: reference.Record.ID},
		),
		attachrecord.AttachFactsKey(reference.Record.ID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil ||
		read.Values[1] == nil {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "Attach credential owner has not settled")
	}
	defer etcdstore.ClearValues(read.Values)
	current, err := attachrecord.DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	applied, err := blueprintunits.DecodeApplied(read.Values[1].Value)
	if err != nil {
		return nil, err
	}
	expected := attachrecord.CloneAttachRecord(reference.Record)
	expected.TaskID, expected.CreatedAt, expected.Status = current.TaskID, current.CreatedAt, core.AttachReady
	if current.Status != core.AttachReady || !attachrecord.SameBlueprintAttachCandidateRecord(expected, current) ||
		applied.State != blueprintunits.Applied || applied.ParentTaskID != generation.ParentTaskID ||
		(applied.SourceTaskID != current.TaskID && applied.ControllerAttachInputRevision == 0) {
		return nil, errs.New(errs.KindStateConflict, "Attach credential owner changed before connection")
	}
	conditions := make([]etcdstore.Condition, 0, len(keys)+1)
	for index, key := range keys {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: key, ModRevision: etcdstore.RevisionOf(read.Values[index])},
		)
	}
	if len(generation.FactSets) == 0 && !current.HookBundle {
		if read.Values[2] != nil {
			return nil, errs.New(errs.KindStateConflict, "Factless Attach credential owner acquired unexpected facts")
		}
		return conditions, nil
	}
	if read.Values[2] == nil {
		return nil, errs.New(errs.KindStateConflict, "Attach credential facts are missing")
	}
	facts, err := attachrecord.DecodeAttachEncryptedFacts(read.Values[2].Value)
	if err != nil {
		return nil, err
	}
	defer clear(facts.Ciphertext)
	if generation.Provision.HookOwnerOperationID == "" {
		if !sameBlueprintAttachEncryptedFacts(facts, generation.GeneratedInputs) {
			return nil, errs.New(errs.KindStateConflict, "Attach credential facts changed after admission")
		}
		return conditions, nil
	}
	output, found, err := repository.GetBlueprintAttachOutputGeneration(ctx, current.ID, applied.SourceTaskID, revision)
	if err != nil {
		return nil, err
	}
	defer attachoutputs.Clear(&output.Record)
	if !found || output.Record.ParentTaskID != generation.ParentTaskID ||
		output.Record.OperationID != generation.Provision.HookOwnerOperationID || output.Record.PlanID != applied.SourcePlanID ||
		output.Record.AssignmentID != applied.SourceAssignment || output.Record.ExecutionEpoch != applied.ExecutionEpoch ||
		!sameBlueprintAttachEncryptedFacts(facts, output.Record.Facts) {
		return nil, errs.New(errs.KindStateConflict, "Attach credential hook output changed")
	}
	conditions = append(conditions, etcdstore.Condition{
		Key: attachoutputs.Key(current.ID, applied.SourceTaskID), ModRevision: output.Revision,
	})
	return conditions, nil
}
