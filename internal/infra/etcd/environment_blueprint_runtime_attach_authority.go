package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A runtime projection cannot adopt a different Attach after its unit settled.
// Capture the applied receipt and, for Controller adoption, its exact retained
// binding at the same MVCC revision as the topology being rendered.
func (repository *EnvironmentBlueprintRepository) blueprintRuntimeAttachConditions(
	ctx context.Context, environmentID string, attach etcdstore.Versioned[attachrecord.Record], revision int64,
) ([]etcdstore.Condition, error) {
	key := blueprintunits.AppliedKey(
		environmentID,
		blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attach.Record.ID},
	)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach receipt is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	receipt, err := blueprintunits.DecodeApplied(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if receipt.EnvironmentID != environmentID || receipt.Target.Kind != ids.KindAttach ||
		receipt.Target.ID != attach.Record.ID || receipt.State != blueprintunits.Applied {
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach receipt changed")
	}
	conditions := []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}
	if receipt.ControllerAttachInputRevision == 0 {
		if receipt.Execution == nil || receipt.Execution.Validate() != nil ||
			receipt.Execution.TaskID != attach.Record.TaskID || receipt.ParentTaskID != attach.Record.TaskID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach execution changed")
		}
		return conditions, nil
	}
	digest, err := attachrecord.BindingDigest(attach.Record)
	if err != nil || digest != receipt.ControllerAttachBindingSHA256 {
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach differs from its acknowledged binding")
	}
	if len(receipt.ControllerAttachReferences) == 0 || receipt.ControllerAttachReferences[0].AttachID != attach.Record.CredentialAttachID {
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach has no captured credential reference")
	}
	for _, expected := range receipt.ControllerAttachReferences {
		current, guards, err := attachrecord.ReadBindingReference(ctx, repository.store, expected.AttachID, revision)
		if err != nil {
			return nil, err
		}
		if current != expected {
			return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach credential or grant changed")
		}
		conditions = append(conditions, guards...)
	}
	return conditions, nil
}
