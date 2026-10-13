package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
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
		if receipt.SourceTaskID != attach.Record.TaskID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach execution changed")
		}
		return conditions, nil
	}
	inputKey := attachinputs.Key(receipt.ParentTaskID, attach.Record.ID)
	input, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{inputKey}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if input == nil || input.ReadRevision != revision || len(input.Values) != 1 || input.Values[0] == nil ||
		input.Values[0].ModRevision != receipt.ControllerAttachInputRevision {
		if input != nil {
			etcdstore.ClearValues(input.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach binding is unavailable")
	}
	defer etcdstore.ClearValues(input.Values)
	generation, err := attachinputs.Decode(input.Values[0].Value)
	if err != nil {
		return nil, err
	}
	defer attachinputs.Clear(&generation)
	if generation.ParentTaskID != receipt.ParentTaskID || generation.EnvironmentID != environmentID ||
		generation.AttachID != attach.Record.ID || !generation.MatchesControllerBinding(attach.Record, attach.Revision) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime Attach binding changed")
	}
	conditions = append(
		conditions,
		etcdstore.Condition{Key: inputKey, ModRevision: receipt.ControllerAttachInputRevision},
	)
	if generation.Binding != nil {
		return append(conditions, generation.Binding.Conditions()...), nil
	}
	if generation.Provision == nil {
		return conditions, nil
	}
	credentialConditions, err := repository.blueprintAttachCredentialConditions(ctx, generation, revision)
	if err != nil {
		return nil, err
	}
	conditions = append(conditions, credentialConditions...)
	return append(conditions, generation.Provision.Conditions()...), nil
}
