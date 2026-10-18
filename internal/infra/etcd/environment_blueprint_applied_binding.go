package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadAppliedAttachBinding reads resource-owned facts, not execution history.
// Its conditions must join the transaction consuming those facts.
func (repository *EnvironmentBlueprintRepository) ReadAppliedAttachBinding(ctx context.Context, environmentID, attachID string, revision int64) (attachments.AppliedBinding, []keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return attachments.AppliedBinding{}, nil, err
	}
	if repository == nil || repository.HierarchyRepository == nil || revision <= 0 || ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindAttach, attachID) != nil {
		return attachments.AppliedBinding{}, nil, errs.New(errs.KindValidationFailed, "applied Attach binding requires an Environment and fixed revision")
	}
	keys := []string{attachments.AppliedBindingKey(attachID), blueprintunits.AppliedKey(environmentID, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attachID}), attachments.AttachKey(attachID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return attachments.AppliedBinding{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "applied Attach binding is unavailable")
	}
	conditions := make([]keyvalue.Condition, 0, len(keys))
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision {
			return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "applied Attach binding revision changed")
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision})
	}
	binding, err := attachments.DecodeAppliedBinding(read.Values[0].Value)
	if err != nil {
		return attachments.AppliedBinding{}, nil, err
	}
	accepted := false
	defer func() {
		if !accepted {
			binding.Clear()
		}
	}()
	receipt, err := blueprintunits.DecodeApplied(read.Values[1].Value)
	if err != nil {
		return attachments.AppliedBinding{}, nil, err
	}
	current, err := attachments.DecodeAttachRecord(read.Values[2].Value)
	if err != nil {
		return attachments.AppliedBinding{}, nil, err
	}
	digest, err := attachments.BindingDigest(binding.Consumer)
	currentDigest, currentErr := attachments.BindingDigest(current)
	if err != nil || currentErr != nil || digest != currentDigest || binding.Consumer.ID != attachID || binding.Consumer.EnvironmentID != environmentID || receipt.EnvironmentID != environmentID || receipt.Target != (blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: attachID}) || receipt.State != blueprintunits.Applied || receipt.ParentTaskID != binding.TaskID {
		return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "Attach facts do not match the applied connection")
	}
	if receipt.Execution != nil {
		if receipt.Execution.Validate() != nil || receipt.Execution.TaskID != binding.TaskID || receipt.Execution.WorkItemID != binding.WorkItemID || receipt.Execution.Input != binding.Input {
			return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "Attach facts do not match the accepted execution")
		}
	} else if receipt.ControllerAttachInputRevision <= 0 || receipt.ControllerOperationID != binding.OperationID || receipt.ControllerAttachBindingSHA256 != digest {
		return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "Attach facts do not match the accepted binding decision")
	}
	records := append([]attachments.Record{binding.Credential}, binding.Grants...)
	if receipt.Execution == nil && len(receipt.ControllerAttachReferences) != len(records) {
		return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "Applied Attach credential references are incomplete")
	}
	for index, record := range records {
		reference, guards, err := attachments.ReadBindingReference(ctx, repository.store, record.ID, revision)
		if err != nil {
			return attachments.AppliedBinding{}, nil, err
		}
		expectedDigest, err := attachments.BindingDigest(record)
		if err != nil || reference.BindingSHA256 != expectedDigest {
			return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "Applied Attach credential or grant changed")
		}
		if index == 0 {
			expectedFacts := ""
			if binding.Facts != nil {
				expectedFacts = binding.Facts.CiphertextSHA256
			}
			if reference.FactsSHA256 != expectedFacts {
				return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "Applied Attach credential facts changed")
			}
		}
		if receipt.Execution == nil && reference != receipt.ControllerAttachReferences[index] {
			return attachments.AppliedBinding{}, nil, errs.New(errs.KindStateConflict, "Applied Attach differs from its accepted credential reference")
		}
		conditions = append(conditions, guards...)
	}
	accepted = true
	return binding, conditions, nil
}
