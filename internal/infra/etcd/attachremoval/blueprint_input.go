package attachremoval

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintInput freezes operator-owned cleanup inputs before Apply admission.
// Consumer runtime may advance only through this Apply's captured predecessors;
// preparation must prove that provenance before deriving the executable input.
// A nil Snapshot represents a declared binding that never existed, not a
// substitute backing record. Its birth and actual absence remain fenced.
type BlueprintInput struct {
	TaskID                string                                        `json:"task_id"`
	WorkItemID            string                                        `json:"work_item_id"`
	OperationID           string                                        `json:"operation_id"`
	EnvironmentID         string                                        `json:"environment_id"`
	AttachName            string                                        `json:"attach_name"`
	AttachID              string                                        `json:"attach_id"`
	ConsumerServiceID     string                                        `json:"consumer_service_id"`
	DeclarationRevisionID string                                        `json:"declaration_revision_id"`
	BirthRevisionID       string                                        `json:"birth_revision_id"`
	RemovedBefore         []string                                      `json:"removed_before,omitempty"`
	Snapshot              *DetachSnapshot                               `json:"snapshot,omitempty"`
	Hook                  *taskconfiguration.BackingHookEncryptedInputs `json:"hook,omitempty"`
	Sources               []tasksecretpinrecord.Record                  `json:"sources,omitempty"`
	Pins                  *tasksecretpins.Binding                       `json:"pins,omitempty"`
	Conditions            []keyvalue.Condition                          `json:"conditions"`
}

func BlueprintInputKey(environmentID, taskID, workID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(environmentID, taskID) + "detach-inputs/" + workID
}

func (input BlueprintInput) Validate(ctx context.Context, published bool) error {
	if ctx == nil || ids.Validate(ids.KindTask, input.TaskID) != nil ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(
			ids.KindWorkItem,
			input.WorkItemID,
		) != nil || ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(
			ids.KindAttach,
			input.AttachID,
		) != nil || ids.Validate(ids.KindService, input.ConsumerServiceID) != nil ||
		ids.Validate(
			ids.KindTask,
			input.DeclarationRevisionID,
		) != nil || ids.Validate(ids.KindTask, input.BirthRevisionID) != nil ||
		input.AttachName == "" || len(input.Conditions) == 0 || len(input.Conditions) > 2048 {
		return errs.New(errs.KindValidationFailed, "Blueprint Detach requires a retained declaration and frozen source")
	}
	for _, condition := range input.Conditions {
		if condition.Key == "" || condition.ModRevision < 0 {
			return recordcodec.CorruptRecord()
		}
	}

	if len(input.RemovedBefore) > 512 || !slices.IsSorted(input.RemovedBefore) {
		return recordcodec.CorruptRecord()
	}
	for index, id := range input.RemovedBefore {
		if ids.Validate(ids.KindAttach, id) != nil || id == input.AttachID ||
			index > 0 && id == input.RemovedBefore[index-1] {
			return recordcodec.CorruptRecord()
		}
	}
	if input.Snapshot == nil {
		if input.Hook != nil || len(input.Sources) != 0 || input.Pins != nil {
			return recordcodec.CorruptRecord()
		}
		return nil
	}
	if err := input.Snapshot.Validate(); err != nil {
		return err
	}
	source := input.Snapshot.Source
	if source.Attach.ID != input.AttachID || source.Attach.EnvironmentID != input.EnvironmentID ||
		source.Attach.ServiceID != input.ConsumerServiceID || source.HookInputSet != nil ||
		source.HookInputs != nil || source.HookOperationID != "" {
		return recordcodec.CorruptRecord()
	}
	requiresHook := false
	if NeedsAgent(source) && source.Backing.Desired.Adapter == "custom" && source.Backing.Desired.Hooks != nil {
		for _, value := range source.Backing.Desired.Hooks.Inputs {
			requiresHook = requiresHook || value.Generate == ""
		}
	}
	if requiresHook != (input.Hook != nil) {
		return recordcodec.CorruptRecord()
	}
	if input.Hook != nil {
		if source.Backing.Desired.Adapter != "custom" || source.Backing.Desired.Hooks == nil ||
			source.Backing.Desired.Hooks.Detach == nil || taskconfiguration.ValidateBackingHookEncryptedInputs(*input.Hook) != nil {
			return recordcodec.CorruptRecord()
		}
	} else if len(input.Sources) != 0 || input.Pins != nil {
		return recordcodec.CorruptRecord()
	}
	return validateHookCapture(input.OperationID, input.TaskID, input.WorkItemID,
		input.Hook, input.Sources, input.Pins, published)
}

func EncodeBlueprintInput(ctx context.Context, input BlueprintInput) ([]byte, error) {
	if err := input.Validate(ctx, true); err != nil {
		return nil, err
	}
	raw, err := recordcodec.Encode("blueprint-detach-input", input)
	if err != nil {
		return nil, err
	}
	if len(raw) > MaximumInputBytes {
		clear(raw)
		return nil, errs.New(errs.KindValidationFailed, "Blueprint Detach capture exceeds the input size limit")
	}
	return raw, nil
}

func DecodeBlueprintInput(ctx context.Context, raw []byte) (BlueprintInput, error) {
	if len(raw) > MaximumInputBytes {
		return BlueprintInput{}, recordcodec.CorruptRecord()
	}
	input, err := recordcodec.Decode[BlueprintInput](raw, "blueprint-detach-input")
	if err != nil || input.Validate(ctx, true) != nil {
		input.Clear()
		return BlueprintInput{}, recordcodec.CorruptRecord()
	}
	return input, nil
}

func (input *BlueprintInput) Clear() {
	if input == nil {
		return
	}
	if input.Snapshot != nil {
		input.Snapshot.Clear()
	}
	if input.Hook != nil {
		clear(input.Hook.Ciphertext)
	}
}

// HasHostEffects selects placement from admission authority, including peer
// removals that precede this binding. No later mutable backing choice can
// turn a Controller decision into host execution.
func (input BlueprintInput) HasHostEffects() bool {
	if input.Snapshot == nil {
		return false
	}
	source := input.Snapshot.Source
	return NeedsAgent(source) || input.Snapshot.Runtime != nil && source.Attach.Status != "detached" &&
		!input.Snapshot.KeepsConsumerNetworkAfter(input.RemovedBefore)
}
