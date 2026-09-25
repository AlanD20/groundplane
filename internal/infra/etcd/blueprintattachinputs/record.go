// Package blueprintattachinputs owns the immutable private input generation
// prepared for one Blueprint-authored Custom Attach.
package blueprintattachinputs

import (
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const recordPrefix = "/v1/private/blueprint-attach-input-generations/"

const TaskAttachIDParam = "blueprint_attach_input_generation_attach_id"

type OwnerKind string

const (
	OwnerParent OwnerKind = "parent"
	OwnerChild  OwnerKind = "child"
)

// Transfer is immutable evidence that publication moved one generation from
// its visible Blueprint parent to the exact hidden child which executes it.
type Transfer struct {
	ParentTaskID string `json:"parent_task_id"`
	ChildTaskID  string `json:"child_task_id"`
}

// HookPlan is the non-secret executable authority needed to run the Attach
// hook. Resolved values live only in the encrypted envelopes on Generation.
type HookPlan struct {
	Attach *backinghook.Definition      `json:"attach"`
	Detach *backinghook.Definition      `json:"detach,omitempty"`
	Facts  []backinghook.FactDefinition `json:"facts,omitempty"`
	Inputs []HookInput                  `json:"inputs,omitempty"`
}

type HookInputSource string

const (
	HookInputResolved  HookInputSource = "resolved"
	HookInputGenerated HookInputSource = "generated"
)

type HookInput struct {
	Key    string          `json:"key"`
	Source HookInputSource `json:"source"`
}

// Generation is immutable authority first owned by its parent and then by the
// exact published child. Transfer never resolves mutable backing records.
type Generation struct {
	ID                   string                                        `json:"id"`
	EnvironmentID        string                                        `json:"environment_id"`
	RevisionID           string                                        `json:"revision_id"`
	ParentTaskID         string                                        `json:"parent_task_id"`
	OperationID          string                                        `json:"operation_id"`
	AttachID             string                                        `json:"attach_id"`
	AttachName           string                                        `json:"attach_name"`
	AuthoredSpecSHA256   string                                        `json:"authored_spec_sha256"`
	ConsumerServiceID    string                                        `json:"consumer_service_id"`
	CredentialOwnerID    string                                        `json:"credential_owner_id"`
	BackingProjectID     string                                        `json:"backing_project_id"`
	BackingEnvironmentID string                                        `json:"backing_environment_id"`
	BackingServiceID     string                                        `json:"backing_service_id"`
	BackingNetworkID     string                                        `json:"backing_network_id"`
	AdapterKey           string                                        `json:"adapter_key"`
	Authentication       core.BackingAuthentication                    `json:"authentication"`
	Hook                 HookPlan                                      `json:"hook"`
	FactSets             []attachrecord.FactSetMetadata                `json:"fact_sets,omitempty"`
	GeneratedInputs      attachrecord.EncryptedFacts                   `json:"generated_inputs"`
	ResolvedInputs       *taskconfiguration.BackingHookEncryptedInputs `json:"resolved_inputs,omitempty"`
	SecretSources        []tasksecretpinrecord.Record                  `json:"secret_sources,omitempty"`
	SecretPins           *taskconfiguration.TaskSecretPinSet           `json:"secret_pins,omitempty"`
	OwnerKind            OwnerKind                                     `json:"owner_kind"`
	OwnerTaskID          string                                        `json:"owner_task_id"`
	Transfer             *Transfer                                     `json:"transfer,omitempty"`
}

func Key(revisionID, attachID string) string {
	return recordPrefix + revisionID + "/" + attachID
}

func ParentPrefix(parentTaskID string) string {
	return recordPrefix + parentTaskID + "/"
}

func ValidateDraft(record Generation) error {
	return validate(record, false)
}

func Validate(record Generation) error {
	return validate(record, true)
}

func MatchesConfiguration(record Generation, configuration backinghook.Configuration) bool {
	if configuration.Attach == nil {
		return false
	}
	cloned := backinghook.CloneConfiguration(&configuration)
	facts := append([]backinghook.FactDefinition(nil), cloned.Facts...)
	sort.Slice(facts, func(left, right int) bool { return facts[left].Key < facts[right].Key })
	inputs := make([]HookInput, len(cloned.Inputs))
	for index, input := range cloned.Inputs {
		source := HookInputResolved
		if input.Generate == backinghook.GeneratePassword {
			source = HookInputGenerated
		}
		inputs[index] = HookInput{Key: input.Key, Source: source}
	}
	sort.Slice(inputs, func(left, right int) bool { return inputs[left].Key < inputs[right].Key })
	return sameDefinition(record.Hook.Attach, cloned.Attach) &&
		sameDefinition(record.Hook.Detach, cloned.Detach) &&
		slices.Equal(record.Hook.Facts, facts) && slices.Equal(record.Hook.Inputs, inputs)
}

func BindSecretPins(record Generation, count uint64, digest string) (Generation, error) {
	if record.OwnerKind != "" || record.OwnerTaskID != "" || record.Transfer != nil {
		return Generation{}, errs.New(errs.KindValidationFailed, "Blueprint Attach draft ownership is invalid")
	}
	if len(record.SecretSources) == 0 {
		if count != 0 || digest != "" {
			return Generation{}, errs.New(errs.KindValidationFailed, "Blueprint Attach Secret pin binding is invalid")
		}
		record.SecretPins = nil
	} else {
		record.SecretPins = &taskconfiguration.TaskSecretPinSet{
			TaskID: record.ParentTaskID, Count: count, SHA256: digest,
		}
	}
	record.OwnerKind = OwnerParent
	record.OwnerTaskID = record.ParentTaskID
	if err := validate(record, true); err != nil {
		return Generation{}, err
	}
	return record, nil
}

// TransferToChild changes only lifecycle ownership. The immutable parent,
// source identities, encrypted values and original Secret-pin TaskID remain
// unchanged so replay can prove the exact admitted generation.
func TransferToChild(record Generation, childTaskID string) (Generation, error) {
	if err := validate(record, true); err != nil || record.OwnerKind != OwnerParent ||
		record.OwnerTaskID != record.ParentTaskID || record.Transfer != nil ||
		ids.Validate(ids.KindTask, childTaskID) != nil || childTaskID == record.ParentTaskID {
		return Generation{}, errs.New(errs.KindStateConflict, "Blueprint Attach input ownership cannot transfer")
	}
	record.OwnerKind = OwnerChild
	record.OwnerTaskID = childTaskID
	record.Transfer = &Transfer{ParentTaskID: record.ParentTaskID, ChildTaskID: childTaskID}
	if err := validate(record, true); err != nil {
		return Generation{}, err
	}
	return record, nil
}

func Encode(record Generation) ([]byte, error) {
	if err := validate(record, true); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint_attach_input_generation", record)
}

func Decode(value []byte) (Generation, error) {
	record, err := recordcodec.Decode[Generation](value, "blueprint_attach_input_generation")
	if err != nil || validate(record, true) != nil {
		Clear(&record)
		return Generation{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func Clear(record *Generation) {
	if record == nil {
		return
	}
	clear(record.GeneratedInputs.Ciphertext)
	if record.ResolvedInputs != nil {
		clear(record.ResolvedInputs.Ciphertext)
	}
	*record = Generation{}
}

func validate(record Generation, bound bool) error {
	if ids.Validate(ids.KindOperation, record.ID) != nil || record.ID != record.OperationID ||
		ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.RevisionID) != nil || record.RevisionID != record.ParentTaskID ||
		ids.Validate(ids.KindTask, record.ParentTaskID) != nil ||
		ids.Validate(
			ids.KindAttach,
			record.AttachID,
		) != nil || attachrecord.ValidateAttachName(record.AttachName) != nil ||
		!recordcodec.ValidSHA256(record.AuthoredSpecSHA256) ||
		ids.Validate(ids.KindService, record.ConsumerServiceID) != nil || record.CredentialOwnerID != record.AttachID ||
		ids.Validate(ids.KindProject, record.BackingProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, record.BackingEnvironmentID) != nil ||
		ids.Validate(ids.KindService, record.BackingServiceID) != nil ||
		ids.Validate(ids.KindNetwork, record.BackingNetworkID) != nil || record.AdapterKey == "" {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach input generation identity is invalid")
	}
	if _, err := core.ResolveBackingAuthentication(false, record.Authentication); err != nil {
		return err
	}
	probe := backinghook.Configuration{
		Attach: record.Hook.Attach, Detach: record.Hook.Detach,
		Facts: append([]backinghook.FactDefinition(nil), record.Hook.Facts...),
	}
	empty := ""
	for _, input := range record.Hook.Inputs {
		definition := backinghook.InputDefinition{Key: input.Key, Value: &empty}
		if input.Source == HookInputGenerated {
			definition.Value = nil
			definition.Generate = backinghook.GeneratePassword
		} else if input.Source != HookInputResolved {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach hook input source is invalid")
		}
		probe.Inputs = append(probe.Inputs, definition)
	}
	if record.Hook.Attach == nil || !slices.IsSortedFunc(record.Hook.Inputs, func(left, right HookInput) int {
		if left.Key < right.Key {
			return -1
		}
		if left.Key > right.Key {
			return 1
		}
		return 0
	}) ||
		backinghook.ValidateConfiguration(
			probe,
		) != nil || attachrecord.ValidateAttachEncryptedFacts(record.GeneratedInputs) != nil ||
		record.GeneratedInputs.AttachID != record.AttachID || !matchingFactSets(record.Hook.Facts, record.FactSets) {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach hook generation is invalid")
	}
	if record.ResolvedInputs != nil {
		if taskconfiguration.ValidateBackingHookEncryptedInputs(*record.ResolvedInputs) != nil ||
			record.ResolvedInputs.OperationID != record.OperationID {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach resolved inputs are invalid")
		}
	}
	if !slices.IsSortedFunc(record.SecretSources, func(left, right tasksecretpinrecord.Record) int {
		if left.SecretID < right.SecretID {
			return -1
		}
		if left.SecretID > right.SecretID {
			return 1
		}
		return 0
	}) {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach Secret sources are not canonical")
	}
	for index, source := range record.SecretSources {
		if tasksecretpinrecord.Validate(source) != nil || source.OperationID != record.OperationID ||
			(index > 0 && record.SecretSources[index-1].SecretID == source.SecretID) {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach Secret source is invalid")
		}
	}
	if record.ResolvedInputs == nil && len(record.SecretSources) != 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach Secret sources have no encrypted input")
	}
	if bound {
		if (len(record.SecretSources) == 0) != (record.SecretPins == nil) ||
			taskconfiguration.ValidateTaskSecretPinSet(record.SecretPins) != nil ||
			record.SecretPins != nil && (record.SecretPins.TaskID != record.ParentTaskID ||
				record.SecretPins.Count != uint64(len(record.SecretSources))) {
			return errs.New(errs.KindValidationFailed, "Blueprint Attach Secret pin ownership is invalid")
		}
		switch record.OwnerKind {
		case OwnerParent:
			if record.OwnerTaskID != record.ParentTaskID || record.Transfer != nil {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach parent ownership is invalid")
			}
		case OwnerChild:
			if ids.Validate(ids.KindTask, record.OwnerTaskID) != nil || record.OwnerTaskID == record.ParentTaskID ||
				record.Transfer == nil || record.Transfer.ParentTaskID != record.ParentTaskID ||
				record.Transfer.ChildTaskID != record.OwnerTaskID {
				return errs.New(errs.KindValidationFailed, "Blueprint Attach child ownership is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Blueprint Attach input owner kind is invalid")
		}
	} else if record.SecretPins != nil || record.OwnerKind != "" || record.OwnerTaskID != "" || record.Transfer != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach draft already owns retained authority")
	}
	return nil
}

func matchingFactSets(facts []backinghook.FactDefinition, sets []attachrecord.FactSetMetadata) bool {
	if len(facts) == 0 {
		return len(sets) == 0
	}
	if len(sets) != 1 || sets[0].GrantAttachID != "" || len(sets[0].Facts) != len(facts) {
		return false
	}
	for index := range facts {
		if sets[0].Facts[index].Key != facts[index].Key || sets[0].Facts[index].Secret != facts[index].Secret {
			return false
		}
	}
	return true
}

func sameDefinition(left, right *backinghook.Definition) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.TimeoutSeconds == right.TimeoutSeconds && slices.Equal(left.Command, right.Command)
}
