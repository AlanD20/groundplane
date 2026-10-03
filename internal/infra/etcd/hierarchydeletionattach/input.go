// Package hierarchydeletionattach owns the encrypted, immutable input for
// hierarchy-owned Attach credential cleanup.
package hierarchydeletionattach

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumInputBytes = 1 << 20

type Input struct {
	Schema                 int                                           `json:"schema"`
	ParentOperationID      string                                        `json:"parent_operation_id"`
	SnapshotRevision       int64                                         `json:"snapshot_revision"`
	ActionOrdinal          int64                                         `json:"action_ordinal"`
	PlanID                 string                                        `json:"plan_id,omitempty"`
	TaskID                 string                                        `json:"task_id,omitempty"`
	Attach                 attachrecord.Record                           `json:"attach"`
	AttachRevision         int64                                         `json:"attach_revision"`
	Facts                  *attachrecord.EncryptedFacts                  `json:"facts,omitempty"`
	FactsRevision          int64                                         `json:"facts_revision"`
	Backing                servicerecord.ServiceRecord                   `json:"backing"`
	BackingRevision        int64                                         `json:"backing_revision"`
	BackingDesiredRevision int64                                         `json:"backing_desired_revision"`
	TenantID               string                                        `json:"tenant_id"`
	ProjectID              string                                        `json:"project_id"`
	HookInputs             *taskconfiguration.BackingHookEncryptedInputs `json:"hook_inputs,omitempty"`
	HookInputSet           *taskconfiguration.TaskBackingHookInputSet    `json:"hook_input_set,omitempty"`
	HookOperationID        string                                        `json:"hook_operation_id,omitempty"`
}

func FrozenPrefix(operationID string) string {
	return "/v1/private/hierarchy-deletion-attach-inputs/" + operationID + "/frozen/"
}

func FrozenKey(operationID, attachID string) string { return FrozenPrefix(operationID) + attachID }
func ParentPlanPrefix(operationID string) string {
	return "/v1/private/hierarchy-deletion-attach-inputs/" + operationID + "/plans/"
}
func ParentPlanKey(operationID, planID string) string { return ParentPlanPrefix(operationID) + planID }
func PlanKey(planID string) string                    { return "/v1/indexes/hierarchy-deletion-attach-plans/" + planID }

func Validate(input Input) error {
	if input.Schema != 1 || !hierarchydeletion.ValidHierarchyDeletionPrivateID(input.ParentOperationID, "del") ||
		input.SnapshotRevision <= 0 || input.AttachRevision <= 0 || input.AttachRevision > input.SnapshotRevision ||
		input.BackingRevision < 0 || input.BackingDesiredRevision <= 0 ||
		input.BackingDesiredRevision > input.SnapshotRevision || input.BackingRevision > input.SnapshotRevision ||
		ids.Validate(ids.KindTenant, input.TenantID) != nil || ids.Validate(ids.KindProject, input.ProjectID) != nil ||
		attachrecord.ValidateAttachRecord(
			input.Attach,
		) != nil || servicerecord.ValidateServiceRecord(input.Backing) != nil ||
		input.Backing.Desired.ID != input.Attach.BackingServiceID || input.Backing.EnvironmentID != input.Attach.BackingEnvironmentID ||
		input.Backing.BackingNetworkID != input.Attach.BackingNetworkID {
		return errs.New(errs.KindValidationFailed, "hierarchy Attach cleanup input is invalid")
	}
	if input.PlanID == "" {
		if input.TaskID != "" || input.ActionOrdinal != -1 {
			return errs.New(errs.KindValidationFailed, "frozen hierarchy Attach input contains Task authority")
		}
	} else if ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindTask, input.TaskID) != nil || input.ActionOrdinal < 0 {
		return errs.New(errs.KindValidationFailed, "hierarchy Attach cleanup Task identity is invalid")
	}
	if input.Facts == nil {
		if input.FactsRevision != 0 {
			return errs.New(errs.KindValidationFailed, "hierarchy Attach cleanup facts revision is invalid")
		}
	} else if attachrecord.ValidateAttachEncryptedFacts(*input.Facts) != nil ||
		input.Facts.AttachID != input.Attach.ID || input.FactsRevision <= 0 || input.FactsRevision > input.SnapshotRevision {
		return errs.New(errs.KindValidationFailed, "hierarchy Attach cleanup encrypted facts are invalid")
	}
	if input.HookInputSet == nil && (input.HookInputs != nil || input.HookOperationID != "") ||
		input.HookInputSet != nil &&
			(ids.Validate(ids.KindOperation, input.HookOperationID) != nil || input.HookInputSet.ProjectID != input.Attach.BackingProjectID ||
				!recordcodec.ValidSHA256(input.HookInputSet.CiphertextSHA256)) {
		return errs.New(errs.KindValidationFailed, "hierarchy Attach cleanup hook inputs are invalid")
	}
	if input.HookInputSet != nil {
		for _, pin := range input.HookInputSet.SecretSources {
			if tasksecretpinrecord.Validate(pin) != nil || pin.OperationID != input.HookOperationID {
				return errs.New(errs.KindValidationFailed, "hierarchy Attach hook Secret source is invalid")
			}
		}
	}
	if input.HookInputs != nil && (taskconfiguration.ValidateBackingHookEncryptedInputs(*input.HookInputs) != nil ||
		input.HookInputs.CiphertextSHA256 != input.HookInputSet.CiphertextSHA256 || input.HookInputs.OperationID != input.HookOperationID) {
		return errs.New(errs.KindValidationFailed, "hierarchy Attach hook ciphertext differs from its authority")
	}
	return nil
}

func Encode(input Input) ([]byte, error) {
	if err := Validate(input); err != nil {
		return nil, err
	}
	stored := input
	stored.HookInputs = nil
	value, err := recordcodec.Encode("hierarchy-deletion-attach-input", stored)
	if err != nil {
		return nil, err
	}
	if len(value) > MaximumInputBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "hierarchy Attach cleanup input exceeds its bound")
	}
	return value, nil
}

func Decode(value []byte) (Input, error) {
	if len(value) > MaximumInputBytes {
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	input, err := recordcodec.Decode[Input](value, "hierarchy-deletion-attach-input")
	if input.HookInputs != nil && input.HookInputSet != nil {
		input.HookInputs.SecretSources = input.HookInputSet.SecretSources
	}
	if err != nil || Validate(input) != nil {
		Clear(&input)
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return input, nil
}

func Clear(input *Input) {
	if input == nil {
		return
	}
	if input.Facts != nil {
		clear(input.Facts.Ciphertext)
	}
	if input.HookInputs != nil {
		clear(input.HookInputs.Ciphertext)
	}
}

func Digest(input Input) (string, error) {
	value, err := Encode(input)
	if err != nil {
		return "", err
	}
	defer clear(value)
	return hierarchydeletion.HierarchyDeletionBytesDigest(value), nil
}

type Reader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

func Read(ctx context.Context, store Reader, key string) (Input, int64, error) {
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return Input{}, 0, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Input{}, 0, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	input, err := Decode(read.Values[0].Value)
	if err == nil {
		err = LoadHookInputs(ctx, store, &input)
	}
	if err != nil {
		Clear(&input)
		return Input{}, 0, err
	}
	return input, read.Values[0].ModRevision, err
}

func LoadHookInputs(ctx context.Context, store Reader, input *Input) error {
	if input.HookInputSet == nil {
		return nil
	}
	key := taskconfiguration.BackingHookTaskInputKey(input.HookOperationID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	stored, err := taskconfiguration.DecodeBackingHookEncryptedInputs(read.Values[0].Value)
	if err != nil {
		return err
	}
	stored.SecretSources = append([]tasksecretpinrecord.Record(nil), input.HookInputSet.SecretSources...)
	input.HookInputs = &stored
	return Validate(*input)
}

// SameBackingDesired compares the captured typed selection without relying on
// mutable runtime timestamps or internal reader metadata.
func SameBackingDesired(left, right servicerecord.ServiceRecord) bool {
	l, le := json.Marshal(left.Desired)
	r, re := json.Marshal(right.Desired)
	defer clear(l)
	defer clear(r)
	return le == nil && re == nil && string(l) == string(r) && left.EnvironmentID == right.EnvironmentID &&
		left.BackingNetworkID == right.BackingNetworkID
}
