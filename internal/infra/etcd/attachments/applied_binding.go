package attachments

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AppliedBinding is resource state. It retains the data needed to resolve an
// acknowledged connection after execution history and provisioning inputs expire.
// It deliberately excludes hook commands, resolved hook inputs and Secret pins.
type AppliedBinding struct {
	TaskID             string              `json:"task_id"`
	WorkItemID         string              `json:"work_item_id"`
	OperationID        string              `json:"operation_id"`
	Input              workexecution.Input `json:"input"`
	AuthoredSpecSHA256 string              `json:"authored_spec_sha256"`
	Consumer           Record              `json:"consumer"`
	Credential         Record              `json:"credential"`
	Grants             []Record            `json:"grants,omitempty"`
	Facts              *EncryptedFacts     `json:"facts,omitempty"`
}

func AppliedBindingKey(attachID string) string { return "/v1/runtime/attach-bindings/" + attachID }

func (binding AppliedBinding) Validate() error {
	consumer, credential := binding.Consumer, binding.Credential
	if ids.Validate(ids.KindTask, binding.TaskID) != nil || ids.Validate(ids.KindWorkItem, binding.WorkItemID) != nil ||
		ids.Validate(ids.KindOperation, binding.OperationID) != nil || binding.Input.Validate() != nil ||
		!recordcodec.ValidSHA256(binding.AuthoredSpecSHA256) || ValidateAttachRecord(consumer) != nil || ValidateAttachRecord(credential) != nil ||
		consumer.Status != core.AttachReady || credential.Status != core.AttachReady ||
		consumer.Operation != AttachOperationProvision || credential.Operation != AttachOperationProvision ||
		!credential.OwnsCredential() || consumer.CredentialAttachID != credential.ID ||
		consumer.EnvironmentID != credential.EnvironmentID || consumer.BackingServiceID != credential.BackingServiceID ||
		consumer.BackingNetworkID != credential.BackingNetworkID || len(binding.Grants) != len(credential.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "applied Attach binding ownership is invalid")
	}
	for index, grant := range binding.Grants {
		if ValidateAttachRecord(grant) != nil || grant.Status != core.AttachReady || grant.Operation != AttachOperationProvision ||
			grant.ID != credential.GrantAttachIDs[index] || !grant.OwnsCredential() || grant.EnvironmentID != credential.EnvironmentID ||
			grant.BackingServiceID != credential.BackingServiceID {
			return errs.New(errs.KindValidationFailed, "applied Attach grant ownership is invalid")
		}
	}
	if len(credential.FactSets) != 0 || credential.HookBundle {
		if binding.Facts == nil || ValidateAttachEncryptedFacts(*binding.Facts) != nil || binding.Facts.AttachID != credential.ID {
			return errs.New(errs.KindValidationFailed, "applied Attach facts are unavailable")
		}
	} else if binding.Facts != nil {
		return errs.New(errs.KindValidationFailed, "factless applied Attach contains an envelope")
	}
	return nil
}

func EncodeAppliedBinding(binding AppliedBinding) ([]byte, error) {
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("applied_attach_binding", binding)
}

func DecodeAppliedBinding(raw []byte) (AppliedBinding, error) {
	binding, err := recordcodec.Decode[AppliedBinding](raw, "applied_attach_binding")
	if err != nil || binding.Validate() != nil {
		binding.Clear()
		return AppliedBinding{}, recordcodec.CorruptRecord()
	}
	return binding, nil
}

func (binding *AppliedBinding) Clear() {
	if binding != nil && binding.Facts != nil {
		clear(binding.Facts.Ciphertext)
	}
}

func (binding AppliedBinding) Clone() AppliedBinding {
	binding.Consumer = CloneAttachRecord(binding.Consumer)
	binding.Credential = CloneAttachRecord(binding.Credential)
	binding.Grants = slices.Clone(binding.Grants)
	for index := range binding.Grants {
		binding.Grants[index] = CloneAttachRecord(binding.Grants[index])
	}
	if binding.Facts != nil {
		facts := *binding.Facts
		facts.Ciphertext = slices.Clone(facts.Ciphertext)
		binding.Facts = &facts
	}
	return binding
}
