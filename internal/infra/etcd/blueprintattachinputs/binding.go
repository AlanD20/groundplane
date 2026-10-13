package blueprintattachinputs

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BindingRecord struct {
	Record   attachrecord.Record `json:"record"`
	Revision int64               `json:"revision"`
}

// CapturedBinding adopts an already acknowledged Attach without provisioning
// it again. Facts remain encrypted in Generation.GeneratedInputs. Every source
// revision is compared at admission and again at Controller settlement.
type CapturedBinding struct {
	Consumer      BindingRecord   `json:"consumer"`
	Credential    BindingRecord   `json:"credential"`
	Grants        []BindingRecord `json:"grants,omitempty"`
	FactsRevision int64           `json:"facts_revision"`
}

func (binding CapturedBinding) Conditions() []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{Key: attachrecord.AttachKey(binding.Consumer.Record.ID), ModRevision: binding.Consumer.Revision},
	}
	if binding.Credential.Record.ID != binding.Consumer.Record.ID {
		conditions = append(conditions, etcdstore.Condition{
			Key: attachrecord.AttachKey(binding.Credential.Record.ID), ModRevision: binding.Credential.Revision,
		})
	}
	conditions = append(conditions, etcdstore.Condition{
		Key: attachrecord.AttachFactsKey(binding.Credential.Record.ID), ModRevision: binding.FactsRevision,
	})
	for _, grant := range binding.Grants {
		if grant.Record.ID != binding.Consumer.Record.ID && grant.Record.ID != binding.Credential.Record.ID {
			conditions = append(conditions, etcdstore.Condition{
				Key: attachrecord.AttachKey(grant.Record.ID), ModRevision: grant.Revision,
			})
		}
	}
	return conditions
}

func validateCapturedBinding(generation Generation) error {
	binding := generation.Binding
	hasHook := generation.Hook.Attach != nil || generation.Hook.Detach != nil ||
		len(generation.Hook.Facts) != 0 || len(generation.Hook.Inputs) != 0
	hasPrivateInputs := generation.ResolvedInputs != nil || len(generation.SecretSources) != 0 ||
		generation.SecretPins != nil
	if binding == nil || generation.Provision != nil || hasHook || hasPrivateInputs ||
		generation.OwnerKind == OwnerChild || generation.Transfer != nil {
		return errs.New(errs.KindValidationFailed, "captured Attach binding has conflicting execution authority")
	}
	consumer, credential := binding.Consumer.Record, binding.Credential.Record
	if !validBindingRecord(binding.Consumer) || !validBindingRecord(binding.Credential) ||
		consumer.ID != generation.AttachID || consumer.Name != generation.AttachName ||
		consumer.ServiceID != generation.ConsumerServiceID || consumer.EnvironmentID != generation.EnvironmentID ||
		credential.ID != generation.CredentialOwnerID || !credential.OwnsCredential() ||
		consumer.CredentialAttachID != credential.ID || credential.EnvironmentID != consumer.EnvironmentID ||
		consumer.BackingProjectID != generation.BackingProjectID ||
		consumer.BackingEnvironmentID != generation.BackingEnvironmentID ||
		consumer.BackingServiceID != generation.BackingServiceID || consumer.BackingNetworkID != generation.BackingNetworkID ||
		credential.BackingServiceID != consumer.BackingServiceID ||
		!slices.EqualFunc(generation.FactSets, credential.FactSets, sameFactSet) ||
		len(binding.Grants) != len(credential.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "captured Attach binding identity is invalid")
	}
	if len(credential.FactSets) != 0 || credential.HookBundle {
		if binding.FactsRevision <= 0 || attachrecord.ValidateAttachEncryptedFacts(generation.GeneratedInputs) != nil ||
			generation.GeneratedInputs.AttachID != credential.ID {
			return errs.New(errs.KindValidationFailed, "captured Attach credential facts are invalid")
		}
	} else if binding.FactsRevision != 0 || generation.GeneratedInputs.AttachID != "" ||
		len(generation.GeneratedInputs.Ciphertext) != 0 || generation.GeneratedInputs.EnvelopeVersion != 0 ||
		generation.GeneratedInputs.Cipher != "" || generation.GeneratedInputs.DigestAlgorithm != "" ||
		generation.GeneratedInputs.CiphertextSHA256 != "" {
		return errs.New(errs.KindValidationFailed, "factless Attach binding has unexpected encrypted facts")
	}
	for index, grant := range binding.Grants {
		if !validBindingRecord(grant) || grant.Record.EnvironmentID != consumer.EnvironmentID ||
			grant.Record.BackingServiceID != consumer.BackingServiceID ||
			grant.Record.ID != credential.GrantAttachIDs[index] {
			return errs.New(errs.KindValidationFailed, "captured Attach grant identity is invalid")
		}
	}
	return nil
}

func validBindingRecord(binding BindingRecord) bool {
	return binding.Revision > 0 && attachrecord.ValidateAttachRecord(binding.Record) == nil &&
		binding.Record.Status == core.AttachReady && binding.Record.Operation == attachrecord.AttachOperationProvision
}
