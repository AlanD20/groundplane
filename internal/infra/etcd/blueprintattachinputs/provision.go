package blueprintattachinputs

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Provision captures the declared binding and credential owner before any
// provisioning begins. Ready references carry their exact admission revisions;
// new references are dependencies on other units in the same Apply.
type Provision struct {
	HookOwnerOperationID string              `json:"hook_owner_operation_id,omitempty"`
	RequiresProvisioning bool                `json:"requires_provisioning"`
	Candidate            attachrecord.Record `json:"candidate"`
	Credential           BindingRecord       `json:"credential"`
	Grants               []BindingRecord     `json:"grants,omitempty"`
	FactsRevision        int64               `json:"facts_revision"`
}

func (provision Provision) Conditions() []etcdstore.Condition {
	var conditions []etcdstore.Condition
	if provision.Credential.Revision > 0 {
		conditions = append(conditions, etcdstore.Condition{
			Key: attachrecord.AttachKey(provision.Credential.Record.ID), ModRevision: provision.Credential.Revision,
		}, etcdstore.Condition{
			Key: attachrecord.AttachFactsKey(provision.Credential.Record.ID), ModRevision: provision.FactsRevision,
		})
	}
	for _, grant := range provision.Grants {
		if grant.Revision > 0 {
			conditions = append(conditions, etcdstore.Condition{
				Key: attachrecord.AttachKey(grant.Record.ID), ModRevision: grant.Revision,
			})
		}
	}
	return conditions
}

func (generation Generation) PendingRecord(taskID string, createdAt time.Time) (attachrecord.Record, error) {
	if generation.Provision == nil {
		if generation.Hook.Attach == nil && generation.Hook.Detach != nil {
			record, err := attachrecord.NewPendingAttachRecord(
				generation.AttachID,
				generation.EnvironmentID,
				generation.AttachName,
				generation.BackingProjectID,
				generation.BackingEnvironmentID,
				generation.BackingServiceID,
				generation.BackingNetworkID,
				generation.ConsumerServiceID,
				generation.CredentialOwnerID,
				nil,
				generation.FactSets,
				taskID,
				createdAt,
			)
			record.HookBundle = true
			return record, err
		}
		return attachrecord.Record{}, errs.New(errs.KindValidationFailed, "Attach provisioning input is missing")
	}
	record := attachrecord.CloneAttachRecord(generation.Provision.Candidate)
	record.TaskID, record.CreatedAt = taskID, createdAt.UTC()
	if err := attachrecord.ValidateAttachRecord(record); err != nil {
		return attachrecord.Record{}, err
	}
	return record, nil
}

func (generation Generation) SettlesInController() bool {
	return generation.Binding != nil ||
		(generation.Provision != nil && !generation.Provision.RequiresProvisioning) ||
		(generation.Provision == nil && generation.Hook.Attach == nil && generation.Hook.Detach != nil)
}

func (generation Generation) MatchesControllerBinding(record attachrecord.Record, revision int64) bool {
	if generation.Binding != nil {
		return generation.Binding.Consumer.Revision == revision &&
			attachrecord.SameBlueprintAttachCandidateRecord(generation.Binding.Consumer.Record, record)
	}
	if !generation.SettlesInController() {
		return false
	}
	createdAt := record.CreatedAt
	if generation.Provision != nil {
		createdAt = generation.Provision.Candidate.CreatedAt
	}
	expected, err := generation.PendingRecord(generation.ParentTaskID, createdAt)
	if err != nil {
		return false
	}
	expected.Status = core.AttachReady
	return attachrecord.SameBlueprintAttachCandidateRecord(expected, record)
}

func validateProvision(generation Generation) error {
	provision := generation.Provision
	if provision == nil || generation.Binding != nil || generation.Hook.Attach != nil ||
		generation.Hook.Detach != nil ||
		len(generation.Hook.Facts) != 0 ||
		len(generation.Hook.Inputs) != 0 ||
		generation.ResolvedInputs != nil ||
		len(generation.SecretSources) != 0 ||
		generation.SecretPins != nil {
		return errs.New(errs.KindValidationFailed, "Attach provisioning has conflicting hook or binding authority")
	}
	candidate, credential := provision.Candidate, provision.Credential.Record
	if attachrecord.ValidateAttachRecord(candidate) != nil || candidate.Status != core.AttachPending ||
		candidate.Operation != attachrecord.AttachOperationProvision || candidate.HookBundle ||
		candidate.TaskID != generation.ParentTaskID || candidate.ID != generation.AttachID ||
		candidate.EnvironmentID != generation.EnvironmentID || candidate.Name != generation.AttachName ||
		candidate.ServiceID != generation.ConsumerServiceID || candidate.CredentialAttachID != generation.CredentialOwnerID ||
		candidate.BackingProjectID != generation.BackingProjectID || candidate.BackingEnvironmentID != generation.BackingEnvironmentID ||
		candidate.BackingServiceID != generation.BackingServiceID || candidate.BackingNetworkID != generation.BackingNetworkID ||
		attachrecord.ValidateAttachRecord(credential) != nil || !credential.OwnsCredential() ||
		credential.ID != generation.CredentialOwnerID || credential.EnvironmentID != candidate.EnvironmentID ||
		credential.BackingServiceID != candidate.BackingServiceID || credential.BackingNetworkID != candidate.BackingNetworkID ||
		!slices.EqualFunc(candidate.FactSets, generation.FactSets, sameFactSet) ||
		!slices.EqualFunc(credential.FactSets, generation.FactSets, sameFactSet) ||
		len(
			provision.Grants,
		) != len(
			credential.GrantAttachIDs,
		) || !validProvisionReference(provision.Credential, generation.ParentTaskID) {
		return errs.New(errs.KindValidationFailed, "Attach provisioning identity is invalid")
	}
	deferredHook := provision.HookOwnerOperationID != ""
	if deferredHook && (candidate.OwnsCredential() || !credential.HookBundle || provision.Credential.Revision != 0 ||
		ids.Validate(ids.KindOperation, provision.HookOwnerOperationID) != nil || provision.FactsRevision != 0 ||
		generation.GeneratedInputs.AttachID != "" || len(generation.GeneratedInputs.Ciphertext) != 0) {
		return errs.New(errs.KindValidationFailed, "Attach hook credential dependency is invalid")
	}
	if (len(generation.FactSets) != 0 || credential.HookBundle) && !deferredHook {
		if attachrecord.ValidateAttachEncryptedFacts(generation.GeneratedInputs) != nil ||
			generation.GeneratedInputs.AttachID != credential.ID ||
			(provision.Credential.Revision > 0 && provision.FactsRevision <= 0) ||
			(provision.Credential.Revision == 0 && provision.FactsRevision != 0) {
			return errs.New(errs.KindValidationFailed, "Attach provisioning facts are invalid")
		}
	} else if provision.FactsRevision != 0 || generation.GeneratedInputs.AttachID != "" ||
		len(generation.GeneratedInputs.Ciphertext) != 0 {
		return errs.New(errs.KindValidationFailed, "Factless Attach provisioning has unexpected facts")
	}
	if provision.RequiresProvisioning &&
		(!candidate.OwnsCredential() || generation.Authentication == core.BackingAuthenticationNone ||
			len(generation.FactSets) == 0) {
		return errs.New(errs.KindValidationFailed, "Attach provisioning procedure has no owned authenticated identity")
	}
	for index, grant := range provision.Grants {
		if !validProvisionReference(grant, generation.ParentTaskID) || !grant.Record.OwnsCredential() ||
			grant.Record.ID != credential.GrantAttachIDs[index] || grant.Record.EnvironmentID != candidate.EnvironmentID ||
			grant.Record.BackingServiceID != candidate.BackingServiceID {
			return errs.New(errs.KindValidationFailed, "Attach provisioning grant is invalid")
		}
	}
	return nil
}

func validProvisionReference(reference BindingRecord, parentID string) bool {
	if attachrecord.ValidateAttachRecord(reference.Record) != nil ||
		reference.Record.Operation != attachrecord.AttachOperationProvision {
		return false
	}
	return validBindingRecord(reference) || (reference.Revision == 0 && reference.Record.Status == core.AttachPending &&
		reference.Record.TaskID == parentID)
}

func sameFactSet(left, right attachrecord.FactSetMetadata) bool {
	return left.GrantAttachID == right.GrantAttachID && slices.Equal(left.Facts, right.Facts)
}
