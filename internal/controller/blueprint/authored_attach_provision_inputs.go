package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareAuthoredAttachProvisionInputs(
	ctx context.Context, environmentID, parentID string, specs map[string]core.AttachmentSpec,
	services []core.Service, current []etcdstore.Versioned[attachrecord.Record],
	owned projectionrecord.EnvironmentOwnedIdentities, allocator *desiredrevision.BlueprintIdentityAllocator,
	preparedInputs []etcd.BlueprintAttachInputGenerationPublication,
) ([]etcd.BlueprintAttachInputGenerationPublication, error) {
	hookOwners := make(map[string]etcd.BlueprintAttachInputGenerationPublication)
	for _, input := range preparedInputs {
		if input.Generation.Hook.Attach != nil || input.Generation.Hook.Detach != nil {
			hookOwners[input.Generation.AttachName] = input
		}
	}
	deferred := make(map[string]core.AttachmentSpec)
	ordinary := make(map[string]core.AttachmentSpec, len(specs))
	for name, spec := range specs {
		if _, found := hookOwners[spec.Credential.Attach]; spec.Credential.Mode == "existing" && found {
			deferred[name] = spec
		} else {
			ordinary[name] = spec
		}
	}
	changes := make([]blueprints.EnvironmentBlueprintServiceChange, len(services))
	for index, desired := range services {
		changes[index].Record = servicerecord.ServiceRecord{EnvironmentID: environmentID, Desired: desired}
	}
	attachIDs := make(map[string]string, len(owned.Attaches))
	for _, identity := range owned.Attaches {
		attachIDs[identity.Name] = identity.ID
	}
	allocate := func(kind ids.Kind, name string) string {
		for key, id := range attachIDs {
			if kind == ids.KindAttach && name == "attach:"+key {
				return id
			}
		}
		return allocator.Named(kind, name)
	}
	prepared, err := service.prepareBlueprintAttachCandidates(
		ctx, environmentID, parentID, ordinary, changes, current, allocate, service.now().UTC(),
	)
	if err != nil {
		return nil, err
	}
	defer prepared.clear()
	byID := make(map[string]attachinputs.BindingRecord, len(current)+len(prepared.candidates))
	for _, binding := range current {
		byID[binding.Record.ID] = attachinputs.BindingRecord{Record: binding.Record, Revision: binding.Revision}
	}
	factsByID := make(map[string]*attachrecord.EncryptedFacts, len(prepared.candidates))
	for _, candidate := range prepared.candidates {
		byID[candidate.Record.ID] = attachinputs.BindingRecord{Record: candidate.Record}
		factsByID[candidate.Record.ID] = candidate.Facts
	}
	reader, ok := service.repository.(authoredAttachFactReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Attach provisioning fact reader is unavailable")
	}
	publications := make([]etcd.BlueprintAttachInputGenerationPublication, 0, len(prepared.candidates))
	failed := true
	defer func() {
		if failed {
			clearAuthoredAttachInputGenerations(publications)
		}
	}()
	for _, candidate := range prepared.candidates {
		credential, found := byID[candidate.Record.CredentialAttachID]
		if !found {
			return nil, errs.New(errs.KindStateConflict, "Attach provisioning credential owner is missing")
		}
		adapter, registered := adapters.Get(candidate.BackingService.Record.Desired.Adapter)
		if !registered {
			return nil, errs.New(errs.KindValidationFailed, "Attach provisioning adapter is unavailable")
		}
		provision := &attachinputs.Provision{Candidate: candidate.Record, Credential: credential,
			RequiresProvisioning: candidate.Record.OwnsCredential() && !adapter.Custom() &&
				candidate.BackingService.Record.Desired.Authentication != core.BackingAuthenticationNone}
		for _, id := range credential.Record.GrantAttachIDs {
			grant, found := byID[id]
			if !found {
				return nil, errs.New(errs.KindStateConflict, "Attach provisioning grant is missing")
			}
			provision.Grants = append(provision.Grants, grant)
		}
		var encrypted attachrecord.EncryptedFacts
		if credential.Revision > 0 {
			var versioned etcdstore.Versioned[attachrecord.Record]
			for _, binding := range current {
				if binding.Record.ID == credential.Record.ID {
					versioned = binding
					break
				}
			}
			facts, found, err := reader.GetAttachFactsVersioned(ctx, versioned)
			if err != nil {
				return nil, err
			}
			if found {
				encrypted, provision.FactsRevision = facts.Record, facts.Revision
			}
		} else if facts := factsByID[credential.Record.ID]; facts != nil {
			encrypted = *facts
			encrypted.Ciphertext = slices.Clone(facts.Ciphertext)
		}
		operationID := allocator.Named(ids.KindOperation, "attach-input:"+candidate.Record.Name)
		fingerprint, err := core.AttachmentSpecFingerprint(specs[candidate.Record.Name])
		if err != nil {
			clear(encrypted.Ciphertext)
			return nil, err
		}
		generation := attachinputs.Generation{
			ID: operationID, OperationID: operationID, ParentTaskID: parentID, RevisionID: parentID,
			EnvironmentID: environmentID, AttachID: candidate.Record.ID, AttachName: candidate.Record.Name,
			AuthoredSpecSHA256: fingerprint, ConsumerServiceID: candidate.Record.ServiceID,
			CredentialOwnerID: candidate.Record.CredentialAttachID, BackingProjectID: candidate.Record.BackingProjectID,
			BackingEnvironmentID: candidate.Record.BackingEnvironmentID, BackingServiceID: candidate.Record.BackingServiceID,
			BackingNetworkID: candidate.Record.BackingNetworkID, AdapterKey: candidate.BackingService.Record.Desired.Adapter,
			Authentication: candidate.BackingService.Record.Desired.Authentication, FactSets: candidate.Record.FactSets,
			GeneratedInputs: encrypted, Provision: provision,
		}
		if err := attachinputs.ValidateDraft(generation); err != nil {
			attachinputs.Clear(&generation)
			return nil, err
		}
		publications = append(publications, etcd.BlueprintAttachInputGenerationPublication{
			Generation: generation, BackingProject: candidate.BackingProject,
			BackingEnvironment: candidate.BackingEnvironment, BackingService: candidate.BackingService,
		})
	}
	for _, name := range sortedAuthoredMapKeys(deferred) {
		spec := deferred[name]
		owner := hookOwners[spec.Credential.Attach]
		generation := owner.Generation
		ownerRecord, err := attachrecord.NewPendingAttachRecord(
			generation.AttachID,
			environmentID,
			generation.AttachName,
			generation.BackingProjectID,
			generation.BackingEnvironmentID,
			generation.BackingServiceID,
			generation.BackingNetworkID,
			generation.ConsumerServiceID,
			generation.AttachID,
			nil,
			generation.FactSets,
			parentID,
			service.now().UTC(),
		)
		if err != nil {
			return nil, err
		}
		ownerRecord.HookBundle = true
		consumerID := ""
		for _, consumer := range services {
			if consumer.Name == spec.Service {
				consumerID = consumer.ID
				break
			}
		}
		candidate, err := attachrecord.NewPendingAttachRecord(attachIDs[name], environmentID, name,
			generation.BackingProjectID, generation.BackingEnvironmentID, generation.BackingServiceID,
			generation.BackingNetworkID, consumerID, generation.AttachID, nil,
			generation.FactSets, parentID, service.now().UTC())
		if err != nil {
			return nil, err
		}
		fingerprint, err := core.AttachmentSpecFingerprint(spec)
		if err != nil {
			return nil, err
		}
		operationID := allocator.Named(ids.KindOperation, "attach-input:"+name)
		alias := attachinputs.Generation{
			ID: operationID, OperationID: operationID, ParentTaskID: parentID, RevisionID: parentID,
			EnvironmentID: environmentID, AttachID: candidate.ID, AttachName: name, AuthoredSpecSHA256: fingerprint,
			ConsumerServiceID: consumerID, CredentialOwnerID: generation.AttachID, AdapterKey: generation.AdapterKey,
			Authentication: generation.Authentication, BackingProjectID: generation.BackingProjectID,
			BackingEnvironmentID: generation.BackingEnvironmentID, BackingServiceID: generation.BackingServiceID,
			BackingNetworkID: generation.BackingNetworkID, FactSets: generation.FactSets,
			Provision: &attachinputs.Provision{Candidate: candidate,
				Credential: attachinputs.BindingRecord{
					Record: ownerRecord,
				}, HookOwnerOperationID: generation.OperationID},
		}
		if generation.Hook.Attach == nil {
			alias.Provision.HookOwnerOperationID = ""
			alias.GeneratedInputs = generation.GeneratedInputs
			alias.GeneratedInputs.Ciphertext = slices.Clone(generation.GeneratedInputs.Ciphertext)
		}
		if err := attachinputs.ValidateDraft(alias); err != nil {
			attachinputs.Clear(&alias)
			return nil, err
		}
		publications = append(publications, etcd.BlueprintAttachInputGenerationPublication{
			Generation: alias, BackingProject: owner.BackingProject, BackingEnvironment: owner.BackingEnvironment,
			BackingService: owner.BackingService,
		})
	}
	failed = false
	return publications, nil
}
