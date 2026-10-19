package attachments

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// captureCreateInput captures the same resource intent used by Blueprint
// reconciliation, but belongs to the direct action's Task. It does not render
// desired workloads or construct an executable generic plan.
func (service *MutationService) captureCreateInput(ctx context.Context, scope etcd.AttachCreateScope,
	adapter adapters.Adapter, attachID, name, taskID, operationID string, at time.Time,
) (_ blueprintattachinputs.Generation, resultErr error) {
	if len(scope.Services) != 1 || adapter == nil {
		return blueprintattachinputs.Generation{}, errs.New(
			errs.KindValidationFailed,
			"Attach input requires one consumer and its backing adapter",
		)
	}
	consumer := scope.Services[0]
	generation := blueprintattachinputs.Generation{ID: operationID, OperationID: operationID,
		ParentTaskID: taskID, RevisionID: taskID, EnvironmentID: scope.Environment.Record.ID,
		AttachID: attachID, AttachName: name, ConsumerServiceID: consumer.Record.Desired.ID,
		CredentialOwnerID: attachID, BackingProjectID: scope.BackingProject.Record.ID,
		BackingEnvironmentID: scope.BackingEnvironment.Record.ID, BackingServiceID: scope.BackingService.Record.Desired.ID,
		BackingNetworkID: scope.BackingService.Record.BackingNetworkID, AdapterKey: adapter.Key(),
		Authentication: scope.BackingService.Record.Desired.Authentication}
	defer func() {
		if resultErr != nil {
			blueprintattachinputs.Clear(&generation)
		}
	}()
	spec := core.AttachmentSpec{BackingProject: scope.BackingProject.Record.Slug,
		BackingService: scope.BackingService.Record.Desired.Name, Service: consumer.Record.Desired.Name,
		Credential: core.AttachmentCredentialSpec{Mode: "new"}}
	for _, grant := range scope.Grants {
		spec.Grants = append(spec.Grants, grant.Record.Name)
	}
	sort.Strings(spec.Grants)
	var factsRevision int64
	if scope.CredentialOwner != nil {
		owner := *scope.CredentialOwner
		generation.CredentialOwnerID = owner.Record.ID
		generation.FactSets = cloneAttachFactMetadata(owner.Record.FactSets)
		spec.Credential = core.AttachmentCredentialSpec{Mode: "existing", Attach: owner.Record.Name}
		facts, found, err := service.repository.GetAttachFactsVersioned(ctx, owner)
		if err != nil {
			return blueprintattachinputs.Generation{}, err
		}
		if found {
			generation.GeneratedInputs, factsRevision = facts.Record, facts.Revision
		}
	} else {
		identity, metadata, facts, resolved, err := service.prepareAttachFacts(ctx, attachID, operationID, consumer, scope, adapter)
		if identity != nil {
			identity.Clear()
		}
		if err != nil {
			return blueprintattachinputs.Generation{}, err
		}
		generation.FactSets, generation.ResolvedInputs = metadata, resolved
		if facts != nil {
			generation.GeneratedInputs = *facts
		}
		if resolved != nil {
			generation.SecretSources = slices.Clone(resolved.SecretSources)
		}
	}
	var err error
	generation.AuthoredSpecSHA256, err = core.AttachmentSpecFingerprint(spec)
	if err != nil {
		return blueprintattachinputs.Generation{}, err
	}
	hooks := scope.BackingService.Record.Desired.Hooks
	if scope.CredentialOwner == nil && adapter.Custom() && hooks != nil &&
		(hooks.Attach != nil || hooks.Detach != nil) {
		configuration := backinghook.CloneConfiguration(hooks)
		generation.Hook = blueprintattachinputs.HookPlan{Attach: configuration.Attach, Detach: configuration.Detach,
			Facts: configuration.Facts}
		sort.Slice(
			generation.Hook.Facts,
			func(left, right int) bool { return generation.Hook.Facts[left].Key < generation.Hook.Facts[right].Key },
		)
		for _, input := range configuration.Inputs {
			source := blueprintattachinputs.HookInputResolved
			if input.Generate == backinghook.GeneratePassword {
				source = blueprintattachinputs.HookInputGenerated
			}
			generation.Hook.Inputs = append(
				generation.Hook.Inputs,
				blueprintattachinputs.HookInput{Key: input.Key, Source: source},
			)
		}
		sort.Slice(generation.Hook.Inputs, func(left, right int) bool {
			return generation.Hook.Inputs[left].Key < generation.Hook.Inputs[right].Key
		})
	} else {
		candidate, err := attachrecord.NewPendingAttachRecord(attachID, generation.EnvironmentID, name,
			generation.BackingProjectID, generation.BackingEnvironmentID, generation.BackingServiceID, generation.BackingNetworkID,
			generation.ConsumerServiceID, generation.CredentialOwnerID, attachGrantIDs(scope.Grants), generation.FactSets, taskID, at)
		if err != nil {
			return blueprintattachinputs.Generation{}, err
		}
		provision := &blueprintattachinputs.Provision{Candidate: candidate,
			Credential: blueprintattachinputs.BindingRecord{Record: candidate}, FactsRevision: factsRevision,
			RequiresProvisioning: scope.CredentialOwner == nil && !adapter.Custom() && generation.Authentication != core.BackingAuthenticationNone}
		if scope.CredentialOwner != nil {
			provision.Credential = blueprintattachinputs.BindingRecord{Record: attachrecord.CloneAttachRecord(scope.CredentialOwner.Record), Revision: scope.CredentialOwner.Revision}
		}
		// Existing credentials retain their owner's grants, not a second set
		// chosen by the connecting Service.
		for _, id := range provision.Credential.Record.GrantAttachIDs {
			var reference blueprintattachinputs.BindingRecord
			for _, grant := range scope.Grants {
				if grant.Record.ID == id {
					reference = blueprintattachinputs.BindingRecord{Record: attachrecord.CloneAttachRecord(grant.Record), Revision: grant.Revision}
					break
				}
			}
			if reference.Revision == 0 {
				grant, err := service.repository.GetAttach(ctx, id)
				if err != nil {
					return blueprintattachinputs.Generation{}, err
				}
				reference = blueprintattachinputs.BindingRecord{Record: attachrecord.CloneAttachRecord(grant.Record), Revision: grant.Revision}
			}
			provision.Grants = append(provision.Grants, reference)
		}
		generation.Provision = provision
	}
	if err := blueprintattachinputs.ValidateDraft(generation); err != nil {
		return blueprintattachinputs.Generation{}, err
	}
	return generation, nil
}
