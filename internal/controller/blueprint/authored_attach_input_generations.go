package blueprint

import (
	"context"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareAuthoredAttachInputGenerations(
	ctx context.Context,
	environmentID string,
	revisionID string,
	specs map[string]core.AttachmentSpec,
	services []core.Service,
	current []etcdstore.Versioned[attachrecord.Record],
	owned projectionrecord.EnvironmentOwnedIdentities,
	allocator *desiredrevision.BlueprintIdentityAllocator,
) ([]etcd.BlueprintAttachInputGenerationPublication, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	serviceByName := make(map[string]core.Service, len(services))
	for _, candidate := range services {
		serviceByName[candidate.Name] = candidate
	}
	currentByName := make(map[string]struct{}, len(current))
	for _, candidate := range current {
		currentByName[candidate.Record.Name] = struct{}{}
	}
	attachIDByName := make(map[string]string, len(owned.Attaches))
	for _, identity := range owned.Attaches {
		attachIDByName[identity.Name] = identity.ID
	}
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	publications := make([]etcd.BlueprintAttachInputGenerationPublication, 0, len(names))
	failed := true
	defer func() {
		if failed {
			clearAuthoredAttachInputGenerations(publications)
		}
	}()
	for _, name := range names {
		spec := specs[name]
		if _, exists := currentByName[name]; exists || spec.Credential.Mode != "new" {
			continue
		}
		consumer, exists := serviceByName[spec.Service]
		if !exists {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach consumer Service is not authored by the Environment",
			)
		}
		backingProject, err := service.repository.ResolveBackingProject(ctx, spec.BackingProject)
		if err != nil {
			return nil, err
		}
		backingEnvironment, err := service.repository.ResolveEnvironment(ctx, backingProject.Record.ID, "main")
		if err != nil {
			return nil, err
		}
		backingServices, err := service.listBlueprintServices(ctx, backingEnvironment.Record.ID)
		if err != nil {
			return nil, err
		}
		var backingService etcdstore.Versioned[servicerecord.ServiceRecord]
		for _, candidate := range backingServices {
			if candidate.Record.Desired.Name == spec.BackingService {
				backingService = candidate
				break
			}
		}
		if backingService.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindServiceNotFound, "Blueprint Attach backing Service was not found")
		}
		adapter, registered := adapters.Get(backingService.Record.Desired.Adapter)
		if !registered || !adapter.Custom() {
			continue
		}
		hooks := backingService.Record.Desired.Hooks
		if hooks == nil || hooks.Attach == nil {
			continue
		}
		authentication, authErr := core.ResolveBackingAuthentication(
			adapter.SupportsAuthenticationModes(), backingService.Record.Desired.Authentication,
		)
		if backingProject.Record.Kind != hierarchyrecord.ProjectKindBacking || backingProject.Record.TenantID != "" ||
			backingEnvironment.Record.Name != "main" ||
			backingEnvironment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
			backingService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning ||
			backingService.Record.BackingNetworkID == "" || len(spec.Grants) != 0 || authErr != nil ||
			authentication != backingService.Record.Desired.Authentication {
			return nil, errs.New(
				errs.KindStateConflict,
				"Blueprint Custom Attach requires a ready backing Service without grants",
			)
		}
		attachID := attachIDByName[name]
		operationID := allocator.Named(ids.KindOperation, "attach-input:"+name)
		metadata, generated, resolved, err := service.attachFacts.SealCustomHookBundle(
			ctx, attachID, backingProject.Record.ID, operationID, *hooks,
		)
		if err != nil {
			return nil, err
		}
		hookInputs := make([]attachinputs.HookInput, len(hooks.Inputs))
		for index, input := range hooks.Inputs {
			source := attachinputs.HookInputResolved
			if input.Generate == backinghook.GeneratePassword {
				source = attachinputs.HookInputGenerated
			}
			hookInputs[index] = attachinputs.HookInput{Key: input.Key, Source: source}
		}
		sort.Slice(hookInputs, func(left, right int) bool { return hookInputs[left].Key < hookInputs[right].Key })
		facts := append([]backinghook.FactDefinition(nil), hooks.Facts...)
		sort.Slice(facts, func(left, right int) bool { return facts[left].Key < facts[right].Key })
		hookConfiguration := backinghook.CloneConfiguration(hooks)
		generation := attachinputs.Generation{
			ID: operationID, EnvironmentID: environmentID, RevisionID: revisionID,
			ParentTaskID: revisionID, OperationID: operationID,
			AttachID: attachID, AttachName: name, ConsumerServiceID: consumer.ID,
			CredentialOwnerID: attachID,
			BackingProjectID:  backingProject.Record.ID, BackingEnvironmentID: backingEnvironment.Record.ID,
			BackingServiceID: backingService.Record.Desired.ID,
			BackingNetworkID: backingService.Record.BackingNetworkID,
			AdapterKey:       backingService.Record.Desired.Adapter,
			Authentication:   backingService.Record.Desired.Authentication,
			Hook: attachinputs.HookPlan{
				Attach: hookConfiguration.Attach, Detach: hookConfiguration.Detach,
				Facts: facts, Inputs: hookInputs,
			},
			FactSets: metadata, GeneratedInputs: *generated, ResolvedInputs: resolved,
		}
		if resolved != nil {
			generation.SecretSources = slices.Clone(resolved.SecretSources)
		}
		if err := attachinputs.ValidateDraft(generation); err != nil {
			attachinputs.Clear(&generation)
			return nil, err
		}
		publications = append(publications, etcd.BlueprintAttachInputGenerationPublication{
			Generation: generation, BackingProject: backingProject,
			BackingEnvironment: backingEnvironment, BackingService: backingService,
		})
	}
	failed = false
	return publications, nil
}

func clearAuthoredAttachInputGenerations(publications []etcd.BlueprintAttachInputGenerationPublication) {
	for index := range publications {
		attachinputs.Clear(&publications[index].Generation)
	}
}
