package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
	"sort"
	"time"
)

type resolvedBlueprintAttach struct {
	name               string
	spec               core.AttachmentSpec
	consumer           servicerecord.ServiceRecord
	backingProject     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	backingEnvironment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	backingService     etcdstore.Versioned[servicerecord.ServiceRecord]
	adapter            adapters.Adapter
	authentication     core.BackingAuthentication
}

type preparedBlueprintAttaches struct {
	candidates []blueprintplanning.EnvironmentBlueprintAttachCandidateInput
	effective  []etcdstore.Versioned[attachrecord.Record]
}

func (prepared *preparedBlueprintAttaches) clear() {
	if prepared == nil {
		return
	}
	for index := range prepared.candidates {
		if prepared.candidates[index].Facts != nil {
			clear(prepared.candidates[index].Facts.Ciphertext)
		}
	}
}

// Candidate preparation is shared by whole-Environment planning and resource
// units. It resolves and seals credentials without publishing or executing them.
func (service *Service) prepareBlueprintAttachCandidates(
	ctx context.Context, environmentID, taskID string, specs map[string]core.AttachmentSpec,
	serviceChanges []blueprints.EnvironmentBlueprintServiceChange,
	current []etcdstore.Versioned[attachrecord.Record], namedID func(ids.Kind, string) string, createdAt time.Time,
) (preparedBlueprintAttaches, error) {
	prepared := preparedBlueprintAttaches{
		effective: append([]etcdstore.Versioned[attachrecord.Record](nil), current...),
	}
	if len(specs) == 0 {
		return prepared, nil
	}
	serviceByName := make(map[string]servicerecord.ServiceRecord, len(serviceChanges))
	for _, change := range serviceChanges {
		serviceByName[change.Record.Desired.Name] = change.Record
	}
	currentByName := make(map[string]etcdstore.Versioned[attachrecord.Record], len(current))
	for _, attach := range current {
		currentByName[attach.Record.Name] = attach
	}
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	resolved := make(map[string]resolvedBlueprintAttach, len(names))
	for _, name := range names {
		if err := attachrecord.ValidateAttachName(name); err != nil {
			return preparedBlueprintAttaches{}, err
		}
		spec := specs[name]
		consumer, exists := serviceByName[spec.Service]
		if !exists {
			return preparedBlueprintAttaches{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach consumer Service is not authored by the Environment",
			)
		}
		backingProject, err := service.repository.ResolveBackingProject(ctx, spec.BackingProject)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		backingEnvironment, err := service.repository.ResolveEnvironment(ctx, backingProject.Record.ID, "main")
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		backingServices, err := service.listBlueprintServices(ctx, backingEnvironment.Record.ID)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		var backingService etcdstore.Versioned[servicerecord.ServiceRecord]
		for _, candidate := range backingServices {
			if candidate.Record.Desired.Name == spec.BackingService {
				backingService = candidate
				break
			}
		}
		if backingService.Record.Desired.ID == "" {
			return preparedBlueprintAttaches{}, errs.New(
				errs.KindServiceNotFound,
				"Blueprint Attach backing Service was not found",
			)
		}
		adapter, registered := adapters.Get(backingService.Record.Desired.Adapter)
		if backingProject.Record.Kind != hierarchyrecord.ProjectKindBacking ||
			backingEnvironment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
			backingService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning ||
			backingService.Record.BackingNetworkID == "" || !registered {
			return preparedBlueprintAttaches{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Attach requires a ready registered backing Service",
			)
		}
		authentication, authErr := core.ResolveBackingAuthentication(
			adapter.SupportsAuthenticationModes(), backingService.Record.Desired.Authentication,
		)
		if authErr != nil || authentication != backingService.Record.Desired.Authentication {
			return preparedBlueprintAttaches{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Attach backing Service authentication policy is invalid",
			)
		}
		if len(spec.Grants) != 0 && !adapter.SupportsGrants() {
			return preparedBlueprintAttaches{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach adapter does not support grants",
			)
		}
		resolved[name] = resolvedBlueprintAttach{
			name: name, spec: spec, consumer: consumer, backingProject: backingProject,
			backingEnvironment: backingEnvironment, backingService: backingService, adapter: adapter,
			authentication: authentication,
		}
	}
	if err := validateExistingBlueprintAttaches(names, resolved, currentByName); err != nil {
		return preparedBlueprintAttaches{}, err
	}
	for _, name := range names {
		item := resolved[name]
		hooks := item.backingService.Record.Desired.Hooks
		_, existing := currentByName[name]
		if !existing && item.spec.Credential.Mode == "new" && item.adapter.Custom() &&
			hooks != nil && (hooks.Attach != nil || hooks.Detach != nil) {
			return preparedBlueprintAttaches{}, errs.New(
				errs.KindValidationFailed,
				"create the custom Attach directly and wait until ready before referencing its facts or credential owner in Blueprint",
			)
		}
	}
	newCount := 0
	for _, name := range names {
		if _, exists := currentByName[name]; !exists {
			newCount++
		}
	}
	if newCount == 0 {
		return prepared, nil
	}

	attachIDs := make(map[string]string, len(names))
	for _, name := range names {
		if currentAttach, exists := currentByName[name]; exists {
			attachIDs[name] = currentAttach.Record.ID
		} else {
			attachIDs[name] = namedID(ids.KindAttach, "attach:"+name)
		}
	}
	identities := make(map[string]*attachplan.Identity)
	defer func() {
		for _, identity := range identities {
			identity.Clear()
		}
	}()
	for _, name := range names {
		item := resolved[name]
		if _, exists := currentByName[name]; exists || item.spec.Credential.Mode != "new" || item.adapter.Custom() {
			continue
		}
		identityName, err := attachments.ProvisionIdentity(
			attachIDs[name],
			item.consumer.Desired.Name,
			item.adapter.CredentialIdentityLimit(),
		)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		role := identityName
		var password []byte
		if item.authentication == core.BackingAuthenticationPassword {
			role = "default"
		}
		if item.authentication == core.BackingAuthenticationNone {
			role = ""
		} else {
			password, err = attachments.GeneratePassword(service.random)
			if err != nil {
				return preparedBlueprintAttaches{}, err
			}
		}
		identities[name] = &attachplan.Identity{
			Authentication: item.authentication,
			Database:       identityName, Role: role, Password: password,
		}
	}
	inputs := make([]blueprintplanning.EnvironmentBlueprintAttachCandidateInput, 0, newCount)
	records := make(map[string]attachrecord.Record, newCount)
	failed := true
	defer func() {
		if failed {
			for _, input := range inputs {
				if input.Facts != nil {
					clear(input.Facts.Ciphertext)
				}
			}
			prepared.clear()
			for _, identity := range identities {
				identity.Clear()
			}
		}
	}()
	for _, name := range names {
		item := resolved[name]
		if _, exists := currentByName[name]; exists || item.spec.Credential.Mode != "new" {
			continue
		}
		grantNames := append([]string(nil), item.spec.Grants...)
		sort.Slice(grantNames, func(left, right int) bool {
			return attachIDs[grantNames[left]] < attachIDs[grantNames[right]]
		})
		grantIDs := make([]string, 0, len(grantNames))
		grantFacts := make([]attachments.GrantInput, 0, len(grantNames))
		retainedGrants := make([]etcdstore.Versioned[attachrecord.Record], 0, len(grantNames))
		identity := identities[name]
		for _, grantName := range grantNames {
			grant, exists := resolved[grantName]
			grantIdentity := identities[grantName]
			retainedGrant, retained := currentByName[grantName]
			if !exists || grant.spec.Credential.Mode != "new" ||
				grant.backingService.Record.Desired.ID != item.backingService.Record.Desired.ID ||
				(!retained && grantIdentity == nil) {
				return preparedBlueprintAttaches{}, errs.New(
					errs.KindValidationFailed,
					"Blueprint Attach grant must name a credential owner on the same backing Service",
				)
			}
			grantID := attachIDs[grantName]
			database := ""
			if retained {
				if err := service.attachFacts.ResolveReadyDatabase(ctx, retainedGrant, func(value string) error {
					database = value
					return nil
				}); err != nil {
					return preparedBlueprintAttaches{}, err
				}
				retainedGrants = append(retainedGrants, retainedGrant)
			} else {
				database = grantIdentity.Database
			}
			grantIDs = append(grantIDs, grantID)
			grantFacts = append(grantFacts, attachments.GrantInput{
				AttachID: grantID,
				Params: adapters.Input{
					Authentication: item.authentication,
					Host: backingendpoint.New(
						item.backingService.Record.Desired.ID,
					), Port: item.adapter.Port(),
					Database: database, Role: identity.Role, Password: identity.Password,
				},
			})
			identity.Grants = append(identity.Grants, attachplan.GrantIdentity{
				AttachID: grantID, Database: database,
			})
		}
		var own adapters.Input
		if identity != nil {
			own = adapters.Input{
				Authentication: item.authentication,
				Host:           backingendpoint.New(item.backingService.Record.Desired.ID), Port: item.adapter.Port(),
				Database: identity.Database, Role: identity.Role, Password: identity.Password,
			}
		}
		metadata, encrypted, err := service.attachFacts.SealFactSets(
			ctx, attachIDs[name], item.adapter, own, grantFacts,
		)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		record, err := attachrecord.NewPendingAttachRecord(
			attachIDs[name], environmentID, name,
			item.backingProject.Record.ID, item.backingEnvironment.Record.ID,
			item.backingService.Record.Desired.ID, item.backingService.Record.BackingNetworkID,
			item.consumer.Desired.ID, attachIDs[name], grantIDs, metadata, taskID, createdAt,
		)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		records[name] = record
		inputs = append(inputs, blueprintplanning.EnvironmentBlueprintAttachCandidateInput{
			Record: record, Facts: encrypted, BackingProject: item.backingProject,
			BackingEnvironment: item.backingEnvironment, BackingService: item.backingService,
			RetainedGrantTargets: retainedGrants,
		})
	}
	for _, name := range names {
		item := resolved[name]
		if _, exists := currentByName[name]; exists || item.spec.Credential.Mode != "existing" {
			continue
		}
		owner, exists := records[item.spec.Credential.Attach]
		var retainedOwner *etcdstore.Versioned[attachrecord.Record]
		if !exists {
			currentOwner, retained := currentByName[item.spec.Credential.Attach]
			if retained {
				owner = currentOwner.Record
				retainedOwner = &currentOwner
				exists = true
			}
		}
		if !exists || !owner.OwnsCredential() || retainedOwner != nil && owner.Status != core.AttachReady ||
			owner.BackingServiceID != item.backingService.Record.Desired.ID {
			return preparedBlueprintAttaches{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach credential owner is unavailable",
			)
		}
		record, err := attachrecord.NewPendingAttachRecord(
			attachIDs[name],
			environmentID,
			name,
			item.backingProject.Record.ID,
			item.backingEnvironment.Record.ID,
			item.backingService.Record.Desired.ID,
			item.backingService.Record.BackingNetworkID,
			item.consumer.Desired.ID,
			owner.ID,
			nil,
			attachrecord.CloneAttachFactSets(owner.FactSets),
			taskID,
			createdAt,
		)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		records[name] = record
		inputs = append(inputs, blueprintplanning.EnvironmentBlueprintAttachCandidateInput{
			Record: record, BackingProject: item.backingProject,
			BackingEnvironment: item.backingEnvironment, BackingService: item.backingService,
			RetainedCredentialOwner: retainedOwner,
		})
	}
	prepared.candidates = inputs
	for _, name := range names {
		if record, created := records[name]; created {
			prepared.effective = append(prepared.effective, etcdstore.Versioned[attachrecord.Record]{Record: record})
		}
	}
	failed = false
	return prepared, nil
}

func validateExistingBlueprintAttaches(
	names []string,
	resolved map[string]resolvedBlueprintAttach,
	current map[string]etcdstore.Versioned[attachrecord.Record],
) error {
	for _, name := range names {
		item := resolved[name]
		versioned, exists := current[name]
		if !exists {
			continue
		}
		record := versioned.Record
		ownerID := record.ID
		if item.spec.Credential.Mode == "existing" {
			owner, exists := current[item.spec.Credential.Attach]
			if !exists {
				return errs.New(errs.KindStateConflict, "Blueprint Attach credential owner is missing")
			}
			ownerID = owner.Record.ID
		}
		grantIDs := make([]string, 0, len(item.spec.Grants))
		for _, grantName := range item.spec.Grants {
			grant, exists := current[grantName]
			if !exists {
				return errs.New(errs.KindStateConflict, "Blueprint Attach grant is missing")
			}
			grantIDs = append(grantIDs, grant.Record.ID)
		}
		sort.Strings(grantIDs)
		if record.Status != core.AttachReady || record.Operation != attachrecord.AttachOperationProvision ||
			record.BackingProjectID != item.backingProject.Record.ID ||
			record.BackingEnvironmentID != item.backingEnvironment.Record.ID ||
			record.BackingServiceID != item.backingService.Record.Desired.ID ||
			record.BackingNetworkID != item.backingService.Record.BackingNetworkID ||
			record.ServiceID != item.consumer.Desired.ID || record.CredentialAttachID != ownerID ||
			!slices.Equal(record.GrantAttachIDs, grantIDs) {
			return errs.New(
				errs.KindStateConflict,
				"Existing Blueprint Attach does not match the authored topology",
			)
		}
	}
	return nil
}
