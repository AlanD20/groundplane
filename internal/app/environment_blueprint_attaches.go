package app

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type resolvedBlueprintAttach struct {
	name               string
	spec               core.AttachmentSpec
	consumer           etcd.ServiceRecord
	backingProject     etcd.Versioned[etcd.ProjectRecord]
	backingEnvironment etcd.Versioned[etcd.EnvironmentRecord]
	backingService     etcd.Versioned[etcd.ServiceRecord]
	adapter            adapters.Adapter
	authentication     core.BackingAuthentication
}

type blueprintAttachProcedure struct {
	record     etcd.AttachRecord
	adapterKey string
	identity   *controllerpkg.AttachPlanIdentity
}

type preparedBlueprintAttaches struct {
	publication etcd.BlueprintAttachTaskPreparation
	effective   []etcd.Versioned[etcd.AttachRecord]
	procedures  []blueprintAttachProcedure
	facts       *blueprintAttachFactOverlay
}

func (prepared *preparedBlueprintAttaches) clear() {
	if prepared == nil {
		return
	}
	for index := range prepared.procedures {
		prepared.procedures[index].identity.Clear()
	}
	if prepared.facts != nil {
		prepared.facts.clear()
	}
	etcd.ClearBlueprintAttachTaskPreparation(&prepared.publication)
}

func (service *environmentBlueprintService) prepareBlueprintAttaches(
	ctx context.Context,
	environmentID string,
	taskID string,
	specs map[string]core.AttachmentSpec,
	serviceChanges []etcd.EnvironmentBlueprintServiceChange,
	current []etcd.Versioned[etcd.AttachRecord],
	namedID func(ids.Kind, string) string,
	ownsEnvironmentFence bool,
	createdAt time.Time,
) (preparedBlueprintAttaches, error) {
	prepared := preparedBlueprintAttaches{
		effective: append([]etcd.Versioned[etcd.AttachRecord](nil), current...),
		facts:     newBlueprintAttachFactOverlay(service.attachFacts),
	}
	if len(specs) == 0 {
		return prepared, nil
	}
	serviceByName := make(map[string]etcd.ServiceRecord, len(serviceChanges))
	for _, change := range serviceChanges {
		serviceByName[change.Record.Desired.Name] = change.Record
	}
	currentByName := make(map[string]etcd.Versioned[etcd.AttachRecord], len(current))
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
		if err := etcd.ValidateAttachName(name); err != nil {
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
		var backingService etcd.Versioned[etcd.ServiceRecord]
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
		if backingProject.Record.Kind != etcd.ProjectKindBacking ||
			backingEnvironment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady ||
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
	identities := make(map[string]*controllerpkg.AttachPlanIdentity)
	for _, name := range names {
		item := resolved[name]
		if _, exists := currentByName[name]; exists || item.spec.Credential.Mode != "new" || item.adapter.Manual() {
			continue
		}
		identityName, err := attachProvisionIdentity(attachIDs[name], item.consumer.Desired.Name)
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
			password, err = generateAttachPassword(service.random)
			if err != nil {
				return preparedBlueprintAttaches{}, err
			}
		}
		identities[name] = &controllerpkg.AttachPlanIdentity{
			Authentication: item.authentication,
			Database:       identityName, Role: role, Password: password,
		}
	}
	inputs := make([]etcd.EnvironmentBlueprintAttachCandidateInput, 0, newCount)
	records := make(map[string]etcd.AttachRecord, newCount)
	failed := true
	defer func() {
		if failed {
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
		grantFacts := make([]AttachGrantFactParams, 0, len(grantNames))
		retainedGrants := make([]etcd.Versioned[etcd.AttachRecord], 0, len(grantNames))
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
			grantFacts = append(grantFacts, AttachGrantFactParams{
				AttachID: grantID,
				Params: adapters.FactParams{
					Authentication: item.authentication,
					Host: backingendpoint.New(
						item.backingService.Record.Desired.ID,
					), Port: item.adapter.Port(),
					Database: database, Role: identity.Role, Password: identity.Password,
				},
			})
			identity.Grants = append(identity.Grants, controllerpkg.AttachPlanGrantIdentity{
				AttachID: grantID, Database: database,
			})
		}
		var own adapters.FactParams
		if identity != nil {
			own = adapters.FactParams{
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
		record, err := etcd.NewPendingAttachRecord(
			attachIDs[name], environmentID, name,
			item.backingProject.Record.ID, item.backingEnvironment.Record.ID,
			item.backingService.Record.Desired.ID, item.backingService.Record.BackingNetworkID,
			item.consumer.Desired.ID, attachIDs[name], grantIDs, metadata, taskID, createdAt,
		)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		records[name] = record
		inputs = append(inputs, etcd.EnvironmentBlueprintAttachCandidateInput{
			Record: record, Facts: encrypted, BackingProject: item.backingProject,
			BackingEnvironment: item.backingEnvironment, BackingService: item.backingService,
			RetainedGrantTargets: retainedGrants,
		})
		if identity != nil {
			if err := prepared.facts.addOwner(name, own, grantNames, grantFacts, item.adapter); err != nil {
				return preparedBlueprintAttaches{}, err
			}
			if item.authentication != core.BackingAuthenticationNone {
				prepared.procedures = append(prepared.procedures, blueprintAttachProcedure{
					record: record, adapterKey: item.adapter.Key(), identity: identity,
				})
			} else {
				identity.Clear()
				delete(identities, name)
			}
			// Later owners may grant this database; keep its identity through preparation.
			// The returned procedures own cleanup, with the failure defer covering all identities.
		}
	}
	for _, name := range names {
		item := resolved[name]
		if _, exists := currentByName[name]; exists || item.spec.Credential.Mode != "existing" {
			continue
		}
		owner, exists := records[item.spec.Credential.Attach]
		var retainedOwner *etcd.Versioned[etcd.AttachRecord]
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
		record, err := etcd.NewPendingAttachRecord(
			attachIDs[name], environmentID, name,
			item.backingProject.Record.ID, item.backingEnvironment.Record.ID,
			item.backingService.Record.Desired.ID, item.backingService.Record.BackingNetworkID,
			item.consumer.Desired.ID, owner.ID, nil, cloneBlueprintAttachFactSets(owner.FactSets), taskID, createdAt,
		)
		if err != nil {
			return preparedBlueprintAttaches{}, err
		}
		records[name] = record
		inputs = append(inputs, etcd.EnvironmentBlueprintAttachCandidateInput{
			Record: record, BackingProject: item.backingProject,
			BackingEnvironment: item.backingEnvironment, BackingService: item.backingService,
			RetainedCredentialOwner: retainedOwner,
		})
		prepared.facts.addAlias(name, item.spec.Credential.Attach)
	}
	publication, err := etcd.PrepareEnvironmentBlueprintAttachTask(
		taskID, environmentID, inputs, ownsEnvironmentFence, createdAt,
	)
	if err != nil {
		return preparedBlueprintAttaches{}, err
	}
	for index := range inputs {
		if inputs[index].Facts != nil {
			clear(inputs[index].Facts.Ciphertext)
			inputs[index].Facts.Ciphertext = nil
		}
	}
	prepared.publication = publication
	for _, name := range names {
		if record, created := records[name]; created {
			prepared.effective = append(prepared.effective, etcd.Versioned[etcd.AttachRecord]{Record: record})
		}
	}
	sort.Slice(prepared.procedures, func(left, right int) bool {
		return prepared.procedures[left].record.ID < prepared.procedures[right].record.ID
	})
	failed = false
	return prepared, nil
}

func validateExistingBlueprintAttaches(
	names []string,
	resolved map[string]resolvedBlueprintAttach,
	current map[string]etcd.Versioned[etcd.AttachRecord],
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
		if record.Status != core.AttachReady || record.Operation != etcd.AttachOperationProvision ||
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

func (prepared preparedBlueprintAttaches) procedureSteps(
	taskID string,
	timeout int64,
	namedID func(ids.Kind, string) string,
) ([]*agentpb.ExecutionStep, []etcd.TaskStepRecord, error) {
	steps := make([]*agentpb.ExecutionStep, 0)
	records := make([]etcd.TaskStepRecord, 0)
	for _, procedure := range prepared.procedures {
		stepRecords := make([]etcd.TaskStepRecord, 0, len(procedure.record.GrantAttachIDs)+1)
		stepRecords = append(
			stepRecords,
			etcd.TaskStepRecord{
				Kind: etcd.TaskStepOperation,
				ID:   namedID(ids.KindStep, "attach:"+procedure.record.ID+":provision"),
			},
		)
		for _, grantID := range procedure.record.GrantAttachIDs {
			stepRecords = append(
				stepRecords,
				etcd.TaskStepRecord{
					Kind: etcd.TaskStepOperation,
					ID:   namedID(ids.KindStep, "attach:"+procedure.record.ID+":grant:"+grantID),
				},
			)
		}
		procedureTask := etcd.TaskRecord{
			ID: taskID, Type: etcd.TaskAttach, Target: procedure.record.ID,
			Steps: stepRecords, TimeoutSeconds: timeout,
		}
		built, err := controllerpkg.BuildAttachProvisionSteps(
			procedureTask, procedure.record, procedure.adapterKey, *procedure.identity,
		)
		if err != nil {
			clearBlueprintAttachProcedureSteps(steps)
			return nil, nil, err
		}
		steps = append(steps, built...)
		records = append(records, stepRecords...)
	}
	return steps, records, nil
}

type blueprintAttachFactValue struct {
	secret bool
	value  []byte
}

type blueprintAttachFactOverlay struct {
	fallback entrygeneration.EntryFactResolver
	aliases  map[string]string
	sets     map[string]map[string]map[string]blueprintAttachFactValue
}

func newBlueprintAttachFactOverlay(fallback entrygeneration.EntryFactResolver) *blueprintAttachFactOverlay {
	return &blueprintAttachFactOverlay{
		fallback: fallback, aliases: map[string]string{},
		sets: map[string]map[string]map[string]blueprintAttachFactValue{},
	}
}

func (overlay *blueprintAttachFactOverlay) addOwner(
	name string,
	own adapters.FactParams,
	grantNames []string,
	grants []AttachGrantFactParams,
	adapter adapters.Adapter,
) error {
	overlay.aliases[name] = name
	overlay.sets[name] = map[string]map[string]blueprintAttachFactValue{}
	if err := overlay.addSet(name, "", adapter, own); err != nil {
		return err
	}
	for index, grant := range grants {
		if err := overlay.addSet(name, grantNames[index], adapter, grant.Params); err != nil {
			return err
		}
	}
	return nil
}

func (overlay *blueprintAttachFactOverlay) addAlias(name string, owner string) {
	overlay.aliases[name] = owner
}

func (overlay *blueprintAttachFactOverlay) addSet(
	owner string,
	grant string,
	adapter adapters.Adapter,
	params adapters.FactParams,
) error {
	facts, err := adapters.BuildFacts(adapter, params)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	defer adapters.ClearFacts(facts)
	values := make(map[string]blueprintAttachFactValue, len(facts))
	for _, fact := range facts {
		values[fact.Key] = blueprintAttachFactValue{secret: fact.Secret, value: append([]byte(nil), fact.Value...)}
	}
	overlay.sets[owner][grant] = values
	return nil
}

func (overlay *blueprintAttachFactOverlay) ResolveFact(
	ctx context.Context,
	environmentID string,
	reference core.FactRef,
	destinationSecret bool,
	consume secretvalue.PlaintextConsumer,
) error {
	owner, candidate := overlay.aliases[reference.Attach]
	if !candidate {
		return overlay.fallback.ResolveFact(ctx, environmentID, reference, destinationSecret, consume)
	}
	sets, candidateOwner := overlay.sets[owner]
	if !candidateOwner {
		reference.Attach = owner
		return overlay.fallback.ResolveFact(ctx, environmentID, reference, destinationSecret, consume)
	}
	set := sets[reference.Grant]
	fact, found := set[reference.Key]
	if !found {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach fact is not declared")
	}
	if fact.secret && !destinationSecret {
		return errs.New(errs.KindValidationFailed, "Secret Blueprint Attach fact requires a secret Entry destination")
	}
	value := append([]byte(nil), fact.value...)
	defer clear(value)
	return consume(value)
}

func (overlay *blueprintAttachFactOverlay) clear() {
	for _, sets := range overlay.sets {
		for _, facts := range sets {
			for key, fact := range facts {
				clear(fact.value)
				delete(facts, key)
			}
		}
	}
}

func cloneBlueprintAttachFactSets(values []etcd.AttachFactSetMetadata) []etcd.AttachFactSetMetadata {
	cloned := make([]etcd.AttachFactSetMetadata, len(values))
	for index, value := range values {
		cloned[index] = etcd.AttachFactSetMetadata{
			GrantAttachID: value.GrantAttachID,
			Facts:         append([]etcd.AttachFactDefinition(nil), value.Facts...),
		}
	}
	return cloned
}

func clearBlueprintAttachProcedureSteps(steps []*agentpb.ExecutionStep) {
	for _, step := range steps {
		if procedure := step.GetAdapterProcedure(); procedure != nil {
			clear(procedure.Password)
			procedure.Password = nil
		}
	}
}
