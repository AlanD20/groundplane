package blueprint

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type environmentBlueprintSnapshot struct {
	tenant      etcdstore.Versioned[hierarchyrecord.TenantRecord]
	project     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	head        etcdstore.Versioned[blueprints.EnvironmentBlueprintHead]
	desired     etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]
	identities  etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]
	volumes     []projectionrecord.EnvironmentVolumeIdentity
	hasHead     bool
}

func (repository *durableRepository) GetService(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (service *Service) GetBlueprint(
	ctx context.Context,
	environmentID string,
) (apiTypes.EnvironmentBlueprintDocument, error) {
	snapshot, err := service.loadEnvironmentBlueprintSnapshot(ctx, environmentID)
	if err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, err
	}
	attaches, _, err := service.listBlueprintAttaches(ctx, environmentID)
	if err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, err
	}
	authoring, err := service.environmentBlueprintAuthoringDocument(ctx, snapshot, attaches)
	if err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, err
	}
	document, err := blueprintparser.MarshalAuthoringDocument(authoring)
	if err != nil {
		return apiTypes.EnvironmentBlueprintDocument{}, err
	}
	return apiTypes.EnvironmentBlueprintDocument{
		EnvironmentID: environmentID,
		Revision:      environmentBlueprintRevision(snapshot.head, snapshot.hasHead),
		Document:      string(document),
	}, nil
}

func (service *Service) ValidateBlueprint(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	expectedRevision string,
) (apiTypes.EnvironmentBlueprintValidation, error) {
	if ctx == nil {
		return apiTypes.EnvironmentBlueprintValidation{}, errs.New(
			errs.KindInternal,
			"Environment Blueprint context is required",
		)
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || bundle.Validate() != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, errs.New(
			errs.KindValidationFailed,
			"Environment Blueprint validation input is invalid",
		)
	}
	snapshot, err := service.loadEnvironmentBlueprintSnapshot(ctx, environmentID)
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	revision := environmentBlueprintRevision(snapshot.head, snapshot.hasHead)
	if expectedRevision != revision {
		return apiTypes.EnvironmentBlueprintValidation{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint changed after the authoring revision was loaded",
		)
	}
	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID,
		Tenant:        snapshot.tenant.Record.Slug,
		Project:       snapshot.project.Record.Slug,
		Environment:   snapshot.environment.Record.Name,
	}, bundle)
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	if err := taskplanning.ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	if snapshot.hasHead {
		if err := requireExplicitBlueprintVolumes(parsed.Project, snapshot.volumes); err != nil {
			return apiTypes.EnvironmentBlueprintValidation{}, err
		}
	}
	services, err := service.listBlueprintServices(ctx, environmentID)
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	attaches, readRevision, err := service.listBlueprintAttaches(ctx, environmentID)
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	if err := service.rejectUnsupportedBlueprintOmissions(ctx, environmentID, parsed, services, attaches); err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	if parsed.Extensions.Backup != nil {
		validationVolumes, validationAttaches, err := environmentBlueprintBackupValidationTargets(
			snapshot, parsed, attaches,
		)
		if err != nil {
			return apiTypes.EnvironmentBlueprintValidation{}, err
		}
		if err := service.validateEnvironmentBlueprintBackup(
			ctx, environmentID, readRevision, parsed.Extensions.Backup,
			validationVolumes, validationAttaches,
		); err != nil {
			return apiTypes.EnvironmentBlueprintValidation{}, err
		}
	}
	current, err := service.environmentBlueprintAuthoringDocument(ctx, snapshot, attaches)
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	if err := validateCurrentAttachmentDeclarations(parsed.Extensions.Attachments, current.Attachments, attaches); err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	var currentProject *composetypes.Project
	if snapshot.hasHead {
		currentProject, err = composerender.LoadNormalizedEnvironmentDesiredProject(
			ctx, snapshot.desired.Record, snapshot.identities.Record,
		)
		if err != nil {
			return apiTypes.EnvironmentBlueprintValidation{}, err
		}
	}
	return apiTypes.EnvironmentBlueprintValidation{
		Revision: revision,
		Changes: environmentBlueprintChanges(
			current,
			parsed,
			snapshot.hasHead,
			currentProject,
			attaches,
		),
	}, nil
}

func (service *Service) loadEnvironmentBlueprintSnapshot(
	ctx context.Context,
	environmentID string,
) (environmentBlueprintSnapshot, error) {
	if ctx == nil {
		return environmentBlueprintSnapshot{}, errs.New(errs.KindInternal, "Environment Blueprint context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return environmentBlueprintSnapshot{}, errs.New(errs.KindValidationFailed, "Environment id is invalid")
	}
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return environmentBlueprintSnapshot{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return environmentBlueprintSnapshot{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return environmentBlueprintSnapshot{}, err
	}
	if environment.Record.ProjectID != project.Record.ID || project.Record.TenantID != tenant.Record.ID ||
		project.Record.Kind != hierarchyrecord.ProjectKindTenant {
		return environmentBlueprintSnapshot{}, errs.New(errs.KindInternal, "Environment hierarchy is inconsistent")
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil {
		return environmentBlueprintSnapshot{}, err
	}
	desired, hasDesired, err := service.repository.GetEnvironmentDesiredInput(ctx, environmentID)
	if err != nil {
		return environmentBlueprintSnapshot{}, err
	}
	identities, hasIdentities, err := service.repository.GetEnvironmentOwnedIdentities(ctx, environmentID)
	if err != nil {
		return environmentBlueprintSnapshot{}, err
	}
	if hasHead != hasDesired || hasHead != hasIdentities || hasHead &&
		(head.Record.EnvironmentID != environmentID || head.Revision <= 0 ||
			desired.Revision != head.Revision || identities.Revision != head.Revision ||
			desired.Record.EnvironmentID != environmentID || identities.Record.EnvironmentID != environmentID ||
			desired.Record.RevisionID != head.Record.RevisionID ||
			identities.Record.RevisionID != head.Record.RevisionID ||
			desired.Record.RenderGeneration != identities.Record.RenderGeneration) {
		return environmentBlueprintSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint desired snapshot changed",
		)
	}
	volumes := []projectionrecord.EnvironmentVolumeIdentity(nil)
	if hasHead {
		if _, err := authoredOwnedIdentitySnapshot(ctx, desired.Record, identities.Record); err != nil {
			return environmentBlueprintSnapshot{}, err
		}
		volumes = make([]projectionrecord.EnvironmentVolumeIdentity, len(identities.Record.Volumes))
		for index, identity := range identities.Record.Volumes {
			volumes[index] = projectionrecord.EnvironmentVolumeIdentity{
				ID: identity.ID, Key: identity.Name, Slug: identity.Slug,
			}
		}
	}
	return environmentBlueprintSnapshot{
		tenant: tenant, project: project, environment: environment,
		head: head, desired: desired, identities: identities, volumes: volumes, hasHead: hasHead,
	}, nil
}

func environmentBlueprintRevision(
	head etcdstore.Versioned[blueprints.EnvironmentBlueprintHead],
	found bool,
) string {
	if !found {
		return apiTypes.EnvironmentBlueprintInitialRevision
	}
	return head.Record.RevisionID
}

func (service *Service) environmentBlueprintAuthoringDocument(
	ctx context.Context,
	snapshot environmentBlueprintSnapshot,
	attaches []etcdstore.Versioned[attachrecord.Record],
) (blueprintparser.AuthoringDocument, error) {
	input := blueprintparser.AuthoringDocument{
		Envelope: core.Envelope{
			Kind:   core.KindDocEnvironment,
			Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{
				Tenant:      snapshot.tenant.Record.Slug,
				Project:     snapshot.project.Record.Slug,
				Environment: snapshot.environment.Record.Name,
			},
		},
		NetworkPool: snapshot.environment.Record.NetworkPool,
		Compose:     []byte("services: {}\n"),
	}
	if snapshot.hasHead {
		desired := core.CloneBlueprintDesiredInput(snapshot.desired.Record.Input)
		compose, err := composerender.AuthoringComposeVolumes(
			desired.NormalizedCompose, snapshot.volumes, snapshot.environment.Record.VolumeDir,
		)
		if err != nil {
			return blueprintparser.AuthoringDocument{}, err
		}
		input.NetworkPool = desired.NetworkPool
		input.Compose = compose
		input.Requires = desired.Requires
		input.Attachments = desired.Attachments
		input.Entries = desired.Entries
		input.Routes = desired.Routes
		input.Scripts = desired.Scripts
		input.Components = desired.Components
		input.Backup = desired.Backup
		input.ReleaseGroups = desired.ReleaseGroups
	}
	current, err := service.authoringAttachmentSpecs(ctx, snapshot.environment.Record.ID, attaches)
	if err != nil {
		return blueprintparser.AuthoringDocument{}, err
	}
	input.Attachments = current
	return input, nil
}

func environmentBlueprintChanges(
	current blueprintparser.AuthoringDocument,
	candidate blueprintparser.Result,
	hasCurrent bool,
	currentProject *composetypes.Project,
	currentAttaches []etcdstore.Versioned[attachrecord.Record],
) []apiTypes.EnvironmentBlueprintChange {
	currentKeys := make(map[string]map[string]struct{})
	candidateKeys := make(map[string]map[string]struct{})
	if hasCurrent {
		addBlueprintResourceKey(currentKeys, "compose", "root")
		if currentProject != nil {
			for key := range currentProject.Services {
				addBlueprintResourceKey(currentKeys, "service", key)
			}
			for key := range currentProject.DisabledServices {
				addBlueprintResourceKey(currentKeys, "service", key)
			}
			for key := range currentProject.Networks {
				addBlueprintResourceKey(currentKeys, "zone", key)
			}
			for key := range currentProject.Volumes {
				addBlueprintResourceKey(currentKeys, "volume", key)
			}
			for key := range currentProject.Configs {
				addBlueprintResourceKey(currentKeys, "config", key)
			}
			for key := range currentProject.Secrets {
				addBlueprintResourceKey(currentKeys, "secret", key)
			}
		}
	}
	addBlueprintResourceKey(candidateKeys, "compose", "root")
	if current.Backup != nil {
		addBlueprintResourceKey(currentKeys, "backup", "policy")
	}
	if candidate.Extensions.Backup != nil {
		addBlueprintResourceKey(candidateKeys, "backup", "policy")
	}
	for key := range current.Attachments {
		addBlueprintResourceKey(currentKeys, "attach", key)
	}
	for key := range candidate.Extensions.Attachments {
		addBlueprintResourceKey(candidateKeys, "attach", key)
	}
	for key := range current.Entries {
		addBlueprintResourceKey(currentKeys, "entry", key)
	}
	for key := range candidate.Extensions.Entries {
		addBlueprintResourceKey(candidateKeys, "entry", key)
	}
	for key := range current.Scripts {
		addBlueprintResourceKey(currentKeys, "script", key)
	}
	for key := range candidate.Extensions.Scripts {
		addBlueprintResourceKey(candidateKeys, "script", key)
	}
	for key := range current.Components {
		addBlueprintResourceKey(currentKeys, "component", key)
	}
	for key := range candidate.Extensions.Components {
		addBlueprintResourceKey(candidateKeys, "component", key)
	}
	for key := range current.ReleaseGroups {
		addBlueprintResourceKey(currentKeys, "release-group", key)
	}
	for key := range candidate.Extensions.ReleaseGroups {
		addBlueprintResourceKey(candidateKeys, "release-group", key)
	}
	for _, route := range current.Routes {
		addBlueprintResourceKey(currentKeys, "route", route.Hostname+route.Path)
	}
	for _, route := range candidate.Extensions.Routes {
		addBlueprintResourceKey(candidateKeys, "route", route.Hostname+route.Path)
	}
	for key := range candidate.Project.Services {
		addBlueprintResourceKey(candidateKeys, "service", key)
	}
	for key := range candidate.Project.DisabledServices {
		addBlueprintResourceKey(candidateKeys, "service", key)
	}
	for key := range candidate.Project.Networks {
		addBlueprintResourceKey(candidateKeys, "zone", key)
	}
	for key := range candidate.Project.Volumes {
		addBlueprintResourceKey(candidateKeys, "volume", key)
	}
	for key := range candidate.Project.Configs {
		addBlueprintResourceKey(candidateKeys, "config", key)
	}
	for key := range candidate.Project.Secrets {
		addBlueprintResourceKey(candidateKeys, "secret", key)
	}

	changes := make([]apiTypes.EnvironmentBlueprintChange, 0)
	readyAttaches := make(map[string]struct{}, len(currentAttaches))
	for _, attach := range currentAttaches {
		if attach.Record.Status == core.AttachReady {
			readyAttaches[attach.Record.Name] = struct{}{}
		}
	}
	for resource, keys := range candidateKeys {
		for key := range keys {
			action := apiTypes.BlueprintChangeCreate
			if _, exists := currentKeys[resource][key]; exists {
				action = apiTypes.BlueprintChangeUpdate
				if resource == "attach" {
					if _, ready := readyAttaches[key]; ready &&
						sameAttachmentSpec(current.Attachments[key], candidate.Extensions.Attachments[key]) {
						action = apiTypes.BlueprintChangeRetain
					}
				}
			}
			change := apiTypes.EnvironmentBlueprintChange{
				Resource: resource, Key: key, Action: action,
			}
			if resource == "entry" && action == apiTypes.BlueprintChangeCreate {
				spec := candidate.Extensions.Entries[key]
				change.EmptySecretValue = spec.Secret && spec.Source.SecretRef == "" && spec.Source.Fact == nil
			}
			changes = append(changes, change)
		}
	}
	for resource, keys := range currentKeys {
		for key := range keys {
			if _, included := candidateKeys[resource][key]; included {
				continue
			}
			action := apiTypes.BlueprintChangeRetain
			if resource == "entry" || resource == "script" || resource == "backup" ||
				resource == "config" || resource == "secret" ||
				resource == "component" || resource == "release-group" {
				action = apiTypes.BlueprintChangeRemove
			}
			changes = append(changes, apiTypes.EnvironmentBlueprintChange{
				Resource: resource, Key: key, Action: action,
			})
		}
	}
	sort.Slice(changes, func(left, right int) bool {
		if changes[left].Resource != changes[right].Resource {
			return changes[left].Resource < changes[right].Resource
		}
		if changes[left].Key != changes[right].Key {
			return changes[left].Key < changes[right].Key
		}
		return changes[left].Action < changes[right].Action
	})
	return changes
}

func addBlueprintResourceKey(values map[string]map[string]struct{}, resource, key string) {
	if values[resource] == nil {
		values[resource] = make(map[string]struct{})
	}
	values[resource][key] = struct{}{}
}
