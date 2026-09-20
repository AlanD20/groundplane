package blueprint

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/scriptdefinition"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentBlueprintSnapshot struct {
	tenant      etcd.Versioned[hierarchyrecord.TenantRecord]
	project     etcd.Versioned[hierarchyrecord.ProjectRecord]
	environment etcd.Versioned[hierarchyrecord.EnvironmentRecord]
	head        etcd.Versioned[etcd.EnvironmentBlueprintHead]
	projection  etcd.Versioned[etcd.EnvironmentComposeProjection]
	hasHead     bool
}

func (repository *durableRepository) GetService(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
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
	authoring, err := service.environmentBlueprintAuthoringDocument(ctx, snapshot)
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
	if err := controller.ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	if parsed.Extensions.Backup != nil {
		attaches, readRevision, err := service.listBlueprintAttaches(ctx, environmentID)
		if err != nil {
			return apiTypes.EnvironmentBlueprintValidation{}, err
		}
		validationProjection, validationAttaches, err := environmentBlueprintBackupValidationTargets(
			snapshot, parsed, attaches,
		)
		if err != nil {
			return apiTypes.EnvironmentBlueprintValidation{}, err
		}
		if err := service.validateEnvironmentBlueprintBackup(
			ctx, environmentID, readRevision, parsed.Extensions.Backup,
			validationProjection, validationAttaches,
		); err != nil {
			return apiTypes.EnvironmentBlueprintValidation{}, err
		}
	}
	current, err := service.environmentBlueprintAuthoringDocument(ctx, snapshot)
	if err != nil {
		return apiTypes.EnvironmentBlueprintValidation{}, err
	}
	return apiTypes.EnvironmentBlueprintValidation{
		Revision: revision,
		Changes:  environmentBlueprintChanges(current, parsed, snapshot.hasHead, snapshot.projection.Record),
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
	projection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return environmentBlueprintSnapshot{}, err
	}
	if hasHead != hasProjection || hasHead &&
		(head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
			head.Record.RevisionID != projection.Record.RevisionID) {
		return environmentBlueprintSnapshot{}, errs.New(
			errs.KindInternal,
			"Environment Blueprint desired head is inconsistent",
		)
	}
	return environmentBlueprintSnapshot{
		tenant: tenant, project: project, environment: environment,
		head: head, projection: projection, hasHead: hasHead,
	}, nil
}

func environmentBlueprintRevision(
	head etcd.Versioned[etcd.EnvironmentBlueprintHead],
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
	serviceNames := make(map[string]string)
	if snapshot.hasHead {
		input.Compose = append([]byte(nil), snapshot.projection.Record.NormalizedCompose...)
		input.Requires = environmentBlueprintAuthoringRequirements(snapshot.projection.Record)
		for _, projected := range snapshot.projection.Record.DesiredServices {
			if projected.EnvironmentID != snapshot.environment.Record.ID || projected.Desired.Name == "" {
				return blueprintparser.AuthoringDocument{}, errs.New(
					errs.KindInternal,
					"Environment Blueprint Service projection is invalid",
				)
			}
			serviceNames[projected.Desired.ID] = projected.Desired.Name
		}
		var err error
		input.Routes, err = environmentBlueprintAuthoringRoutes(snapshot.projection.Record.DesiredRoutes, serviceNames)
		if err != nil {
			return blueprintparser.AuthoringDocument{}, err
		}
		input.Entries, err = entry.BlueprintAuthoring(snapshot.projection.Record.Entries)
		if err != nil {
			return blueprintparser.AuthoringDocument{}, err
		}
		input.Components, err = environmentBlueprintAuthoringComponents(snapshot.projection.Record.Components)
		if err != nil {
			return blueprintparser.AuthoringDocument{}, err
		}
	}
	scripts, _, err := service.listBlueprintScripts(ctx, snapshot.environment.Record.ID, service.repository)
	if err != nil {
		return blueprintparser.AuthoringDocument{}, err
	}
	input.Scripts, err = scriptdefinition.Authoring(
		scripts,
		snapshot.projection.Record.Volumes,
		snapshot.projection.Record.Entries,
	)
	if err != nil {
		return blueprintparser.AuthoringDocument{}, err
	}
	attaches, _, err := service.listBlueprintAttaches(ctx, snapshot.environment.Record.ID)
	if err != nil {
		return blueprintparser.AuthoringDocument{}, err
	}
	input.Attachments, err = service.environmentBlueprintAuthoringAttachments(ctx, attaches, serviceNames)
	if err != nil {
		return blueprintparser.AuthoringDocument{}, err
	}
	input.Backup, err = service.environmentBlueprintAuthoringBackup(ctx, snapshot, attaches)
	if err != nil {
		return blueprintparser.AuthoringDocument{}, err
	}
	input.ReleaseGroups, err = service.releaseGroups.AuthoringSpecs(
		ctx,
		snapshot.environment.Record.ID,
		serviceNames,
	)
	if err != nil {
		return blueprintparser.AuthoringDocument{}, err
	}
	return input, nil
}

func environmentBlueprintAuthoringRequirements(
	projection etcd.EnvironmentComposeProjection,
) []core.Requirement {
	return projection.BlueprintRequirements.Clone().Authored
}

func environmentBlueprintAuthoringRoutes(
	routes []etcd.EnvironmentRouteProjection,
	serviceNames map[string]string,
) ([]core.RouteSpec, error) {
	result := make([]core.RouteSpec, len(routes))
	for index, projected := range routes {
		name, found := serviceNames[projected.Desired.TargetServiceID]
		if !found {
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Route target is missing")
		}
		result[index] = core.RouteSpec{
			Hostname:   projected.Desired.Host,
			Path:       projected.Desired.Path,
			Target:     name,
			TargetPort: projected.Desired.TargetPort,
			Exposure:   projected.Desired.Exposure,
		}
	}
	return result, nil
}

func environmentBlueprintAuthoringComponents(
	records []etcd.ComponentRecord,
) (map[string]core.ComponentSpec, error) {
	result := make(map[string]core.ComponentSpec)
	for _, record := range records {
		var capability core.ComponentCapability
		spec := core.ComponentSpec{Implementation: record.Desired.Kind, Enabled: record.Desired.Enabled}
		switch record.Desired.Kind {
		case core.ComponentKindIngressCaddy:
			capability = core.ComponentCapabilityHTTPRouter
			if record.Desired.Config.Caddy != nil {
				spec.Settings.ZoneIDs = append([]string(nil), record.Desired.Config.Caddy.ZoneIDs...)
				spec.Settings.Alias = record.Desired.Config.Caddy.Alias
				spec.ImplementationConfig.CaddyfileTemplate =
					record.Desired.Config.Caddy.CaddyfileTemplate
			}
		case core.ComponentKindEdgeCloudflare:
			capability = core.ComponentCapabilityEdgeTunnel
			if record.Desired.Config.CloudflareTunnel != nil {
				spec.Settings.ZoneIDs = append([]string(nil), record.Desired.Config.CloudflareTunnel.ZoneIDs...)
				spec.Settings.SecretID = record.Desired.Config.CloudflareTunnel.SecretID
			}
		default:
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Component implementation is invalid")
		}
		if _, duplicate := result[string(capability)]; duplicate {
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Component capability is duplicated")
		}
		result[string(capability)] = spec
	}
	return result, nil
}

func (service *Service) environmentBlueprintAuthoringAttachments(
	ctx context.Context,
	records []etcd.Versioned[etcd.AttachRecord],
	serviceNames map[string]string,
) (map[string]core.AttachmentSpec, error) {
	names := make(map[string]string, len(records))
	for _, versioned := range records {
		names[versioned.Record.ID] = versioned.Record.Name
	}
	result := make(map[string]core.AttachmentSpec)
	for _, versioned := range records {
		record := versioned.Record
		if record.Status == core.AttachDetached {
			continue
		}
		serviceName, found := serviceNames[record.ServiceID]
		if !found {
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Attach Service is missing")
		}
		backingProject, err := service.repository.GetProject(ctx, record.BackingProjectID)
		if err != nil {
			return nil, err
		}
		backingService, err := service.repository.GetService(ctx, record.BackingServiceID)
		if err != nil {
			return nil, err
		}
		credential := core.AttachmentCredentialSpec{Mode: "new"}
		grants := make([]string, 0, len(record.GrantAttachIDs))
		if !record.OwnsCredential() {
			owner, found := names[record.CredentialAttachID]
			if !found {
				return nil, errs.New(errs.KindInternal, "Environment Blueprint Attach credential owner is missing")
			}
			credential = core.AttachmentCredentialSpec{Mode: "existing", Attach: owner}
		} else {
			for _, id := range record.GrantAttachIDs {
				name, found := names[id]
				if !found {
					return nil, errs.New(errs.KindInternal, "Environment Blueprint Attach grant is missing")
				}
				grants = append(grants, name)
			}
			sort.Strings(grants)
		}
		if _, duplicate := result[record.Name]; duplicate {
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Attach name is duplicated")
		}
		result[record.Name] = core.AttachmentSpec{
			BackingProject: backingProject.Record.Slug,
			BackingService: backingService.Record.Desired.Name,
			Service:        serviceName,
			Credential:     credential,
			Grants:         grants,
		}
	}
	return result, nil
}

func environmentBlueprintChanges(
	current blueprintparser.AuthoringDocument,
	candidate blueprintparser.Result,
	hasCurrent bool,
	projection etcd.EnvironmentComposeProjection,
) []apiTypes.EnvironmentBlueprintChange {
	currentKeys := make(map[string]map[string]struct{})
	candidateKeys := make(map[string]map[string]struct{})
	if hasCurrent {
		addBlueprintResourceKey(currentKeys, "compose", "root")
		for _, service := range projection.DesiredServices {
			addBlueprintResourceKey(currentKeys, "service", service.Desired.Name)
		}
		for _, zone := range projection.DesiredZones {
			addBlueprintResourceKey(currentKeys, "zone", zone.Desired.Name)
		}
		for _, volume := range projection.Volumes {
			addBlueprintResourceKey(currentKeys, "volume", volume.Key)
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

	changes := make([]apiTypes.EnvironmentBlueprintChange, 0)
	for resource, keys := range candidateKeys {
		for key := range keys {
			action := apiTypes.BlueprintChangeCreate
			if _, exists := currentKeys[resource][key]; exists {
				action = apiTypes.BlueprintChangeUpdate
			}
			changes = append(changes, apiTypes.EnvironmentBlueprintChange{
				Resource: resource, Key: key, Action: action,
			})
		}
	}
	for resource, keys := range currentKeys {
		for key := range keys {
			if _, included := candidateKeys[resource][key]; included {
				continue
			}
			changes = append(changes, apiTypes.EnvironmentBlueprintChange{
				Resource: resource, Key: key, Action: apiTypes.BlueprintChangeRetain,
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

func environmentBlueprintBackupValidationTargets(
	snapshot environmentBlueprintSnapshot,
	parsed blueprintparser.Result,
	currentAttaches []etcd.Versioned[etcd.AttachRecord],
) (etcd.EnvironmentComposeProjection, []etcd.Versioned[etcd.AttachRecord], error) {
	projection := snapshot.projection.Record
	projection.EnvironmentID = snapshot.environment.Record.ID
	volumeSlugs, err := environmentBlueprintVolumeSlugs(parsed.Project, projection, snapshot.hasHead)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, nil, err
	}
	byKey := make(map[string]etcd.EnvironmentVolumeIdentity, len(projection.Volumes))
	for _, volume := range projection.Volumes {
		byKey[volume.Key] = volume
	}
	at := snapshot.environment.Record.CreatedAt
	if at.IsZero() {
		at = time.Unix(0, 0).UTC()
	}
	for key, label := range volumeSlugs {
		volume, found := byKey[key]
		if !found {
			volume = etcd.EnvironmentVolumeIdentity{
				ID:  ids.DeriveAt(ids.KindVolume, at, snapshot.environment.Record.ID, "validate-volume/"+key),
				Key: key,
			}
		}
		volume.Slug = label
		byKey[key] = volume
	}
	projection.Volumes = projection.Volumes[:0]
	for _, volume := range byKey {
		projection.Volumes = append(projection.Volumes, volume)
	}
	attaches := append([]etcd.Versioned[etcd.AttachRecord](nil), currentAttaches...)
	attachIDs := make(map[string]string, len(attaches)+len(parsed.Extensions.Attachments))
	for _, attach := range attaches {
		attachIDs[attach.Record.Name] = attach.Record.ID
	}
	for name := range parsed.Extensions.Attachments {
		if attachIDs[name] == "" {
			attachIDs[name] = ids.DeriveAt(ids.KindAttach, at, snapshot.environment.Record.ID, "validate-attach/"+name)
		}
	}
	for name, spec := range parsed.Extensions.Attachments {
		found := false
		for _, attach := range currentAttaches {
			found = found || attach.Record.Name == name
		}
		if found {
			continue
		}
		credentialID := attachIDs[spec.Credential.Attach]
		if spec.Credential.Mode == "new" {
			credentialID = attachIDs[name]
		}
		attaches = append(attaches, etcd.Versioned[etcd.AttachRecord]{Record: etcd.AttachRecord{
			ID: attachIDs[name], EnvironmentID: snapshot.environment.Record.ID,
			Name: name, CredentialAttachID: credentialID,
		}})
	}
	return projection, attaches, nil
}
