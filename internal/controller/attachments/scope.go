package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func (service *MutationService) resolveAttachScope(
	ctx context.Context,
	consumer etcdstore.Versioned[servicerecord.ServiceRecord],
	backingServiceID string,
	credentialAttachID string,
	grantIDs []string,
) (etcd.AttachCreateScope, []etcdstore.Versioned[attachrecord.Record], adapters.Adapter, error) {
	environment, err := service.repository.GetEnvironment(ctx, consumer.Record.EnvironmentID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	backingService, err := service.repository.GetService(ctx, backingServiceID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	backingEnvironment, err := service.repository.GetEnvironment(ctx, backingService.Record.EnvironmentID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	backingProject, err := service.repository.GetProject(ctx, backingEnvironment.Record.ProjectID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		backingEnvironment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		project.Record.Kind != hierarchyrecord.ProjectKindTenant || backingProject.Record.Kind != hierarchyrecord.ProjectKindBacking ||
		consumer.Record.EnvironmentID != environment.Record.ID ||
		consumer.Record.Runtime.RuntimeIntent == core.ServiceRuntimeIntentAbsent ||
		backingService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach requires ready consumer/backing Environments and a runnable backing Service",
		)
	}
	adapter, registered := adapters.Get(backingService.Record.Desired.Adapter)
	if !registered || adapter.Key() != backingService.Record.Desired.Adapter {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindValidationFailed,
			"Attach backing Service adapter is not registered",
		)
	}
	authentication, authErr := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), backingService.Record.Desired.Authentication,
	)
	if authErr != nil || authentication != backingService.Record.Desired.Authentication {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach backing Service authentication policy is invalid",
		)
	}
	if len(grantIDs) != 0 && !adapter.SupportsGrants() {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindValidationFailed,
			"Attach backing Service adapter does not support grants",
		)
	}
	head, exists, err := service.repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if !exists {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach requires an applied Environment Blueprint",
		)
	}
	projection, exists, err := service.repository.GetEnvironmentComposeProjectionRevision(
		ctx, environment.Record.ID, head.Record.RevisionID,
	)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if !exists {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach requires the selected Environment Compose projection",
		)
	}
	if head.Record.EnvironmentID != environment.Record.ID ||
		projection.Record.EnvironmentID != environment.Record.ID ||
		projection.Record.RevisionID != head.Record.RevisionID {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach selected Environment Compose projection is inconsistent",
		)
	}
	grants := make([]etcdstore.Versioned[attachrecord.Record], 0, len(grantIDs))
	for _, grantID := range grantIDs {
		grant, grantErr := service.repository.GetAttach(ctx, grantID)
		if grantErr != nil {
			return etcd.AttachCreateScope{}, nil, nil, grantErr
		}
		grants = append(grants, grant)
	}
	slices.SortFunc(grants, func(left, right etcdstore.Versioned[attachrecord.Record]) int {
		if left.Record.ID < right.Record.ID {
			return -1
		}
		if left.Record.ID > right.Record.ID {
			return 1
		}
		return 0
	})
	var credentialOwner *etcdstore.Versioned[attachrecord.Record]
	if credentialAttachID != "" {
		owner, ownerErr := service.repository.GetAttach(ctx, credentialAttachID)
		if ownerErr != nil {
			return etcd.AttachCreateScope{}, nil, nil, ownerErr
		}
		if owner.Record.Status != core.AttachReady || !owner.Record.OwnsCredential() ||
			owner.Record.EnvironmentID != environment.Record.ID ||
			owner.Record.BackingServiceID != backingService.Record.Desired.ID ||
			owner.Record.BackingNetworkID != backingService.Record.BackingNetworkID {
			return etcd.AttachCreateScope{}, nil, nil, errs.New(
				errs.KindScopeUnauthorized,
				"Existing credential must be a ready direct owner in the same Environment and Backing Service",
			)
		}
		credentialOwner = &owner
	}
	attaches, err := service.listAllAttaches(ctx, environment.Record.ID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	scope := etcd.AttachCreateScope{
		Tenant: tenant, Project: project, Environment: environment,
		DesiredHead: head, ComposeProjection: projection,
		Services:       []etcdstore.Versioned[servicerecord.ServiceRecord]{consumer},
		BackingProject: backingProject, BackingEnvironment: backingEnvironment,
		BackingService: backingService, CredentialOwner: credentialOwner, Grants: grants,
	}
	return scope, attaches, adapter, nil
}

func (service *MutationService) listAllAttaches(
	ctx context.Context,
	environmentID string,
) ([]etcdstore.Versioned[attachrecord.Record], error) {
	var records []etcdstore.Versioned[attachrecord.Record]
	cursor := ""
	revision := int64(0)
	for {
		page, err := service.repository.ListAttaches(ctx, environmentID, etcdstore.PageRequest{
			Limit: etcdstore.MaximumPageLimit, Cursor: cursor,
		})
		if err != nil {
			return nil, err
		}
		if page.Revision <= 0 || revision != 0 && page.Revision != revision {
			return nil, errs.New(errs.KindInternal, "Attach topology pages changed revision")
		}
		revision = page.Revision
		records = append(records, page.Items...)
		if page.NextCursor == "" {
			return records, nil
		}
		cursor = page.NextCursor
	}
}
