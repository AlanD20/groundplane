package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateAttachCreateScope(
	ctx context.Context,
	scope AttachCreateScope,
	record attachrecord.Record,
	facts *attachrecord.EncryptedFacts,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := attachrecord.ValidateAttachRecord(record); err != nil {
		return err
	}
	if scope.Tenant.Revision <= 0 || scope.Project.Revision <= 0 || scope.Environment.Revision <= 0 ||
		scope.DesiredHead.Revision <= 0 || scope.ComposeProjection.Revision <= 0 ||
		scope.BackingProject.Revision <= 0 ||
		scope.BackingEnvironment.Revision <= 0 || scope.BackingService.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach scope records must be versioned")
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID ||
		scope.Project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		scope.Project.Record.TenantID == "" ||
		scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID {
		return errs.New(errs.KindScopeUnauthorized, "Attach consumer hierarchy is invalid")
	}
	if scope.BackingProject.Record.Kind != hierarchyrecord.ProjectKindBacking ||
		scope.BackingProject.Record.TenantID != "" ||
		scope.BackingEnvironment.Record.ProjectID != scope.BackingProject.Record.ID ||
		scope.BackingService.Record.EnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingProjectID != scope.BackingProject.Record.ID ||
		record.BackingEnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingServiceID != scope.BackingService.Record.Desired.ID ||
		record.BackingNetworkID != scope.BackingService.Record.BackingNetworkID {
		return errs.New(errs.KindScopeUnauthorized, "Attach backing hierarchy is invalid")
	}
	serviceIDs := make([]string, 0, len(scope.Services))
	for _, service := range scope.Services {
		if service.Revision <= 0 || service.Record.EnvironmentID != record.EnvironmentID {
			return errs.New(errs.KindScopeUnauthorized, "Attach consuming Service is outside the Environment")
		}
		serviceIDs = append(serviceIDs, service.Record.Desired.ID)
	}
	slices.Sort(serviceIDs)
	if len(serviceIDs) != 1 || serviceIDs[0] != record.ServiceID {
		return errs.New(errs.KindValidationFailed, "Attach service_id does not match the resolved Service")
	}
	grantIDs := make([]string, 0, len(scope.Grants))
	for _, grant := range scope.Grants {
		if grant.Revision <= 0 || grant.Record.EnvironmentID != record.EnvironmentID ||
			grant.Record.BackingServiceID != record.BackingServiceID ||
			grant.Record.BackingNetworkID != record.BackingNetworkID || grant.Record.Status != core.AttachReady {
			return errs.New(
				errs.KindScopeUnauthorized,
				"Attach grant must be ready in the same Environment and backing Service",
			)
		}
		grantIDs = append(grantIDs, grant.Record.ID)
	}
	slices.Sort(grantIDs)
	if !slices.Equal(grantIDs, record.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "Attach grant ids do not match the resolved grant records")
	}
	if record.OwnsCredential() {
		if scope.CredentialOwner != nil {
			return errs.New(errs.KindValidationFailed, "Credential-owning Attach cannot reference another owner")
		}
	} else {
		owner := scope.CredentialOwner
		if owner == nil || owner.Revision <= 0 || owner.Record.ID != record.CredentialAttachID ||
			!owner.Record.OwnsCredential() || owner.Record.Status != core.AttachReady ||
			owner.Record.EnvironmentID != record.EnvironmentID ||
			owner.Record.BackingServiceID != record.BackingServiceID ||
			owner.Record.BackingNetworkID != record.BackingNetworkID || facts != nil ||
			len(record.GrantAttachIDs) != 0 || !attachrecord.AttachFactSetsEqual(record.FactSets, owner.Record.FactSets) {
			return errs.New(errs.KindScopeUnauthorized, "Attach existing credential owner is invalid")
		}
	}
	custom := scope.BackingService.Record.Desired.Adapter == "custom"
	if custom {
		hooks := scope.BackingService.Record.Desired.Hooks
		hooked := record.OwnsCredential() && hooks != nil && (hooks.Attach != nil || hooks.Detach != nil)
		if record.HookBundle != hooked || (facts != nil) != hooked || len(record.GrantAttachIDs) != 0 {
			return errs.New(
				errs.KindAdapterCustomOnly,
				"Custom Attach hook bundle does not match its backing Service",
			)
		}
		if hooked && facts.AttachID != record.ID {
			return errs.New(errs.KindValidationFailed, "Custom Attach hook bundle belongs to another Attach")
		}
		if !hooked && len(record.FactSets) != 0 {
			return errs.New(errs.KindAdapterCustomOnly, "Custom Attach without hooks cannot publish facts")
		}
		if hooked {
			return attachrecord.ValidateAttachEncryptedFacts(*facts)
		}
		return nil
	}
	if !record.OwnsCredential() {
		return nil
	}
	if scope.BackingService.Record.Desired.Adapter == "" {
		return errs.New(errs.KindValidationFailed, "Attach backing Service requires an adapter")
	}
	if facts == nil || len(record.FactSets) == 0 {
		return errs.New(errs.KindValidationFailed, "Adapter Attach requires encrypted facts and fact metadata")
	}
	if facts.AttachID != record.ID {
		return errs.New(errs.KindValidationFailed, "Attach fact envelope belongs to another Attach")
	}
	return attachrecord.ValidateAttachEncryptedFacts(*facts)
}
