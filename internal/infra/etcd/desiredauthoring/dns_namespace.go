package desiredauthoring

import (
	"context"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Labels are captured at the same revision as the desired head and protected by
// its hierarchy fence. Namespace scope changes naming, never target ownership.
func (snapshot metadataSnapshot) dnsNamespace(
	ctx context.Context,
	store Store,
	requested *model.NamespaceScope,
) (model.Namespace, error) {
	projectRead, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchy.ProjectKey(snapshot.environment.ProjectID)}, Revision: snapshot.readRevision,
	})
	if err != nil {
		return model.Namespace{}, err
	}
	if projectRead == nil || projectRead.ReadRevision != snapshot.readRevision || len(projectRead.Values) != 1 ||
		projectRead.Values[0] == nil {
		return model.Namespace{}, errs.New(errs.KindStateConflict, "DNS owner Project is unavailable")
	}
	defer keyvalue.ClearValues(projectRead.Values)
	project, err := hierarchy.DecodeProject(projectRead.Values[0].Value)
	if err != nil {
		return model.Namespace{}, err
	}
	if project.ID != snapshot.environment.ProjectID {
		return model.Namespace{}, errs.New(errs.KindInternal, "DNS owner Project identity is invalid")
	}
	if project.Kind != hierarchy.ProjectKindTenant {
		return model.Namespace{}, errs.New(
			errs.KindValidationFailed,
			"tenantless Backing Environments cannot own scoped DNS records; use platform DNS settings",
		)
	}
	tenantRead, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchy.TenantKey(project.TenantID)}, Revision: snapshot.readRevision,
	})
	if err != nil {
		return model.Namespace{}, err
	}
	if tenantRead == nil || tenantRead.ReadRevision != snapshot.readRevision || len(tenantRead.Values) != 1 ||
		tenantRead.Values[0] == nil {
		return model.Namespace{}, errs.New(errs.KindStateConflict, "DNS owner Tenant is unavailable")
	}
	defer keyvalue.ClearValues(tenantRead.Values)
	tenant, err := hierarchy.DecodeTenant(tenantRead.Values[0].Value)
	if err != nil {
		return model.Namespace{}, err
	}
	if tenant.ID != project.TenantID {
		return model.Namespace{}, errs.New(errs.KindInternal, "DNS owner Tenant identity is invalid")
	}
	scope := model.NamespaceEnvironment
	if requested != nil {
		scope = *requested
	}
	return model.CaptureNamespace(scope, tenant.Slug, project.Slug, snapshot.environment.Name)
}
