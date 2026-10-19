package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateServiceLifecycleHierarchy(tenant *keyvalue.Versioned[hierarchy.TenantRecord],
	project keyvalue.Versioned[hierarchy.ProjectRecord], environment keyvalue.Versioned[hierarchy.EnvironmentRecord],
	service keyvalue.Versioned[services.ServiceRecord],
) error {
	if project.Revision <= 0 || environment.Revision <= 0 || service.Revision <= 0 ||
		project.ReadRevision < project.Revision || environment.ReadRevision < environment.Revision || service.ReadRevision < service.Revision ||
		services.ValidateServiceRecord(
			service.Record,
		) != nil || environment.Record.ProjectID != project.Record.ID || service.Record.EnvironmentID != environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "Service lifecycle hierarchy is invalid")
	}
	switch project.Record.Kind {
	case hierarchy.ProjectKindTenant:
		if tenant == nil || tenant.Revision <= 0 || tenant.ReadRevision < tenant.Revision ||
			project.Record.TenantID != tenant.Record.ID {
			return errs.New(errs.KindValidationFailed, "Service lifecycle Tenant hierarchy is invalid")
		}
	case hierarchy.ProjectKindBacking:
		if tenant != nil || project.Record.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "Service lifecycle backing hierarchy is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Service lifecycle Project kind is invalid")
	}
	return nil
}
