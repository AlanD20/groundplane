package blueprint

import (
	"context"

	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Backing Projects have no Tenant. Admission and runtime sealing use the same
// owner resolution rather than requesting an empty Tenant id from storage.
func (service *Service) authoredOwnerTenant(
	ctx context.Context, project hierarchyrecord.ProjectRecord,
) (keyvalue.Versioned[hierarchyrecord.TenantRecord], error) {
	switch project.Kind {
	case hierarchyrecord.ProjectKindTenant:
		tenant, err := service.repository.GetTenant(ctx, project.TenantID)
		if err != nil {
			return keyvalue.Versioned[hierarchyrecord.TenantRecord]{}, err
		}
		if tenant.Record.ID != project.TenantID {
			return keyvalue.Versioned[hierarchyrecord.TenantRecord]{}, errs.New(errs.KindStateConflict, "Blueprint Project's Tenant changed")
		}
		return tenant, nil
	case hierarchyrecord.ProjectKindBacking:
		if project.TenantID == "" {
			return keyvalue.Versioned[hierarchyrecord.TenantRecord]{}, nil
		}
	}
	return keyvalue.Versioned[hierarchyrecord.TenantRecord]{}, errs.New(errs.KindStateConflict, "Blueprint Project ownership is invalid")
}
