package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalreferences"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// ValidateServiceRemovalReferences enforces Remove's no-cascade contract.
// Publication joins these reference guards to native work and deletion fencing.
func (repository *ServiceRepository) ValidateServiceRemovalReferences(ctx context.Context,
	current keyvalue.Versioned[services.ServiceRecord],
	projection keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection],
) error {
	_, err := repository.validateServiceRemovalReferences(ctx, current, projection)
	return err
}

func (repository *ServiceRepository) validateServiceRemovalReferences(ctx context.Context,
	current keyvalue.Versioned[services.ServiceRecord],
	projection keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection],
) (serviceremovalreferences.Guards, error) {
	return serviceremovalreferences.Validate(ctx, repository.store, current, projection)
}
