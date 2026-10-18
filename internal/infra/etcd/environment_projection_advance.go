package etcd

import (
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

func validateEnvironmentComposeProjectionPublicationAdvance(
	previous projectionrecord.EnvironmentComposeProjection,
	hasPrevious bool,
	next projectionrecord.EnvironmentComposeProjection,
) error {
	if err := projectionrecord.ValidateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, "",
	); err != nil {
		return err
	}
	return projectionrecord.PreserveEnvironmentNonEntryDesiredResources(previous, hasPrevious, next)
}
