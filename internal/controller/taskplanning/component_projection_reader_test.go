package taskplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Captured Component file sources have no Task execution-plan dependency.
type componentProjectionReader struct {
	tenant      hierarchy.TenantRecord
	project     hierarchy.ProjectRecord
	environment hierarchy.EnvironmentRecord
	revision    blueprints.EnvironmentBlueprintRevision
	projection  environmentprojection.EnvironmentComposeProjection
}

func (reader *componentProjectionReader) GetTenant(
	context.Context,
	string,
) (keyvalue.Versioned[hierarchy.TenantRecord], error) {
	return keyvalue.Versioned[hierarchy.TenantRecord]{Record: reader.tenant}, nil
}

func (reader *componentProjectionReader) GetProject(
	context.Context,
	string,
) (keyvalue.Versioned[hierarchy.ProjectRecord], error) {
	return keyvalue.Versioned[hierarchy.ProjectRecord]{Record: reader.project}, nil
}

func (reader *componentProjectionReader) GetEnvironment(
	context.Context,
	string,
) (keyvalue.Versioned[hierarchy.EnvironmentRecord], error) {
	return keyvalue.Versioned[hierarchy.EnvironmentRecord]{Record: reader.environment}, nil
}

func (reader *componentProjectionReader) GetEnvironmentBlueprintRevision(
	context.Context,
	string,
	string,
) (keyvalue.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error) {
	return keyvalue.Versioned[blueprints.EnvironmentBlueprintRevision]{Record: reader.revision}, true, nil
}

func (reader *componentProjectionReader) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection], bool, error) {
	return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{Record: reader.projection}, true, nil
}

func (reader *componentProjectionReader) GetEnvironmentComposeProjectionRevision(
	context.Context,
	string,
	string,
) (keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection], bool, error) {
	return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{Record: reader.projection}, true, nil
}
