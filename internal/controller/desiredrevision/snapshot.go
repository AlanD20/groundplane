package desiredrevision

import (
	"github.com/AlanD20/groundplane/internal/core"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
)

func NextGeneration(
	environmentID string,
	head etcdstore.Versioned[blueprints.EnvironmentBlueprintHead],
	hasHead bool,
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	hasProjection bool,
) (int64, uint64, error) {
	if hasHead != hasProjection {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are inconsistent")
	}
	if !hasHead {
		return 0, 1, nil
	}
	if head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
		head.Record.RevisionID != projection.Record.RevisionID || head.Revision <= 0 ||
		projection.Revision != head.Revision || projection.Record.RenderGeneration == math.MaxUint64 {
		return 0, 0, errs.New(errs.KindInternal, "Environment desired-state pointers are corrupt")
	}
	return head.Revision, projection.Record.RenderGeneration + 1, nil
}

func CloneProjection(
	current projectionrecord.EnvironmentComposeProjection,
) projectionrecord.EnvironmentComposeProjection {
	return projectionrecord.CloneEnvironmentComposeProjection(current)
}

func CloneServiceExtensions(source map[string]core.ServiceExtensionSpec) map[string]core.ServiceExtensionSpec {
	result := make(map[string]core.ServiceExtensionSpec, len(source)+1)
	for name, extension := range source {
		result[name] = core.CloneServiceExtension(extension)
	}
	return result
}
