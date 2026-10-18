package taskplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// TaskPlanResolver resolves exact procedures from their capability-owned sealed
// inputs. Rendered procedures use pinned inputs.
type TaskPlanResolver struct {
	volumeRoot         string
	blueprints         blueprintPlanStateReader
	componentCatalog   []componentrender.EnvironmentComponentRegistration
	serviceProxyImages map[serviceproxy.Implementation]*releasedomain.ProxyImage
}

type blueprintPlanStateReader interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
}
