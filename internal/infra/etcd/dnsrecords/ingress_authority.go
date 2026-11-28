package dnsrecords

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/publicingress"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RouteNameCondition fences the complete hostname inventory read by ingress
// admission against new or changed declarations at a later revision.
func (reader *Reader) RouteNameCondition(ctx context.Context, revision int64) (keyvalue.Condition, error) {
	return reader.routeNameCondition(ctx, revision)
}

func (reader *Reader) protectIngressAuthoredRoutes(
	ctx context.Context, input environmentprojection.EnvironmentDesiredInput, revision int64,
) ([]keyvalue.Condition, error) {
	previous, _, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, input.EnvironmentID, revision)
	if err != nil {
		return nil, err
	}
	before, after := authoredRoutesByHostname(
		previous.Record.Input.Routes,
	), authoredRoutesByHostname(
		input.Input.Routes,
	)
	policy := false
	for _, component := range input.Input.Components {
		policy = policy || component.Enabled && ingressKind(component.Implementation)
	}
	return protectIngress(ctx, reader, input.EnvironmentID, revision, policy, before, after)
}

func (reader *Reader) protectIngressProjectedRoutes(
	ctx context.Context, projection environmentprojection.EnvironmentComposeProjection, revision int64,
) ([]keyvalue.Condition, error) {
	previous, _, err := environmentqueries.ReadDeclaredResources(ctx, reader.store, projection.EnvironmentID, revision)
	if err != nil {
		return nil, err
	}
	before, after := projectedRoutesByHostname(
		previous.Record.DesiredRoutes,
	), projectedRoutesByHostname(
		projection.DesiredRoutes,
	)
	policy := false
	for _, component := range projection.Components {
		policy = policy || component.Desired.Enabled && ingressKind(component.Desired.Kind)
	}
	return protectIngress(ctx, reader, projection.EnvironmentID, revision, policy, before, after)
}

func protectIngress[T comparable](
	ctx context.Context,
	reader *Reader,
	environmentID string,
	revision int64,
	enablesPolicy bool,
	before, after map[string][]T,
) ([]keyvalue.Condition, error) {
	names := make([]string, 0, len(before)+len(after))
	for name := range before {
		names = append(names, name)
	}
	for name := range after {
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil, nil
	}
	slices.Sort(names)
	owners, condition, err := publicingress.ReadHostnameOwners(ctx, reader.store, slices.Compact(names), revision)
	if err != nil {
		return nil, err
	}
	for name, owner := range owners {
		if owner != environmentID || enablesPolicy || !slices.Equal(before[name], after[name]) {
			return nil, errs.Newf(
				errs.KindResourceInUse,
				"withdraw public ingress for hostname %s before changing its Routes or Environment ingress policy",
				name,
			)
		}
	}
	return []keyvalue.Condition{condition}, nil
}

func ingressKind(kind core.ComponentKind) bool {
	return kind == core.ComponentKindEdgeCloudflare || kind == core.ComponentKindIngressCaddy ||
		kind == core.ComponentKindIngressEnvoy
}

func authoredRoutesByHostname(routes []core.RouteSpec) map[string][]core.RouteSpec {
	result := make(map[string][]core.RouteSpec)
	for _, route := range routes {
		if route.Hostname != "" {
			result[route.Hostname] = append(result[route.Hostname], route)
		}
	}
	for _, matching := range result {
		slices.SortFunc(matching, func(a, b core.RouteSpec) int { return strings.Compare(a.Path, b.Path) })
	}
	return result
}

func projectedRoutesByHostname(routes []environmentprojection.EnvironmentRouteProjection) map[string][]core.Route {
	result := make(map[string][]core.Route)
	for _, projection := range routes {
		route := projection.Desired
		if route.Host != "" {
			result[route.Host] = append(result[route.Host], route)
		}
	}
	for _, matching := range result {
		slices.SortFunc(matching, func(a, b core.Route) int { return strings.Compare(a.ID, b.ID) })
	}
	return result
}
