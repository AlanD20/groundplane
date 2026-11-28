// Package publicingressauthority joins current local ingress authority at one
// storage revision. It never accepts operator-supplied upstream addresses.
package publicingressauthority

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/controller/publicingress"
	"github.com/AlanD20/groundplane/internal/core"
	edge "github.com/AlanD20/groundplane/internal/core/publicingress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maxEnvironments = 256
	// Each Route contributes several ownership comparisons. Keep the review
	// inside the shared etcd transaction budget even when no Routes share a
	// Service, Environment, or hostname.
	maxPublicationRoutes = 6
	maxEnvironmentPages  = 64
)

type Releases interface {
	ResolveServing(context.Context, string, string, int64) (releasequeries.ServingRelease, error)
	GetReleaseRenderInputAt(
		context.Context,
		string,
		int64,
	) (keyvalue.Versioned[releaserender.ReleaseRenderInput], error)
	GetAppliedProjectionAt(
		context.Context,
		string,
		int64,
	) (keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection], bool, error)
	LoadAcknowledgedServiceRuntimesAtRevision(
		context.Context,
		string,
		[]string,
		int64,
	) ([]keyvalue.Versioned[serviceruntimerecord.Record], error)
}

// NetworkEvidence verifies exact listener ownership, isolated edge reachability
// and current runtime readiness with the local host. It chooses only from the
// captured release proxy address candidates, never unrelated bridge devices.
type NetworkEvidence interface {
	Verify(context.Context, NetworkRequest) (map[string]ingressexecution.NetworkEndpoint, error)
}

type NetworkRequest = ingressexecution.NetworkRequest
type NetworkRoute = ingressexecution.NetworkRoute

type Authority struct {
	store    keyvalue.Store
	releases Releases
	network  NetworkEvidence
}

var _ publicingress.Authority = (*Authority)(nil)

func New(store keyvalue.Store, releases Releases, network NetworkEvidence) *Authority {
	return &Authority{store: store, releases: releases, network: network}
}

func (authority *Authority) Capture(
	ctx context.Context,
	hostID string,
	desired publicingress.Desired,
) (publicingress.Capture, error) {
	if authority.network == nil || authority.releases == nil {
		return publicingress.Capture{}, conflict(
			"public ingress requires verified host listener and origin connectivity",
		)
	}
	if len(desired.RouteIDs) > maxPublicationRoutes {
		return publicingress.Capture{}, errs.Newf(
			errs.KindValidationFailed, "select at most %d public Routes per Host", maxPublicationRoutes,
		)
	}
	binding, err := authority.resolveLocalHost(ctx, hostID)
	if err != nil {
		return publicingress.Capture{}, err
	}
	revision := binding.ReadRevision
	routeNames, err := dnsrecords.NewReader(authority.store).RouteNameCondition(ctx, revision)
	if err != nil {
		return publicingress.Capture{}, err
	}
	executor, executorGuard, err := authority.currentExecutor(ctx, binding.Binding, revision)
	if err != nil {
		return publicingress.Capture{}, err
	}
	capture := publicingress.Capture{
		Input:  edge.Input{HostID: hostID, Revision: revision, Addresses: slices.Clone(desired.Addresses)},
		Guards: append(slices.Clone(binding.Conditions), executorGuard, routeNames),
	}
	request := NetworkRequest{
		HostID:    hostID,
		AgentID:   binding.Binding.AgentID,
		Addresses: slices.Clone(desired.Addresses),
	}
	if len(desired.RouteIDs) == 0 {
		_, err := authority.network.Verify(ctx, request)
		return capture, err
	}
	environments, err := authority.scanEnvironments(ctx, revision)
	if err != nil {
		return publicingress.Capture{}, err
	}
	byHost := make(map[string]string)
	byRoute := make(map[string]environmentprojection.EnvironmentRouteProjection)
	wanted := make(map[string]bool, len(desired.RouteIDs))
	for _, routeID := range desired.RouteIDs {
		if ids.Validate(ids.KindRoute, routeID) != nil || wanted[routeID] {
			return publicingress.Capture{}, errs.New(
				errs.KindValidationFailed,
				"public ingress Route IDs must be valid and unique",
			)
		}
		wanted[routeID] = true
	}
	for _, environment := range environments {
		for _, route := range environment.Record.DesiredRoutes {
			host := route.Desired.Host
			if owner, exists := byHost[host]; exists && owner != route.EnvironmentID {
				byHost[host] = ""
			} else {
				byHost[host] = route.EnvironmentID
			}
			if wanted[route.Desired.ID] {
				if _, exists := byRoute[route.Desired.ID]; exists {
					return publicingress.Capture{}, conflict("public ingress Route identity has multiple owners")
				}
				byRoute[route.Desired.ID] = route
			}
		}
	}
	allowed := make(map[string]bool)
	for _, routeID := range desired.RouteIDs {
		route, exists := byRoute[routeID]
		if !exists || route.Desired.Exposure != "public" || route.Desired.Host == "" {
			return publicingress.Capture{}, conflict(
				"the selected Route must be declared public with an explicit hostname",
			)
		}
		if byHost[route.Desired.Host] != route.EnvironmentID {
			return publicingress.Capture{}, conflict("the public hostname is already declared in another Environment")
		}
		environment := environments[route.EnvironmentID]
		if !allowed[route.EnvironmentID] {
			conditions, err := authority.environmentConditions(ctx, environment, revision)
			if err != nil {
				return publicingress.Capture{}, err
			}
			capture.Guards = append(capture.Guards, conditions...)
			for _, declared := range environment.Record.DesiredRoutes {
				if declared.Desired.Exposure == "internal" {
					capture.Input.InternalRoutes = append(capture.Input.InternalRoutes, edge.InternalRoute{
						Source: declared.Desired, EnvironmentID: route.EnvironmentID,
					})
				}
			}
			allowed[route.EnvironmentID] = true
		}
		condition, err := authority.routeObservationCondition(ctx, route, revision)
		if err != nil {
			return publicingress.Capture{}, err
		}
		endpoint, networkRoute, guards, err := authority.proxy(
			ctx, executor, route.EnvironmentID, route.Desired.TargetServiceID, route.Desired.ID,
			route.Desired.TargetPort, revision,
		)
		if err != nil {
			return publicingress.Capture{}, err
		}
		capture.Guards = append(capture.Guards, condition)
		capture.Guards = append(capture.Guards, guards...)
		capture.Input.Routes = append(capture.Input.Routes, edge.Route{
			Source: route.Desired, EnvironmentID: route.EnvironmentID, Endpoint: endpoint,
		})
		request.Routes = append(request.Routes, networkRoute)
	}
	selected, err := authority.network.Verify(ctx, request)
	if err != nil {
		return publicingress.Capture{}, err
	}
	if len(selected) != len(request.Routes) {
		return publicingress.Capture{}, conflict("the Host did not verify every public Route origin")
	}
	for i, route := range request.Routes {
		upstream := selected[route.RouteID]
		approved := false
		for _, candidate := range route.Addresses {
			approved = approved || upstream.Address == candidate
		}
		if !approved || !upstream.Relay.IsValid() || upstream.Proof == "" {
			return publicingress.Capture{}, conflict(
				"the Host did not verify an authorized relay for the captured stable Service proxy",
			)
		}
		endpoint := &capture.Input.Routes[i].Endpoint
		endpoint.Address, endpoint.Relay, endpoint.RelayProof = upstream.Address, upstream.Relay, upstream.Proof
	}
	return capture, nil
}

func (authority *Authority) resolveLocalHost(ctx context.Context, hostID string) (hostbindings.Snapshot, error) {
	if authority.store == nil || ids.Validate(ids.KindHost, hostID) != nil {
		return hostbindings.Snapshot{}, errs.New(errs.KindValidationFailed, "public ingress requires a valid Host")
	}
	binding, err := hostbindings.ReadLocal(ctx, authority.store, 0)
	if err != nil {
		return hostbindings.Snapshot{}, err
	}
	if binding.Binding.HostID != hostID {
		return hostbindings.Snapshot{}, conflict("public ingress management is available on the enrolled local Host")
	}
	return binding, nil
}

func (authority *Authority) scanEnvironments(
	ctx context.Context,
	revision int64,
) (map[string]keyvalue.Versioned[environmentqueries.DeclaredResources], error) {
	result := make(map[string]keyvalue.Versioned[environmentqueries.DeclaredResources])
	start := ""
	for pageIndex := 0; pageIndex < maxEnvironmentPages; pageIndex++ {
		const prefix = environmentqueries.EnvironmentDesiredHeadScanPrefix
		page, err := authority.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: prefix, StartExclusive: start, Revision: revision, Limit: keyvalue.MaximumPageLimit,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return nil, incomplete("public ingress Environment inventory is incomplete")
		}
		for index := range page.Values {
			value := &page.Values[index]
			if value.Key <= start || !strings.HasPrefix(value.Key, prefix) {
				return nil, incomplete("public ingress Environment inventory did not advance")
			}
			start = value.Key
			id, isCurrent := strings.CutSuffix(strings.TrimPrefix(value.Key, prefix), "/current")
			if !isCurrent {
				continue
			}
			if ids.Validate(ids.KindEnvironment, id) != nil {
				return nil, incomplete("public ingress Environment identity is invalid")
			}
			if len(result) == maxEnvironments {
				return nil, conflict("public ingress Environment inventory exceeds its review limit")
			}
			declared, found, err := environmentqueries.ReadDeclaredResources(ctx, authority.store, id, revision)
			if err != nil {
				return nil, err
			}
			if !found || declared.Record.EnvironmentID != id || declared.ReadRevision != revision ||
				declared.Revision != value.ModRevision {
				return nil, incomplete("public ingress declared Routes do not match the Environment head")
			}
			result[id] = declared
		}
		if !page.More {
			return result, nil
		}
	}
	return nil, conflict("public ingress Environment inventory exceeds its review limit")
}

func (authority *Authority) environmentConditions(
	ctx context.Context,
	declared keyvalue.Versioned[environmentqueries.DeclaredResources],
	revision int64,
) ([]keyvalue.Condition, error) {
	id := declared.Record.EnvironmentID
	desired, found, err := blueprints.ReadCurrentDesiredInput(ctx, authority.store, id, revision)
	if err != nil {
		return nil, err
	}
	if !found || desired.Revision != declared.Revision {
		return nil, incomplete("public ingress Environment policy is unavailable")
	}
	for _, component := range desired.Record.Input.Components {
		if component.Enabled && isIngressKind(component.Implementation) {
			return nil, conflict("an enabled Environment router or Tunnel requires separate public listener authority")
		}
	}
	applied, found, err := authority.releases.GetAppliedProjectionAt(ctx, id, revision)
	if err != nil {
		return nil, err
	}
	if found {
		for _, component := range applied.Record.Components {
			if component.Desired.Enabled && isIngressKind(component.Desired.Kind) {
				return nil, conflict(
					"an applied Environment router or Tunnel requires separate public listener authority",
				)
			}
		}
		for _, runtime := range applied.Record.ManagedComponentRuntimeSources {
			if isIngressKind(runtime.ComponentKind) {
				return nil, conflict("an Environment router or Tunnel retirement has not settled")
			}
		}
	}
	return []keyvalue.Condition{
		{Key: blueprints.EnvironmentBlueprintHeadKey(id), ModRevision: declared.Revision},
		{Key: environmentprojection.EnvironmentComposeProjectionStorageKey(id), ModRevision: applied.Revision},
		{Key: deletions.TombstoneKey("environment", id)},
	}, nil
}

func isIngressKind(kind core.ComponentKind) bool {
	return kind == core.ComponentKindEdgeCloudflare || kind == core.ComponentKindIngressCaddy ||
		kind == core.ComponentKindIngressEnvoy
}

func (authority *Authority) routeObservationCondition(
	ctx context.Context,
	route environmentprojection.EnvironmentRouteProjection,
	revision int64,
) (keyvalue.Condition, error) {
	key := routes.ObservationKey(route.Desired.ID)
	read, err := authority.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Condition{}, incomplete("public ingress Route observation is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	condition := keyvalue.Condition{Key: key}
	if entry := read.Values[0]; entry != nil {
		value, err := routes.DecodeObservation(entry.Value)
		if err != nil || value.EnvironmentID != route.EnvironmentID || value.RouteID != route.Desired.ID ||
			entry.Key != key {
			return keyvalue.Condition{}, incomplete("public ingress Route observation identity is invalid")
		}
		if value.Observation.Status != routes.ObservedUnserved {
			return keyvalue.Condition{}, conflict("the Route still has an Environment ingress provider")
		}
		condition.ModRevision = entry.ModRevision
	}
	return condition, nil
}

func (authority *Authority) PublicIngressRoutes(
	ctx context.Context,
	hostID string,
) ([]api.PublicIngressRouteOption, error) {
	binding, err := authority.resolveLocalHost(ctx, hostID)
	if err != nil {
		return nil, err
	}
	environments, err := authority.scanEnvironments(ctx, binding.ReadRevision)
	if err != nil {
		return nil, err
	}
	result := make([]api.PublicIngressRouteOption, 0)
	for id, environment := range environments {
		names := make(map[string]string, len(environment.Record.DesiredServices))
		for _, service := range environment.Record.DesiredServices {
			names[service.Desired.ID] = service.Desired.Name
		}
		name, err := authority.environmentName(ctx, id, binding.ReadRevision)
		if err != nil {
			return nil, err
		}
		for _, route := range environment.Record.DesiredRoutes {
			if route.Desired.Exposure == "public" && route.Desired.Host != "" {
				result = append(result, api.PublicIngressRouteOption{ID: route.Desired.ID,
					Hostname: route.Desired.Host, Path: route.Desired.Path,
					Environment: name, Service: names[route.Desired.TargetServiceID]})
			}
		}
	}
	slices.SortFunc(result, func(a, b api.PublicIngressRouteOption) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
}

func (authority *Authority) environmentName(ctx context.Context, id string, revision int64) (string, error) {
	key := hierarchy.EnvironmentKey(id)
	read, err := authority.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return "", err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return "", incomplete("public ingress Environment identity is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	environment, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil || environment.ID != id {
		return "", incomplete("public ingress Environment identity is invalid")
	}
	return environment.Name, nil
}

func conflict(message string) error   { return errs.New(errs.KindStateConflict, message) }
func incomplete(message string) error { return errs.New(errs.KindInternal, message) }
