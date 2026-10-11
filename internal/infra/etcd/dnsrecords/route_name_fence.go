package dnsrecords

import (
	"context"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routeNameGenerationKey = "/v1/records/platform/dns-route-name-generation"

// The modification revision is the generation. Rewriting the same empty value
// serializes Route-name publication with a hostname claim across Environments,
// including a newly created Environment that was absent from the claimant's scan.
type routeNameGeneration struct{}

func (reader *Reader) routeNameCondition(ctx context.Context, revision int64) (keyvalue.Condition, error) {
	read, err := reader.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{routeNameGenerationKey}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision {
		return keyvalue.Condition{}, errs.New(errs.KindInternal, "DNS Route-name generation read is incomplete")
	}
	condition := keyvalue.Condition{Key: routeNameGenerationKey}
	if value := read.Values[0]; value != nil {
		if _, err := recordcodec.Decode[routeNameGeneration](value.Value, "dns_route_name_generation"); err != nil {
			return keyvalue.Condition{}, err
		}
		condition.ModRevision = value.ModRevision
	}
	return condition, nil
}

// MatchResolverDNSGeneration checks whether a failed resolver request still
// belongs to the current authored DNS decisions, including newly added Zones.
func (reader *Reader) MatchResolverDNSGeneration(
	ctx context.Context,
	captured []keyvalue.Condition,
	revision int64,
) (keyvalue.Condition, bool, error) {
	current, err := reader.routeNameCondition(ctx, revision)
	if err != nil {
		return keyvalue.Condition{}, false, err
	}
	for _, condition := range captured {
		if condition.Key == current.Key {
			return current, condition == current, nil
		}
	}
	return keyvalue.Condition{}, false, errs.New(errs.KindInternal, "resolver request has no captured DNS generation")
}

// PrepareRouteNamePublication is committed with the caller's desired-head
// publication, not separately. Unchanged hostnames need no global write.
func (reader *Reader) PrepareRouteNamePublication(
	ctx context.Context,
	projection environmentprojection.EnvironmentComposeProjection,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	conditions, mutations, err := reader.prepareRouteNameSet(
		ctx,
		projection.EnvironmentID,
		routeNames(projection),
		revision,
	)
	if err != nil || len(mutations) != 0 {
		return conditions, mutations, err
	}
	zoneConditions, zoneMutations, err := reader.prepareZoneDNSGeneration(
		ctx,
		projection.EnvironmentID,
		projection.NormalizedCompose,
		revision,
	)
	return append(conditions, zoneConditions...), zoneMutations, err
}

// Authored admission reserves Route names before fact-dependent runtime is
// available. Waiting for the eventual projection would leave a claim race
// between accepting a Blueprint and executing its children.
func (reader *Reader) PrepareAuthoredRouteNamePublication(
	ctx context.Context,
	input environmentprojection.EnvironmentDesiredInput,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	names := authoredRouteNames(input.Input.Routes)
	conditions, err := reader.RequireHostnamesUnclaimed(ctx, names, revision)
	if err != nil {
		return nil, nil, err
	}
	generationConditions, mutations, err := reader.prepareRouteNameSet(ctx, input.EnvironmentID, names, revision)
	if err != nil {
		return nil, nil, err
	}
	conditions = append(conditions, generationConditions...)
	if len(mutations) == 0 {
		previous, _, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, input.EnvironmentID, revision)
		if err != nil {
			return nil, nil, err
		}
		if !maps.Equal(previous.Record.Input.DNS, input.Input.DNS) {
			generationConditions, mutations, err = reader.prepareNameGeneration(ctx, revision)
			if err != nil {
				return nil, nil, err
			}
			conditions = append(conditions, generationConditions...)
		}
	}
	if len(mutations) == 0 {
		zoneConditions, zoneMutations, err := reader.prepareZoneDNSGeneration(
			ctx,
			input.EnvironmentID,
			input.Input.NormalizedCompose,
			revision,
		)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, zoneConditions...)
		mutations = zoneMutations
	}
	return conditions, mutations, nil
}

func (reader *Reader) prepareZoneDNSGeneration(
	ctx context.Context,
	environmentID string,
	normalized []byte,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	previous, _, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, environmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	before, err := zoneDNSDecisions(previous.Record.Input.NormalizedCompose)
	if err != nil {
		return nil, nil, err
	}
	after, err := zoneDNSDecisions(normalized)
	if err != nil {
		return nil, nil, err
	}
	if maps.Equal(before, after) {
		return nil, nil, nil
	}
	return reader.prepareNameGeneration(ctx, revision)
}

func (reader *Reader) prepareRouteNameSet(
	ctx context.Context,
	environmentID string,
	names []string,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	previous, found, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, environmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	if found && slices.Equal(authoredRouteNames(previous.Record.Input.Routes), names) ||
		!found && len(names) == 0 {
		return nil, nil, nil
	}
	return reader.prepareNameGeneration(ctx, revision)
}

// Authored DNS changes share this fence with Route names: a resolver scan cannot
// omit a newly accepted Environment or race a namespace/target change.
func (reader *Reader) prepareNameGeneration(
	ctx context.Context,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	condition, err := reader.routeNameCondition(ctx, revision)
	if err != nil {
		return nil, nil, err
	}
	value, err := recordcodec.Encode("dns_route_name_generation", routeNameGeneration{})
	if err != nil {
		return nil, nil, err
	}
	return []keyvalue.Condition{condition}, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: routeNameGenerationKey, Value: value,
	}}, nil
}

func authoredRouteNames(routes []core.RouteSpec) []string {
	names := make([]string, 0, len(routes))
	for _, route := range routes {
		if route.Hostname != "" {
			names = append(names, route.Hostname)
		}
	}
	sort.Strings(names)
	return slices.Compact(names)
}

func routeNames(projection environmentprojection.EnvironmentComposeProjection) []string {
	names := make([]string, 0, len(projection.DesiredRoutes))
	for _, route := range projection.DesiredRoutes {
		if route.Desired.Host != "" {
			names = append(names, route.Desired.Host)
		}
	}
	sort.Strings(names)
	return slices.Compact(names)
}

// A claim also checks existing desired Routes, not only the asynchronous
// host-resolution projection. The generation compare closes the scan's phantom
// gap; publication that changes this set rewrites it in the same transaction.
func (reader *Reader) requireRouteNameAvailable(ctx context.Context, hostname string, revision int64) error {
	start := ""
	for {
		page, err := reader.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: environmentqueries.EnvironmentDesiredHeadScanPrefix, StartExclusive: start,
			Limit: keyvalue.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return errs.New(errs.KindInternal, "DNS Route-name scan is incomplete")
		}
		for _, value := range page.Values {
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(
				strings.TrimPrefix(value.Key, environmentqueries.EnvironmentDesiredHeadScanPrefix),
				"/current",
			)
			if ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return errs.New(errs.KindInternal, "DNS Route-name owner is corrupt")
			}
			input, found, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, environmentID, revision)
			if err != nil {
				return err
			}
			if found && slices.Contains(authoredRouteNames(input.Record.Input.Routes), hostname) {
				return errs.New(
					errs.KindResourceInUse,
					"DNS hostname "+hostname+" is already used by a Route in Environment "+environmentID,
				)
			}
		}
		if !page.More {
			return nil
		}
	}
}
