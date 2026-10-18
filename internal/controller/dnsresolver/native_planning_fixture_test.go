package dnsresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type nativeResolverBaseline struct{ content []byte }

func (baseline nativeResolverBaseline) GetHostResolverBaseline(
	context.Context,
) (keyvalue.Versioned[resolverbaseline.Record], bool, error) {
	digest := sha256.Sum256(baseline.content)
	return keyvalue.Versioned[resolverbaseline.Record]{Record: resolverbaseline.Record{
		Generation: 1, Content: append([]byte(nil), baseline.content...), SHA256: hex.EncodeToString(digest[:]),
		CapturedAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)}, Revision: 8, ReadRevision: 42}, true, nil
}

func (nativeResolverBaseline) EnsureHostResolverBaseline(
	context.Context,
	[]byte,
	time.Time,
) (keyvalue.Versioned[resolverbaseline.Record], error) {
	return keyvalue.Versioned[resolverbaseline.Record]{}, errs.New(errs.KindInternal, "unexpected baseline mutation")
}

type nativeResolverProjection struct {
	record hostresolution.HostResolutionProjectionRecord
}

func (reader nativeResolverProjection) GetHostResolutionProjection(
	context.Context,
) (keyvalue.Versioned[hostresolution.HostResolutionProjectionRecord], bool, error) {
	return keyvalue.Versioned[hostresolution.HostResolutionProjectionRecord]{
		Record:       reader.record,
		Revision:     10,
		ReadRevision: 42,
	}, true, nil
}

type nativeResolverCatalog struct{ definition componentsdk.Definition }

func (catalog nativeResolverCatalog) Digest() [32]byte { return catalog.definition.Digest() }

func (catalog nativeResolverCatalog) FindAction(
	implementation componentsdk.ImplementationKey,
	action componentsdk.ActionID,
) (componentsdk.Definition, componentsdk.ActionDefinition, bool) {
	if implementation != catalog.definition.Implementation() {
		return componentsdk.Definition{}, componentsdk.ActionDefinition{}, false
	}
	definition, found := catalog.definition.FindAction(action)
	return catalog.definition, definition, found
}

func (catalog nativeResolverCatalog) FindActionByCapability(
	capability componentsdk.Capability,
	actionID componentsdk.ActionID,
) (componentsdk.Definition, componentsdk.ActionDefinition, bool) {
	for _, provided := range catalog.definition.Provides() {
		if provided == capability {
			action, found := catalog.definition.FindAction(actionID)
			return catalog.definition, action, found
		}
	}
	return componentsdk.Definition{}, componentsdk.ActionDefinition{}, false
}

type nativeResolverEnvironmentPlanner struct {
	image string
	last  componentsdk.EnvironmentPlan
}

func (planner *nativeResolverEnvironmentPlanner) Plan(
	_ componentsdk.ImplementationKey,
	serviceID string,
	_ componentdns.RenderInput,
) (componentsdk.EnvironmentPlan, error) {
	planner.last = componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{
			{ID: serviceID, Name: "resolver", Image: testPlatformImage(planner.image),
				NetworkMode: componentsdk.ManagedNetworkModeHost, Restart: "unless-stopped", Replicas: 1,
				Mounts: []componentsdk.ManagedMount{
					{
						Source:   "config",
						Target:   "/etc/resolver",
						Kind:     componentsdk.ManagedMountKindDirectory,
						ReadOnly: true,
					},
				},
				ObservationAction: "observe-serving"},
		},
		Files: []componentsdk.ManagedFile{{Path: "config/Corefile", Content: []byte("captured Corefile\n")}},
	}
	return planner.last, nil
}

// The planner only reads native resource state. Unexpected writes/ranges fail;
// no Task, old observation or synthetic successful publication is provided.
type nativeResolverReadStore struct{ values map[string][]byte }

func (store nativeResolverReadStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if request.Revision != 42 {
		return nil, errs.New(errs.KindInternal, "unexpected resolver read revision")
	}
	result := &keyvalue.GetManyResult{ReadRevision: 42, Values: make([]*keyvalue.KeyValue, len(request.Keys))}
	for index, key := range request.Keys {
		if raw, found := store.values[key]; found {
			result.Values[index] = &keyvalue.KeyValue{Key: key, Value: append([]byte(nil), raw...), ModRevision: 12}
		}
	}
	return result, nil
}

func (nativeResolverReadStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	return nil, errs.New(errs.KindInternal, "unexpected resolver range")
}

func (nativeResolverReadStore) MeasureTransaction(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	return keyvalue.TransactionBudget{}, errs.New(errs.KindInternal, "unexpected resolver publication")
}

func (nativeResolverReadStore) Transact(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return keyvalue.TransactionResult{}, errs.New(errs.KindInternal, "unexpected resolver publication")
}

func nativeResolverPlanner(t *testing.T) (*PlatformRenderPlanner, *nativeResolverEnvironmentPlanner) {
	t.Helper()
	projection, err := hostresolution.NewHostResolutionProjectionRecord(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	environment := &nativeResolverEnvironmentPlanner{image: "example/resolver"}
	planner, err := NewPlatformRenderPlanner(nativeResolverProjection{projection}, emptyDNSRecordsResolver{},
		nativeResolverBaseline{[]byte("nameserver 1.1.1.1\n")}, func(context.Context) ([]byte, error) {
			return nil, errs.New(errs.KindInternal, "unexpected baseline capture")
		}, rendererPin{}, environment, nativeResolverCatalog{testResolverDefinition(t)}, "activate-config",
		netip.MustParseAddr("127.0.0.1"), testZoneListenerSource(t))
	if err != nil {
		t.Fatal(err)
	}
	return planner, environment
}
