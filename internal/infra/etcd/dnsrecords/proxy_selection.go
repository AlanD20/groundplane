package dnsrecords

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type proxySelection struct {
	address         string
	runtimeRevision int64
	conditions      []keyvalue.Condition
}

// selectProxy is shared by platform stable-id records and Environment named
// records. An unacknowledged proxy has no answer; its absent keys remain fenced.
func (reader *Reader) selectProxy(
	ctx context.Context,
	service keyvalue.Versioned[services.ServiceRecord],
	zone keyvalue.Versioned[zones.Record],
	revision int64,
) (proxySelection, error) {
	if service.Record.EnvironmentID != zone.Record.EnvironmentID ||
		!slices.Contains(service.Record.Desired.Zones, zone.Record.Desired.Name) ||
		service.Record.Desired.Adapter != "" {
		return proxySelection{}, errs.New(
			errs.KindValidationFailed,
			"DNS Service target must belong to its selected Zone",
		)
	}
	if err := domain.RequireTCPProxyPort(service.Record.Desired.Expose); err != nil {
		return proxySelection{}, err
	}
	serviceID, zoneID := service.Record.Desired.ID, zone.Record.Desired.ID
	keys := []string{
		networkreservations.ComponentAddressRegistryKey(zoneID),
		serviceruntimerecord.Key(serviceID),
		deletions.TombstoneKey("service", serviceID),
		deletions.TombstoneKey("zone", zoneID),
	}
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return proxySelection{}, err
	}
	if read == nil || len(read.Values) != len(keys) || read.ReadRevision != revision {
		return proxySelection{}, errs.New(errs.KindInternal, "DNS target authority read is incomplete")
	}
	if read.Values[2] != nil || read.Values[3] != nil {
		return proxySelection{}, errs.New(errs.KindResourceInUse, "DNS target is being removed")
	}
	selection := proxySelection{conditions: []keyvalue.Condition{services.ServiceDesiredCondition(service)}}
	for index, key := range keys {
		condition := keyvalue.Condition{Key: key}
		if value := read.Values[index]; value != nil {
			condition.ModRevision = value.ModRevision
		}
		selection.conditions = append(selection.conditions, condition)
	}
	if read.Values[0] == nil || read.Values[1] == nil {
		return selection, nil
	}
	registry, err := recordcodec.Decode[networkreservations.ComponentAddressRegistry](
		read.Values[0].Value, "component_address_registry",
	)
	if err != nil || networkreservations.ValidateComponentAddressRegistry(zone.Record, registry) != nil {
		return proxySelection{}, networkreservations.CorruptComponentAddressRegistry()
	}
	runtime, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
		read.Values[1].Value, "service-acknowledged-runtime",
	)
	if err != nil {
		return proxySelection{}, err
	}
	if serviceruntimerecord.Validate(runtime) != nil || runtime.EnvironmentID != service.Record.EnvironmentID ||
		runtime.Runtime.ServiceID != serviceID {
		return proxySelection{}, errs.New(errs.KindInternal, "DNS target runtime authority is corrupt")
	}
	address := registry.ServiceReservations[serviceID]
	if appliedProxyAddress(runtime, serviceID, zoneID, address) {
		selection.address = address
		selection.runtimeRevision = read.Values[1].ModRevision
	}
	return selection, nil
}

func (selection proxySelection) waiting() bool { return selection.address == "" }

func ordinaryRunning(service services.ServiceRecord) bool {
	return service.Desired.Adapter == "" && service.Runtime.RuntimeIntent == core.ServiceRuntimeIntentRunning &&
		service.Runtime.UnconfirmedLifecycleTaskID == ""
}
