package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentServiceProjection struct {
	EnvironmentID    string       `json:"environment_id"`
	BackingNetworkID string       `json:"backing_network_id,omitempty"`
	Desired          core.Service `json:"desired"`
}

// DesiredSelection carries the selected immutable desired state and the
// snapshot at which its independently mutable runtime must be read.
type DesiredSelection struct {
	Services     []EnvironmentServiceProjection
	Revision     int64
	ReadRevision int64
}
type runtimeReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func ReadJoined(
	ctx context.Context,
	store runtimeReader,
	projection DesiredSelection,
	serviceID string,
	desiredFenceKey string,
) (etcdstore.Versioned[ServiceRecord], error) {
	var desired *EnvironmentServiceProjection
	for index := range projection.Services {
		candidate := &projection.Services[index]
		if candidate.Desired.ID == serviceID {
			desired = candidate
			break
		}
	}
	if desired == nil {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{ServiceRuntimeKey(serviceID)}, Revision: projection.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision != projection.ReadRevision {
		return etcdstore.Versioned[ServiceRecord]{}, errs.New(errs.KindInternal, "Service runtime read is invalid")
	}
	runtime := core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning}
	runtimeRevision := int64(0)
	if read.Values[0] != nil {
		sidecar, decodeErr := DecodeServiceRuntimeRecord(read.Values[0].Value)
		if decodeErr != nil || sidecar.EnvironmentID != desired.EnvironmentID || sidecar.ServiceID != serviceID ||
			sidecar.BackingNetworkID != desired.BackingNetworkID {
			return etcdstore.Versioned[ServiceRecord]{}, recordcodec.CorruptRecord()
		}
		runtime = sidecar.Runtime
		runtimeRevision = read.Values[0].ModRevision
	}
	record := ServiceRecord{
		EnvironmentID: desired.EnvironmentID, BackingNetworkID: desired.BackingNetworkID,
		Desired: desired.Desired, Runtime: runtime,
		desiredFenceKey: desiredFenceKey, runtimeRevision: runtimeRevision,
	}
	if err := ValidateServiceRecord(record); err != nil {
		return etcdstore.Versioned[ServiceRecord]{}, recordcodec.CorruptRecord()
	}
	return etcdstore.Versioned[ServiceRecord]{
		Record:       record,
		Revision:     projection.Revision,
		ReadRevision: projection.ReadRevision,
	}, nil
}
