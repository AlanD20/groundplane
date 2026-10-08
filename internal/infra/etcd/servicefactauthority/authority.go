// Package servicefactauthority selects the persisted applied authority for a
// native Backing or a Release workload. Absence never changes the selected kind.
package servicefactauthority

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Kind uint8

const (
	ReleaseRuntime Kind = iota + 1
	BackingRuntime
)

type Applied struct {
	Artifact             *agentpb.ComposeArtifact
	Workload             *agentpb.ComposeService
	LocalImageID         string
	ManagedReleaseSHA256 string
}

func KindForService(service services.EnvironmentServiceProjection) (Kind, error) {
	if service.BackingNetworkID == "" && service.Desired.Adapter == "" {
		return ReleaseRuntime, nil
	}
	if service.BackingNetworkID != "" && service.Desired.Adapter == "postgres" {
		return BackingRuntime, nil
	}
	return 0, errs.New(errs.KindStrategyNotImplemented, "backup Service runtime kind is unsupported")
}

func Key(kind Kind, environmentID, serviceID string) string {
	switch kind {
	case ReleaseRuntime:
		return serviceruntimerecord.Key(serviceID)
	case BackingRuntime:
		return backingpostgresruntime.Key(environmentID, serviceID)
	default:
		return ""
	}
}

func ReadApplied(value *etcdstore.KeyValue, kind Kind, environmentID, serviceID string) (Applied, error) {
	if value == nil || value.ModRevision <= 0 || value.Key != Key(kind, environmentID, serviceID) {
		return Applied{}, invalid()
	}
	if kind == BackingRuntime {
		record, err := backingpostgresruntime.Decode(value.Value)
		if err != nil || record.EnvironmentID != environmentID || record.ServiceID != serviceID {
			return Applied{}, invalid()
		}
		artifact, workload, err := backingpostgresruntime.Select(record)
		if err != nil {
			return Applied{}, err
		}
		return Applied{Artifact: artifact, Workload: workload, LocalImageID: record.LocalImageID,
			ManagedReleaseSHA256: record.CatalogSHA256}, nil
	}
	if kind != ReleaseRuntime {
		return Applied{}, invalid()
	}
	record, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
		value.Value,
		"service-acknowledged-runtime",
	)
	if err != nil || record.EnvironmentID != environmentID || record.Runtime.ServiceID != serviceID ||
		serviceruntimerecord.Validate(record) != nil {
		return Applied{}, invalid()
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(record.Runtime.CurrentArtifact, artifact) != nil || artifact.OwnerId != environmentID {
		return Applied{}, invalid()
	}
	var workload *agentpb.ComposeService
	for _, candidate := range artifact.Services {
		if candidate.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON ||
			candidate.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
			if workload != nil || candidate.GetServiceId() != serviceID {
				return Applied{}, invalid()
			}
			workload = candidate
		}
	}
	if workload == nil || workload.ComposeName == "" || !workloadimage.LocalIDValid(workload.ImageReference) ||
		len(workload.ExpectedLabels) == 0 {
		return Applied{}, invalid()
	}
	at, err := ids.Timestamp(ids.KindConfig, artifact.ArtifactId)
	if err != nil {
		return Applied{}, err
	}
	// Member projections can share a candidate ID but contain different bytes.
	// Every reader uses the same exact-value handle; labels remain unchanged.
	digest := sha256.Sum256(value.Value)
	artifact.ArtifactId = ids.DeriveAt(ids.KindConfig, at,
		hex.EncodeToString(digest[:]), "backup-runtime:"+serviceID)
	return Applied{Artifact: artifact, Workload: workload, LocalImageID: workload.ImageReference}, nil
}

func invalid() error {
	return errs.New(errs.KindStateConflict, "backup Service applied runtime authority is unavailable or invalid")
}
