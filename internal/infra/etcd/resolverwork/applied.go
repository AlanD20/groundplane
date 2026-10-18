package resolverwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumAppliedBytes = 512 * 1024

// Applied is resource state, not a terminal Task receipt. Its exact Corefile,
// runtime and DNS answers remain usable after execution history is collected.
type Applied struct {
	HostID           string                                     `json:"host_id"`
	ComponentID      string                                     `json:"component_id"`
	Generation       uint64                                     `json:"generation"`
	Desired          componentrecord.DesiredRecord              `json:"desired"`
	Configuration    *resolverexecution.Configuration           `json:"configuration,omitempty"`
	FileOwner        string                                     `json:"file_owner"`
	ServingProof     *agentpb.DNSResolverObservationEvidence    `json:"serving_proof,omitempty"`
	BaselineSHA256   string                                     `json:"baseline_sha256"`
	ProjectionSHA256 string                                     `json:"projection_sha256"`
	Hosts            []platformcomponents.PlatformDNSHost       `json:"hosts"`
	EnvironmentDNS   []platformcomponents.EnvironmentDNSCapture `json:"environment_dns"`
	PrivateListener  string                                     `json:"private_listener,omitempty"`
	ZoneListeners    []string                                   `json:"zone_listeners,omitempty"`
	ObservedAt       time.Time                                  `json:"observed_at"`
}

func (applied Applied) ConfigurationSHA256() string {
	if applied.Configuration == nil {
		return ""
	}
	return hex.EncodeToString(applied.Configuration.Apply.ArtifactDigest)
}

func (applied Applied) Validate() error {
	if ids.Validate(ids.KindHost, applied.HostID) != nil || ids.Validate(ids.KindComponent, applied.ComponentID) != nil ||
		applied.Generation == 0 || applied.Desired.ID != applied.ComponentID || applied.Desired.Owner != core.ComponentOwnerPlatform ||
		applied.Desired.OwnerID != "" || applied.Desired.Kind != core.ComponentKindCoreDNS ||
		componentrecord.ValidateRecord(componentrecord.Record{Desired: applied.Desired}) != nil ||
		(applied.Configuration == nil) != (applied.FileOwner == "") || applied.Configuration != nil && !resolverexecution.ValidFileOwner(applied.FileOwner) || !recordcodec.IsCanonicalUTC(applied.ObservedAt) ||
		!recordcodec.ValidSHA256(applied.BaselineSHA256) || !recordcodec.ValidSHA256(applied.ProjectionSHA256) {
		return invalid("DNS resolver applied state has invalid resource identity or configuration")
	}
	configuration := applied.Configuration
	if configuration != nil && (configuration.Validate(applied.ComponentID) != nil || configuration.Generation > applied.Generation) {
		return invalid("DNS resolver applied state has invalid retained file configuration")
	}
	if applied.Desired.Enabled {
		if configuration == nil || configuration.Generation != applied.Generation ||
			resolverexecution.ValidateServingProof(*configuration, applied.ServingProof) != nil ||
			applied.ServingProof.ObservedAt.AsTime().After(applied.ObservedAt) ||
			platformcomponents.ValidateEnvironmentDNS(applied.EnvironmentDNS, applied.Hosts) != nil {
			return invalid("DNS resolver applied state lacks its exact acknowledged serving proof")
		}
	} else if applied.ServingProof != nil || len(applied.Hosts) != 0 || len(applied.EnvironmentDNS) != 0 ||
		applied.PrivateListener != "" || len(applied.ZoneListeners) != 0 {
		return invalid("disabled DNS resolver applied state cannot publish active answers or listeners")
	}
	return nil
}

func EncodeApplied(applied Applied) ([]byte, error) {
	if err := applied.Validate(); err != nil {
		return nil, err
	}
	raw, err := recordcodec.Encode("resolver_applied", applied)
	if err != nil {
		return nil, err
	}
	if len(raw) > maximumAppliedBytes {
		return nil, invalid("DNS resolver applied configuration exceeds its bound")
	}
	return raw, nil
}

func DecodeApplied(raw []byte) (Applied, error) {
	if len(raw) == 0 || len(raw) > maximumAppliedBytes {
		return Applied{}, invalid("DNS resolver applied configuration is missing or oversized")
	}
	applied, err := recordcodec.Decode[Applied](raw, "resolver_applied")
	if err != nil {
		return Applied{}, err
	}
	return applied, applied.Validate()
}

func (repository *Repository) ReadApplied(ctx context.Context, hostID, componentID string, revision int64) (keyvalue.Versioned[Applied], bool, error) {
	return ReadApplied(ctx, repository.store, hostID, componentID, revision)
}

// ReadApplied needs no mutation capability, Task lookup or live Agent binding.
func ReadApplied(ctx context.Context, store hostbindings.Reader, hostID, componentID string, revision int64) (keyvalue.Versioned[Applied], bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[Applied]{}, false, err
	}
	if store == nil || revision < 0 || ids.Validate(ids.KindHost, hostID) != nil || ids.Validate(ids.KindComponent, componentID) != nil {
		return keyvalue.Versioned[Applied]{}, false, invalid("DNS resolver applied read requires its actual host and Component")
	}
	key := AppliedKey(hostID, componentID)
	read, err := readStateKeys(ctx, store, []string{key}, revision)
	if err != nil {
		return keyvalue.Versioned[Applied]{}, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Applied]{ReadRevision: read.ReadRevision}, false, nil
	}
	applied, err := DecodeApplied(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Applied]{}, false, err
	}
	if applied.HostID != hostID || applied.ComponentID != componentID {
		return keyvalue.Versioned[Applied]{}, false, invalid("DNS resolver applied record belongs to another host or Component")
	}
	return keyvalue.Versioned[Applied]{Record: applied, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, true, nil
}
