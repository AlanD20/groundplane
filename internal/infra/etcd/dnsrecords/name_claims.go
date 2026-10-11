package dnsrecords

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const hostnameClaimPrefix = "/v1/records/environment-dns-hostname-claims/"

// HostnameClaim reserves the full hostname even while its Service is absent.
// It carries ownership only; the Environment desired revision owns intention,
// and acknowledged CoreDNS state owns whether an answer is actually serving.
type HostnameClaim struct {
	RecordID      string `json:"record_id"`
	Hostname      string `json:"hostname"`
	EnvironmentID string `json:"environment_id"`
	Label         string `json:"label"`
}

func HostnameClaimKey(hostname string) string { return hostnameClaimPrefix + hostname }

func (claim HostnameClaim) validate() error {
	if ids.Validate(ids.KindEnvironment, claim.EnvironmentID) != nil ||
		ids.Validate(ids.KindDNSRecord, claim.RecordID) != nil ||
		!dnsname.Valid(claim.Hostname) || claim.Hostname == imagefetch.RegistryHostname ||
		!dnsname.Valid(claim.Label) || strings.Contains(claim.Label, ".") ||
		!strings.HasPrefix(claim.Hostname, claim.Label+".") || !strings.HasSuffix(claim.Hostname, ".internal") {
		return errs.New(errs.KindValidationFailed, "Environment DNS hostname claim is invalid")
	}
	return nil
}

func EncodeHostnameClaim(claim HostnameClaim) ([]byte, error) {
	if err := claim.validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("environment_dns_hostname_claim", claim)
}

func DecodeHostnameClaim(value []byte) (HostnameClaim, error) {
	claim, err := recordcodec.Decode[HostnameClaim](value, "environment_dns_hostname_claim")
	if err != nil || claim.validate() != nil {
		return HostnameClaim{}, errs.New(errs.KindInternal, "Environment DNS hostname claim is corrupt")
	}
	return claim, nil
}

// RequireHostnamesUnclaimed participates in the caller's atomic publication.
// Even an absent claim contributes a compare, preventing an Environment claim
// racing a platform DNS or Route update from being silently overwritten.
func (reader *Reader) RequireHostnamesUnclaimed(
	ctx context.Context,
	hostnames []string,
	revision int64,
) ([]keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, err
	}
	names := slices.Clone(hostnames)
	sort.Strings(names)
	names = slices.Compact(names)
	if len(names) == 0 {
		return nil, nil
	}
	keys := make([]string, len(names))
	for index, hostname := range names {
		if !dnsname.Valid(hostname) {
			return nil, errs.New(errs.KindValidationFailed, "DNS hostname is not a canonical name")
		}
		keys[index] = HostnameClaimKey(hostname)
	}
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || len(read.Values) != len(keys) || read.ReadRevision <= 0 ||
		revision > 0 && read.ReadRevision != revision {
		return nil, errs.New(errs.KindInternal, "DNS hostname ownership read is incomplete")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key}
		if value := read.Values[index]; value != nil {
			claim, err := DecodeHostnameClaim(value.Value)
			if err != nil {
				return nil, err
			}
			if claim.Hostname != names[index] {
				return nil, errs.New(errs.KindInternal, "DNS hostname claim does not match its storage key")
			}
			return nil, hostnameOwned(claim)
		}
	}
	return conditions, nil
}

// PrepareHostnameClaim reserves a new name or verifies the exact existing
// owner. The caller commits it with desired-state and native Task publication,
// never by a standalone Put. Retiring a claim requires proven DNS withdrawal.
func (reader *Reader) PrepareHostnameClaim(
	ctx context.Context,
	record model.EnvironmentRecord,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if err := record.Validate(); err != nil {
		return nil, nil, err
	}
	claim := HostnameClaim{
		RecordID:      record.ID,
		Hostname:      record.Hostname(),
		EnvironmentID: record.EnvironmentID,
		Label:         record.Label,
	}
	key := HostnameClaimKey(claim.Hostname)
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || len(read.Values) != 1 || read.ReadRevision <= 0 ||
		revision > 0 && read.ReadRevision != revision {
		return nil, nil, errs.New(errs.KindInternal, "DNS hostname claim read is incomplete")
	}
	condition := keyvalue.Condition{Key: key}
	if current := read.Values[0]; current != nil {
		previous, err := DecodeHostnameClaim(current.Value)
		if err != nil {
			return nil, nil, err
		}
		if previous.Hostname != claim.Hostname {
			return nil, nil, errs.New(errs.KindInternal, "DNS hostname claim does not match its storage key")
		}
		if previous != claim {
			return nil, nil, hostnameOwned(previous)
		}
		condition.ModRevision = current.ModRevision
		return []keyvalue.Condition{condition}, nil, nil
	}
	generation, err := reader.routeNameCondition(ctx, read.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	if err := reader.requireRouteNameAvailable(ctx, claim.Hostname, read.ReadRevision); err != nil {
		return nil, nil, err
	}
	platform, err := reader.references(ctx, read.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	for _, platformRecord := range platform.records {
		if platformRecord.Hostname == claim.Hostname {
			return nil, nil, errs.New(
				errs.KindResourceInUse,
				"DNS hostname "+claim.Hostname+" is already owned by platform DNS; withdraw that record before reusing its name",
			)
		}
	}
	for _, host := range platform.hosts {
		if slices.Contains(host.Hostnames, claim.Hostname) {
			return nil, nil, errs.New(
				errs.KindResourceInUse,
				"DNS hostname "+claim.Hostname+" is still present in the acknowledged CoreDNS configuration; wait for its withdrawal Task to complete",
			)
		}
	}
	conditions := append(platform.conditions, condition, generation)
	conditions, err = uniqueConditions(conditions)
	if err != nil {
		return nil, nil, err
	}
	value, err := EncodeHostnameClaim(claim)
	if err != nil {
		return nil, nil, err
	}
	return conditions, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}, nil
}

func hostnameOwned(claim HostnameClaim) error {
	return errs.New(errs.KindResourceInUse, fmt.Sprintf(
		"DNS hostname %s is reserved by record %s in Environment %s; change that record before reusing its name",
		claim.Hostname, claim.Label, claim.EnvironmentID,
	))
}
