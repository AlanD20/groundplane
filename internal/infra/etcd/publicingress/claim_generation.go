package publicingress

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	claimGenerationKey  = "/v1/public-ingress/hostname-generation"
	claimGenerationKind = "public-ingress-hostname-generation"
)

type claimGeneration struct{}

type claimReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

func readClaimGeneration(ctx context.Context, reader claimReader, revision int64) (keyvalue.Condition, error) {
	result, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{claimGenerationKey}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 1 {
		return keyvalue.Condition{}, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(result.Values)
	condition := keyvalue.Condition{Key: claimGenerationKey}
	if entry := result.Values[0]; entry != nil {
		_, err := recordcodec.Decode[claimGeneration](entry.Value, claimGenerationKind)
		if err != nil || entry.Key != claimGenerationKey || entry.ModRevision <= 0 || entry.ModRevision > revision {
			return keyvalue.Condition{}, recordcodec.CorruptRecord()
		}
		condition.ModRevision = entry.ModRevision
	}
	return condition, nil
}

// ReadHostnameOwners reads the exact public hostname owners and one compact
// generation guard for the caller's existing authored-publication transaction.
// A zero generation is valid before the first edge publication.
func ReadHostnameOwners(
	ctx context.Context, reader claimReader, hostnames []string, revision int64,
) (map[string]string, keyvalue.Condition, error) {
	if revision <= 0 {
		return nil, keyvalue.Condition{}, errs.New(
			errs.KindValidationFailed,
			"public ingress hostname lookup requires a captured revision",
		)
	}
	condition, err := readClaimGeneration(ctx, reader, revision)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	result := make(map[string]string)
	if condition.ModRevision == 0 {
		return result, condition, nil
	}
	for names := range slices.Chunk(hostnames, keyvalue.MaximumOperations) {
		owners, err := readHostnameChunk(ctx, reader, names, revision)
		if err != nil {
			return nil, keyvalue.Condition{}, err
		}
		for hostname, environmentID := range owners {
			result[hostname] = environmentID
		}
	}
	return result, condition, nil
}

func readHostnameChunk(
	ctx context.Context, reader claimReader, hostnames []string, revision int64,
) (map[string]string, error) {
	keys := make([]string, len(hostnames))
	for i, hostname := range hostnames {
		if !dnsname.Valid(hostname) {
			return nil, errs.New(
				errs.KindValidationFailed,
				"public ingress hostname lookup requires canonical hostnames",
			)
		}
		keys[i] = claimKey(hostname)
	}
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	result := make(map[string]string)
	for i, value := range read.Values {
		if value == nil {
			continue
		}
		actual, err := recordcodec.Decode[owner](value.Value, claimKind)
		if err != nil || value.Key != keys[i] || value.ModRevision <= 0 || value.ModRevision > revision ||
			ids.Validate(
				ids.KindHost,
				actual.HostID,
			) != nil || ids.Validate(ids.KindEnvironment, actual.EnvironmentID) != nil {
			return nil, recordcodec.CorruptRecord()
		}
		result[hostnames[i]] = actual.EnvironmentID
	}
	return result, nil
}
