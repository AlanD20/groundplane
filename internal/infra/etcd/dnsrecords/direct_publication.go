package dnsrecords

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDirectDNSPublication reserves only the edited record's new hostname.
// The caller fences the complete previous head and preserves every other
// declaration, so a single-record edit does not rewrite up to 256 name claims.
// Old claims remain until the resolver proves their answers withdrawn.
func (reader *Reader) PrepareDirectDNSPublication(ctx context.Context,
	input environmentprojection.EnvironmentDesiredInput,
	owned environmentprojection.EnvironmentOwnedIdentities, recordID string, revision int64,
) (conditions []keyvalue.Condition, mutations []keyvalue.Mutation, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if revision <= 0 || ids.Validate(ids.KindDNSRecord, recordID) != nil {
		return nil, nil, errs.New(
			errs.KindValidationFailed,
			"DNS publication requires a record identity and captured revision",
		)
	}
	if err := core.ValidateBlueprintDNSNames(input.Input.DNS, input.Input.Routes); err != nil {
		return nil, nil, err
	}
	records, err := AuthoredEnvironmentRecords(input, owned)
	if err != nil {
		return nil, nil, err
	}
	conditions, mutations, err = reader.prepareNameGeneration(ctx, revision)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(mutations)
		}
	}()
	for _, record := range records {
		if record.ID != recordID {
			continue
		}
		claims, writes, err := reader.PrepareHostnameClaim(ctx, record, revision)
		if err != nil {
			return conditions, mutations, err
		}
		conditions = append(conditions, claims...)
		mutations = append(mutations, writes...)
		break
	}
	conditions, err = uniqueConditions(conditions)
	return conditions, mutations, err
}
