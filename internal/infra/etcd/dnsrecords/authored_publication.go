package dnsrecords

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareAuthoredDNSPublication contributes name ownership to the same atomic
// publication as the accepted Blueprint head, parent Task and idempotency record.
// Old claims are deliberately retained: omission or a namespace change cannot
// release a name that the last successful CoreDNS configuration still serves.
func (reader *Reader) PrepareAuthoredDNSPublication(
	ctx context.Context,
	input environmentprojection.EnvironmentDesiredInput,
	identities environmentprojection.EnvironmentOwnedIdentities,
	revision int64,
) (conditions []keyvalue.Condition, mutations []keyvalue.Mutation, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if revision <= 0 {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint DNS publication requires a captured revision")
	}
	if err := core.ValidateBlueprintDNSNames(input.Input.DNS, input.Input.Routes); err != nil {
		return nil, nil, err
	}
	conditions, mutations, err := reader.PrepareAuthoredRouteNamePublication(ctx, input, revision)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(mutations)
		}
	}()
	if len(input.Input.DNS) == 0 {
		return conditions, mutations, nil
	}
	records, err := AuthoredEnvironmentRecords(input, identities)
	if err != nil {
		return conditions, mutations, err
	}
	for _, record := range records {
		claimConditions, claimMutations, err := reader.PrepareHostnameClaim(ctx, record, revision)
		if err != nil {
			return conditions, mutations, err
		}
		conditions = append(conditions, claimConditions...)
		mutations = append(mutations, claimMutations...)
	}
	conditions, err = uniqueConditions(conditions)
	if err != nil {
		return conditions, mutations, err
	}
	return conditions, mutations, nil
}
