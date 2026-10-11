package dnsrecords

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const hostnameRetirementBatch = 16

// PrepareObsoleteHostnameRetirement covers metadata-only edits, which have no
// Blueprint parent to settle a DNS unit. The caller must prove the acknowledged
// resolver bindings equal this complete current selection and compare both the
// selection's conditions and resolver acknowledgement in the final transaction.
// Waiting records still own their names even though they have no DNS answer.
func (reader *Reader) PrepareObsoleteHostnameRetirement(ctx context.Context,
	current []platformcomponents.EnvironmentDNSCapture, hosts []platformcomponents.PlatformDNSHost, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if revision <= 0 {
		return nil, nil, errs.New(errs.KindValidationFailed, "DNS hostname retirement requires a captured revision")
	}
	if err := platformcomponents.ValidateEnvironmentDNS(current, hosts); err != nil {
		return nil, nil, err
	}
	retained := make(map[string]struct{}, len(current))
	for _, capture := range current {
		retained[capture.Binding.Record.Hostname()] = struct{}{}
	}
	// An old answer must protect its name even if a caller's current selection
	// omits it. Never use desired absence as evidence of resolver withdrawal.
	for _, host := range hosts {
		for _, name := range host.Hostnames {
			retained[name] = struct{}{}
		}
	}
	var conditions []keyvalue.Condition
	var mutations []keyvalue.Mutation
	start := ""
	for {
		page, err := reader.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: hostnameClaimPrefix, StartExclusive: start, Limit: keyvalue.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return nil, nil, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return nil, nil, errs.New(errs.KindInternal, "DNS hostname retirement scan is incomplete")
		}
		for _, value := range page.Values {
			start = value.Key
			claim, err := DecodeHostnameClaim(value.Value)
			if err != nil {
				keyvalue.ClearRangeValues(page.Values)
				return nil, nil, err
			}
			if value.Key != HostnameClaimKey(claim.Hostname) {
				keyvalue.ClearRangeValues(page.Values)
				return nil, nil, errs.New(errs.KindInternal, "DNS hostname claim key is corrupt")
			}
			if _, keep := retained[claim.Hostname]; keep {
				continue
			}
			conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: value.Key})
			if len(mutations) == hostnameRetirementBatch {
				keyvalue.ClearRangeValues(page.Values)
				return conditions, mutations, nil
			}
		}
		keyvalue.ClearRangeValues(page.Values)
		if !page.More {
			return conditions, mutations, nil
		}
	}
}

// PrepareHostnameRetirement releases only this stable record's obsolete names.
// The caller must compare the acknowledged resolver and current authored head
// in the same transaction. Batches stay bounded even after repeated renames.
func (reader *Reader) PrepareHostnameRetirement(ctx context.Context,
	environmentID, recordID, keepHostname string, hosts []platformcomponents.PlatformDNSHost, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, false, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindDNSRecord, recordID) != nil ||
		revision <= 0 {
		return nil, nil, false, errs.New(errs.KindValidationFailed, "DNS hostname retirement identity is invalid")
	}
	var conditions []keyvalue.Condition
	var mutations []keyvalue.Mutation
	more := false
	start := ""
	for {
		page, err := reader.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: hostnameClaimPrefix, StartExclusive: start, Limit: keyvalue.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return nil, nil, false, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return nil, nil, false, errs.New(errs.KindInternal, "DNS hostname retirement scan is incomplete")
		}
		for _, value := range page.Values {
			start = value.Key
			claim, err := DecodeHostnameClaim(value.Value)
			if err != nil {
				return nil, nil, false, err
			}
			if value.Key != HostnameClaimKey(claim.Hostname) {
				return nil, nil, false, errs.New(errs.KindInternal, "DNS hostname claim key is corrupt")
			}
			if claim.EnvironmentID != environmentID || claim.RecordID != recordID || claim.Hostname == keepHostname {
				continue
			}
			for _, host := range hosts {
				if slices.Contains(host.Hostnames, claim.Hostname) {
					// No claim is released while any obsolete answer is serving.
					return nil, nil, true, nil
				}
			}
			if len(mutations) == hostnameRetirementBatch {
				more = true
				continue
			}
			conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: value.Key})
		}
		keyvalue.ClearRangeValues(page.Values)
		if !page.More {
			return conditions, mutations, more, nil
		}
	}
}
