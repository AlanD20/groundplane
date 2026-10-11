package dnsrecords

import (
	"context"
	"sort"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SelectAuthoredEnvironmentRecords uses the accepted immutable desired input,
// including a head whose fact-dependent runtime projection is not sealed yet.
// An unsealed named target waits; it never inherits the previous head's answer.
func (reader *Reader) SelectAuthoredEnvironmentRecords(
	ctx context.Context, environmentID string, revision int64,
) (EnvironmentSelection, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return EnvironmentSelection{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || revision < 0 {
		return EnvironmentSelection{}, errs.New(
			errs.KindValidationFailed,
			"Environment DNS owner or revision is invalid",
		)
	}
	view, err := reader.environmentDNSView(ctx, environmentID, revision)
	if err != nil {
		return EnvironmentSelection{}, err
	}
	desired, found, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, environmentID, view.revision)
	if err != nil {
		return EnvironmentSelection{}, err
	}
	if !found {
		return reader.selectEnvironmentRecordsInView(ctx, nil, view)
	}
	records, err := AuthoredEnvironmentRecords(desired.Record, view.identities.Record)
	if err != nil {
		return EnvironmentSelection{}, err
	}
	return reader.selectEnvironmentRecordsInView(ctx, records, view)
}

// AuthoredEnvironmentRecords assigns only the Zone ids already captured for
// this same desired revision. It neither searches other Environments nor
// replaces authored Service names with a prepared runtime identity.
func AuthoredEnvironmentRecords(
	desired environmentprojection.EnvironmentDesiredInput,
	identities environmentprojection.EnvironmentOwnedIdentities,
) ([]model.EnvironmentRecord, error) {
	if err := environmentprojection.ValidateEnvironmentDesiredInput(desired); err != nil {
		return nil, err
	}
	if err := environmentprojection.ValidateEnvironmentOwnedIdentities(identities); err != nil {
		return nil, err
	}
	if desired.EnvironmentID != identities.EnvironmentID || desired.RevisionID != identities.RevisionID ||
		desired.RenderGeneration != identities.RenderGeneration {
		return nil, errs.New(
			errs.KindStateConflict,
			"Environment DNS input and Zone identities belong to different desired revisions",
		)
	}
	zones := make(map[string]string, len(identities.Networks))
	for _, identity := range identities.Networks {
		zones[identity.Name] = identity.ID
	}
	recordIDs := make(map[string]string, len(identities.DNSRecords))
	for _, identity := range identities.DNSRecords {
		recordIDs[identity.Name] = identity.ID
	}
	if len(recordIDs) != len(desired.Input.DNS) {
		return nil, errs.New(errs.KindStateConflict, "Environment DNS declaration identities are incomplete")
	}
	labels := make([]string, 0, len(desired.Input.DNS))
	for label := range desired.Input.DNS {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	records := make([]model.EnvironmentRecord, 0, len(labels))
	for _, label := range labels {
		spec := desired.Input.DNS[label]
		record := model.EnvironmentRecord{
			ID:            recordIDs[label],
			EnvironmentID: desired.EnvironmentID, Label: label, Namespace: spec.Namespace,
			Address: spec.Address, ServiceName: spec.Service,
		}
		if spec.Service != "" {
			record.ZoneID = zones[spec.Zone]
			if record.ZoneID == "" {
				return nil, errs.New(
					errs.KindStateConflict,
					"Environment DNS declared Zone has no identity in its desired revision",
				)
			}
		}
		if err := record.Validate(); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// Missing effective runtime is a valid pending state for an accepted authored
// head. Its absence is compared when publishing DNS, so sealing that runtime
// concurrently cannot turn a waiting selection into a falsely current answer.
func (reader *Reader) environmentDNSRuntimeProjection(
	ctx context.Context,
	identities keyvalue.Versioned[environmentprojection.EnvironmentOwnedIdentities],
	revision int64,
) (keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection], keyvalue.Condition, error) {
	if identities.Record.RevisionID == "" {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{}, keyvalue.Condition{}, nil
	}
	key := blueprints.EnvironmentBlueprintEffectiveProjectionKey(
		identities.Record.EnvironmentID, identities.Record.RevisionID,
	)
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{}, keyvalue.Condition{},
			errs.New(errs.KindInternal, "Environment DNS effective projection read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	condition := keyvalue.Condition{Key: key}
	if read.Values[0] == nil {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{
			ReadRevision: revision,
		}, condition, nil
	}
	projection, err := environmentprojection.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{}, keyvalue.Condition{}, err
	}
	if projection.EnvironmentID != identities.Record.EnvironmentID ||
		projection.RevisionID != identities.Record.RevisionID ||
		projection.RenderGeneration != identities.Record.RenderGeneration || projection.BackingRuntime != nil {
		return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{}, keyvalue.Condition{},
			errs.New(errs.KindStateConflict, "Environment DNS runtime projection differs from its declared identities")
	}
	condition.ModRevision = read.Values[0].ModRevision
	return keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{
		Record: projection, Revision: identities.Revision, ReadRevision: revision,
	}, condition, nil
}
