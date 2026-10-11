package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentDesiredIdentityPublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func prepareEnvironmentDesiredIdentityPublication(
	ctx context.Context,
	store hierarchyStore,
	projection projectionrecord.EnvironmentComposeProjection,
	sourceKind blueprints.EnvironmentBlueprintSourceKind,
	expectedHeadRevision int64,
	readRevision int64,
) (environmentDesiredIdentityPublication, error) {
	dnsConditions, err := dnsrecords.NewReader(store).ProtectDNSProjection(ctx, projection, readRevision)
	if err != nil {
		return environmentDesiredIdentityPublication{}, err
	}
	nameConditions, nameMutations, err := dnsrecords.NewReader(store).
		PrepareRouteNamePublication(ctx, projection, readRevision)
	if err != nil {
		return environmentDesiredIdentityPublication{}, err
	}
	defer etcdstore.ClearMutationValues(nameMutations)
	if expectedHeadRevision < 0 {
		return environmentDesiredIdentityPublication{}, errs.New(
			errs.KindValidationFailed,
			"Environment desired identity predecessor is invalid",
		)
	}
	ownedIdentities, err := projectionrecord.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		return environmentDesiredIdentityPublication{}, err
	}
	if expectedHeadRevision != 0 {
		previous, found, readErr := blueprints.ReadCurrentOwnedIdentities(
			ctx,
			store,
			projection.EnvironmentID,
			readRevision,
		)
		if readErr != nil {
			return environmentDesiredIdentityPublication{}, readErr
		}
		if !found || previous.Revision != expectedHeadRevision {
			return environmentDesiredIdentityPublication{}, errs.New(
				errs.KindStateConflict,
				"Environment desired identity predecessor changed",
			)
		}
		if sourceKind == blueprints.EnvironmentBlueprintSourceMutation {
			ownedIdentities.Attaches = append([]projectionrecord.OwnedIdentity(nil), previous.Record.Attaches...)
			ownedIdentities.Scripts = append([]projectionrecord.OwnedIdentity(nil), previous.Record.Scripts...)
			ownedIdentities.DNSRecords = append([]projectionrecord.OwnedIdentity(nil), previous.Record.DNSRecords...)
		}
		ownedIdentities, err = projectionrecord.RetainOwnedIdentityBirths(ownedIdentities, previous.Record)
		if err != nil {
			return environmentDesiredIdentityPublication{}, err
		}
	}
	effectiveValue, err := projectionrecord.EncodePreparedEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		return environmentDesiredIdentityPublication{}, err
	}
	identityValue, err := projectionrecord.EncodeEnvironmentOwnedIdentities(ownedIdentities)
	if err != nil {
		clear(effectiveValue)
		return environmentDesiredIdentityPublication{}, err
	}
	initialAbsence, err := blueprintunits.PrepareInitialAbsencePublication(
		projection.EnvironmentID,
		projection.RevisionID,
		newbornBlueprintUnitTargets(ownedIdentities, projection.RevisionID),
	)
	if err != nil {
		clear(effectiveValue)
		clear(identityValue)
		return environmentDesiredIdentityPublication{}, err
	}
	effectiveKey := blueprints.EnvironmentBlueprintEffectiveProjectionKey(
		projection.EnvironmentID,
		projection.RevisionID,
	)
	identityKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(
		projection.EnvironmentID,
		projection.RevisionID,
	)
	publication := environmentDesiredIdentityPublication{
		conditions: []etcdstore.Condition{{Key: effectiveKey}, {Key: identityKey}},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: effectiveKey, Value: effectiveValue},
			{Type: etcdstore.MutationPut, Key: identityKey, Value: identityValue},
		},
	}
	publication.conditions = append(publication.conditions, initialAbsence.Conditions()...)
	publication.conditions = append(publication.conditions, dnsConditions...)
	publication.conditions = append(publication.conditions, nameConditions...)
	for _, mutation := range nameMutations {
		publication.mutations = append(publication.mutations, etcdstore.Mutation{
			Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...),
		})
	}
	publication.mutations = append(publication.mutations, initialAbsence.Mutations()...)
	return publication, nil
}

func (publication environmentDesiredIdentityPublication) clear() {
	etcdstore.ClearMutationValues(publication.mutations)
}
