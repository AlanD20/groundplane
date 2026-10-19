package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Apply already published the desired omission. Completion retires only the
// captured runtime and its resource references; it never rewrites the Blueprint
// head or adopts newer desired state.
func (repository *ServiceRepository) prepareBlueprintServiceRemovalState(
	ctx context.Context,
	inputs *serviceremovalwork.Repository,
	input serviceremovalwork.Input,
	view workqueue.View,
) (workqueue.Change, error) {
	conditions, err := inputs.RuntimeGuards(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	desired, err := inputs.DesiredGuard(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(input.Scope.EnvironmentID, input.DesiredRevisionID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint removal identities are unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint removal identity snapshot is incomplete")
	}
	owned, err := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if owned.EnvironmentID != input.Scope.EnvironmentID || owned.RevisionID != input.DesiredRevisionID ||
		owned.RenderGeneration != input.Generation {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Blueprint removal belongs to another captured revision",
		)
	}
	for _, service := range owned.Services {
		if service.ID == input.ServiceID {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Blueprint still declares the Service selected for removal",
			)
		}
	}
	omitted := false
	for _, omission := range owned.PendingOmissions {
		omitted = omitted || omission.Kind == ids.KindService && omission.ID == input.ServiceID
	}
	if !omitted {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Blueprint has no captured omission for this Service",
		)
	}
	dns, err := dnsrecords.NewReader(repository.store).
		RequireDNSUnreferenced(ctx, input.ServiceID, "", view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	scripts, err := scriptsourceevidence.PrepareServiceScriptAbsence(
		ctx,
		repository.store,
		input.ServiceID,
		view.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	references, err := repository.prepareBlueprintRemovalReferences(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	addresses, err := networkreservations.PrepareProxyAddressRelease(
		ctx,
		repository.store,
		input.ServiceID,
		view.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	keep := false
	defer func() {
		if !keep {
			addresses.Clear()
		}
	}()
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, input.Scope.EnvironmentID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if !keep {
			clear(epoch.Value)
		}
	}()
	receipt := workqueue.Change{}
	conditions = append(conditions, desired, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	conditions = append(conditions, dns...)
	conditions = append(conditions, scripts...)
	conditions = append(conditions, references...)
	conditions = append(conditions, addresses.Conditions()...)
	conditions = append(conditions, fence.TransactionConditions()...)
	receipt.Conditions = append(receipt.Conditions, conditions...)
	receipt.Mutations = append(receipt.Mutations, addresses.Mutations()...)
	receipt.Mutations = append(
		receipt.Mutations,
		epoch,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: services.ServiceRuntimeKey(input.ServiceID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: serviceruntimerecord.Key(input.ServiceID)},
		keyvalue.Mutation{
			Type: keyvalue.MutationDelete,
			Key:  deletions.TombstoneKey(string(deletions.DeletionTargetService), input.ServiceID),
		},
	)
	keep = true
	return receipt, nil
}
