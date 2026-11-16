package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// stageBlueprintAcceptanceRecords stages immutable records only. No Task,
// desired head, resource selection or name ownership becomes active here.
// One create-only seal binds their bytes; acceptance compares that seal along
// with the original descriptor, head, epoch and operation-owned conditions.
func (repository *EnvironmentBlueprintRepository) stageBlueprintAcceptanceRecords(
	ctx context.Context, input BlueprintAuthoredPublication, change workqueue.Change,
) (workqueue.Change, error) {
	claim := input.Claim
	taskGuard, err := blueprintStagingTaskCondition(ctx, repository.store, input.Parent)
	if err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(claim.EnvironmentID, claim.RevisionID),
		blueprintPrerequisiteAuthorityKey(claim.EnvironmentID, claim.RevisionID),
		blueprintunits.CapturedGraphKey(claim.EnvironmentID, claim.TaskID),
	}
	sealKey := blueprints.EnvironmentBlueprintRevisionPrefixFinal(
		claim.EnvironmentID,
		claim.RevisionID,
	) + "acceptance-seal"
	guards := []keyvalue.Condition{
		{Key: blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID)},
		{Key: blueprints.EnvironmentBlueprintHeadKey(claim.EnvironmentID), ModRevision: input.ExpectedHeadRevision},
		{Key: blueprintunits.EpochKey(claim.EnvironmentID), ModRevision: input.Baseline.EpochRevision},
		taskGuard,
	}
	var captures []keyvalue.Mutation
	for _, key := range keys {
		index := slices.IndexFunc(
			change.Mutations,
			func(mutation keyvalue.Mutation) bool { return mutation.Key == key },
		)
		if index < 0 || change.Mutations[index].Type != keyvalue.MutationPut || change.Mutations[index].Prefix ||
			!slices.Contains(change.Conditions, keyvalue.Condition{Key: key}) {
			return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint acceptance capture is not create-only")
		}
		captures = append(captures, change.Mutations[index])
	}
	for _, condition := range change.Conditions {
		if condition.Key == guards[0].Key {
			guards[0] = condition
		}
	}
	if guards[0].ModRevision <= 0 {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint acceptance capture has no staging owner")
	}
	digest := sha256.New()
	for _, capture := range captures {
		_, _ = digest.Write([]byte(capture.Key))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(capture.Value)
		_, _ = digest.Write([]byte{0})
	}
	seal := []byte(hex.EncodeToString(digest.Sum(nil)))
	defer clear(seal)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: append(slices.Clone(keys), sealKey)})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys)+1 {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint acceptance capture read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if value := read.Values[len(keys)]; value != nil && value.Key != sealKey {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint acceptance seal read differs from its key")
	}
	sealRevision := keyvalue.RevisionOf(read.Values[len(keys)])
	if sealRevision > 0 {
		for index, capture := range captures {
			value := read.Values[index]
			if value == nil || value.Key != capture.Key || value.ModRevision != sealRevision ||
				!bytes.Equal(value.Value, capture.Value) {
				return workqueue.Change{}, errs.New(
					errs.KindStateConflict,
					"Blueprint acceptance capture differs from its seal",
				)
			}
		}
		if !bytes.Equal(read.Values[len(keys)].Value, seal) {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Blueprint acceptance seal differs from captured input",
			)
		}
	} else {
		conditions := slices.Clone(guards)
		for index, key := range keys {
			if read.Values[index] != nil {
				return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint acceptance capture has no seal")
			}
			conditions = append(conditions, keyvalue.Condition{Key: key})
		}
		conditions = append(conditions, keyvalue.Condition{Key: sealKey})
		mutations := append(slices.Clone(captures), keyvalue.Mutation{Type: keyvalue.MutationPut, Key: sealKey, Value: seal})
		budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !budget.Fits() {
			return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint immutable acceptance capture exceeds the storage limit")
		}
		result, err := repository.store.Transact(ctx, conditions, mutations)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !result.Succeeded || result.Revision <= 0 {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint acceptance staging changed before capture")
		}
		sealRevision = result.Revision
	}
	change.Conditions = slices.DeleteFunc(change.Conditions, func(condition keyvalue.Condition) bool {
		return slices.Contains(keys, condition.Key)
	})
	change.Mutations = slices.DeleteFunc(change.Mutations, func(mutation keyvalue.Mutation) bool {
		return slices.Contains(keys, mutation.Key)
	})
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: sealKey, ModRevision: sealRevision})
	return change, nil
}
