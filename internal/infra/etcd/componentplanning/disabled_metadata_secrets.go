package componentplanning

import (
	"context"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDisabledComponentSecretReplacement changes active metadata references
// with a proven-disabled Component publication. It never changes Task/recovery
// pins or reads plaintext; the caller owns atomic runtime-absence proof.
func PrepareDisabledComponentSecretReplacement(ctx context.Context, store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}, projectID string, current, candidate componentrecord.Record, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if store == nil || revision <= 0 || componentrecord.ValidateRecord(current) != nil ||
		componentrecord.ValidateRecord(candidate) != nil || current.Desired.Enabled || candidate.Desired.Enabled ||
		current.Desired.ID != candidate.Desired.ID || current.Desired.Owner != candidate.Desired.Owner ||
		current.Desired.OwnerID != candidate.Desired.OwnerID || !current.Desired.Kind.SharesEnvironmentCapability(candidate.Desired.Kind) ||
		len(current.Runtime.GeneratedServices) != 0 || len(candidate.Runtime.GeneratedServices) != 0 ||
		current.Runtime.PinnedIPv4 != "" || candidate.Runtime.PinnedIPv4 != "" ||
		current.Runtime.Healthy || candidate.Runtime.Healthy {
		return nil, nil, errs.New(errs.KindValidationFailed, "disabled Component Secret replacement is invalid")
	}
	oldIDs, err := componentrecord.SecretReferences(current)
	if err != nil {
		return nil, nil, err
	}
	nextIDs, err := componentrecord.SecretReferences(candidate)
	if err != nil {
		return nil, nil, err
	}
	allIDs := append(slices.Clone(oldIDs), nextIDs...)
	sort.Strings(allIDs)
	allIDs = slices.Compact(allIDs)
	if len(allIDs) == 0 {
		return nil, nil, nil
	}
	keys := make([]string, 0, len(allIDs)+len(nextIDs)*2)
	for _, id := range allIDs {
		keys = append(keys, componentActiveSecretReferenceKey(id, current.Desired.ID))
	}
	for _, id := range nextIDs {
		keys = append(
			keys,
			secretrecord.RecordKey(id),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), id),
		)
	}
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, errs.New(errs.KindInternal, "disabled Component Secret references are unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return nil, nil, errs.New(errs.KindInternal, "disabled Component Secret reference read is corrupt")
		}
		conditions = append(conditions, etcdstore.Condition{Key: keys[index], ModRevision: etcdstore.RevisionOf(value)})
	}
	mutations := make([]etcdstore.Mutation, 0, len(allIDs))
	for index, id := range allIDs {
		value := read.Values[index]
		if value != nil && string(value.Value) != current.Desired.ID {
			return nil, nil, errs.New(errs.KindStateConflict, "disabled Component Secret reference changed")
		}
		mutation := etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[index]}
		if slices.Contains(nextIDs, id) {
			mutation.Type = etcdstore.MutationPut
			mutation.Value = []byte(current.Desired.ID)
		}
		mutations = append(mutations, mutation)
	}
	for index, id := range nextIDs {
		value := read.Values[len(allIDs)+index*2]
		if value == nil {
			return nil, nil, errs.New(errs.KindSecretNotFound, "Component Secret was not found")
		}
		record, decodeErr := secretrecord.DecodeRecord(value.Value)
		if decodeErr != nil || record.Secret.ID != id || record.Secret.Kind != core.SecretKindEnvVar ||
			(record.Secret.Scope == core.SecretScopeProject && record.Secret.ProjectID != projectID) ||
			(record.Secret.Scope != core.SecretScopeProject && record.Secret.Scope != core.SecretScopePlatform) {
			return nil, nil, errs.New(errs.KindValidationFailed, "Component Secret is unavailable in this Project")
		}
		if read.Values[len(allIDs)+index*2+1] != nil {
			return nil, nil, errs.New(errs.KindResourceInUse, "Component Secret deletion is in progress")
		}
	}
	return conditions, mutations, nil
}
