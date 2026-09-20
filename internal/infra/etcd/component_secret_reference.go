package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const componentSecretReferenceIndexPrefix = "/v1/indexes/secrets/by-component/"

type componentTaskSecretReference struct {
	secretID    string
	componentID string
	revision    int64
}

func componentSecretReferencePrefix(secretID string) string {
	return componentSecretReferenceIndexPrefix + secretID + "/"
}

func componentActiveSecretReferenceKey(secretID string, componentID string) string {
	return componentSecretReferencePrefix(secretID) + "active/" + componentID
}

func componentCandidateSecretReferenceKey(secretID string, taskID string, componentID string) string {
	return componentSecretReferencePrefix(secretID) + "candidates/" + taskID + "/" + componentID
}

func componentSecretReferences(record ComponentRecord) ([]string, error) {
	references := record.Desired.Config.SecretReferences()
	for _, reference := range references {
		if ids.Validate(ids.KindSecret, reference) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Component Secret reference is invalid")
		}
	}
	return references, nil
}

func prepareComponentTaskSecretReferences(
	ctx context.Context,
	store hierarchyStore,
	projectID string,
	taskID string,
	candidates []ComponentTaskCandidate,
	revision int64,
) ([]componentTaskSecretReference, []etcdstore.Condition, []etcdstore.Mutation, error) {
	references := make([]componentTaskSecretReference, 0, len(candidates))
	for _, candidate := range candidates {
		secretIDs, err := componentSecretReferences(candidate.Candidate)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, secretID := range secretIDs {
			references = append(references, componentTaskSecretReference{
				secretID: secretID, componentID: candidate.Candidate.Desired.ID,
			})
		}
	}
	sort.Slice(references, func(left int, right int) bool {
		if references[left].secretID == references[right].secretID {
			return references[left].componentID < references[right].componentID
		}
		return references[left].secretID < references[right].secretID
	})
	if len(references) == 0 {
		return nil, nil, nil, nil
	}
	keys := make([]string, 0, len(references)*2)
	for _, reference := range references {
		keys = append(
			keys,
			secretRecordKey(reference.secretID),
			deletionTombstoneKey(string(DeletionTargetSecret), reference.secretID),
		)
	}
	stored, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, err
	}
	if stored == nil || len(stored.Values) != len(keys) {
		return nil, nil, nil, errs.New(errs.KindInternal, "Component Secret reference read is incomplete")
	}
	conditions := make([]etcdstore.Condition, 0, len(references)*3)
	mutations := make([]etcdstore.Mutation, 0, len(references))
	for index := range references {
		recordValue := stored.Values[index*2]
		tombstoneValue := stored.Values[index*2+1]
		if recordValue == nil {
			return nil, nil, nil, errs.New(errs.KindSecretNotFound, "Component Secret was not found")
		}
		record, decodeErr := decodeSecretRecord(recordValue.Value)
		if decodeErr != nil || record.Secret.ID != references[index].secretID ||
			record.Secret.Kind != core.SecretKindEnvVar ||
			(record.Secret.Scope == core.SecretScopeProject && record.Secret.ProjectID != projectID) ||
			(record.Secret.Scope != core.SecretScopeProject && record.Secret.Scope != core.SecretScopePlatform) {
			return nil, nil, nil, errs.New(
				errs.KindValidationFailed,
				"Component Secret is unavailable in this Project",
			)
		}
		if tombstoneValue != nil {
			return nil, nil, nil, errs.New(errs.KindResourceInUse, "Component Secret deletion is in progress")
		}
		references[index].revision = recordValue.ModRevision
		candidateKey := componentCandidateSecretReferenceKey(
			references[index].secretID,
			taskID,
			references[index].componentID,
		)
		conditions = append(
			conditions,
			etcdstore.Condition{Key: secretRecordKey(references[index].secretID), ModRevision: recordValue.ModRevision},
			etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetSecret), references[index].secretID)},
			etcdstore.Condition{Key: candidateKey},
		)
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: candidateKey, Value: []byte(references[index].componentID),
		})
	}
	return references, conditions, mutations, nil
}

func componentTaskTerminalSecretMutations(
	intent ComponentTaskIntent,
	taskID string,
	terminalStatus TaskStatus,
) ([]etcdstore.Mutation, error) {
	mutations := make([]etcdstore.Mutation, 0, len(intent.Candidates)*3)
	for _, candidate := range intent.Candidates {
		currentIDs, err := componentSecretReferences(candidate.Current)
		if err != nil {
			return nil, err
		}
		nextIDs, err := componentSecretReferences(candidate.Candidate)
		if err != nil {
			return nil, err
		}
		for _, nextID := range nextIDs {
			mutations = append(mutations, etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  componentCandidateSecretReferenceKey(nextID, taskID, candidate.Candidate.Desired.ID),
			})
		}
		if terminalStatus != TaskStatusCompleted || equalSecretReferences(currentIDs, nextIDs) {
			continue
		}
		for _, currentID := range currentIDs {
			mutations = append(mutations, etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  componentActiveSecretReferenceKey(currentID, candidate.Current.Desired.ID),
			})
		}
		for _, nextID := range nextIDs {
			mutations = append(mutations, etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentActiveSecretReferenceKey(nextID, candidate.Candidate.Desired.ID),
				Value: []byte(candidate.Candidate.Desired.ID),
			})
		}
	}
	return mutations, nil
}

func equalSecretReferences(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
