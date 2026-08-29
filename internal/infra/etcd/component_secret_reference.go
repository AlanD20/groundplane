package etcd

import (
	"context"
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

func componentCloudflareSecretID(record ComponentRecord) (string, bool, error) {
	if record.Desired.Kind != core.ComponentKindEdgeCloudflare {
		return "", false, nil
	}
	if record.Desired.Config.CloudflareTunnel == nil {
		if record.Desired.Enabled {
			return "", false, errs.New(
				errs.KindValidationFailed,
				"enabled Cloudflare Tunnel Component requires secret_id",
			)
		}
		return "", false, nil
	}
	secretID := record.Desired.Config.CloudflareTunnel.SecretID
	if ids.Validate(ids.KindSecret, secretID) != nil {
		return "", false, errs.New(
			errs.KindValidationFailed,
			"Cloudflare Tunnel Component secret_id is invalid",
		)
	}
	return secretID, true, nil
}

func prepareComponentTaskSecretReferences(
	ctx context.Context,
	store hierarchyStore,
	projectID string,
	taskID string,
	candidates []ComponentTaskCandidate,
	revision int64,
) ([]componentTaskSecretReference, []Condition, []Mutation, error) {
	references := make([]componentTaskSecretReference, 0, 1)
	for _, candidate := range candidates {
		secretID, present, err := componentCloudflareSecretID(candidate.Candidate)
		if err != nil {
			return nil, nil, nil, err
		}
		if present {
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
	stored, err := store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, err
	}
	if stored == nil || len(stored.Values) != len(keys) {
		return nil, nil, nil, errs.New(errs.KindInternal, "Component Secret reference read is incomplete")
	}
	conditions := make([]Condition, 0, len(references)*3)
	mutations := make([]Mutation, 0, len(references))
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
			Condition{Key: secretRecordKey(references[index].secretID), ModRevision: recordValue.ModRevision},
			Condition{Key: deletionTombstoneKey(string(DeletionTargetSecret), references[index].secretID)},
			Condition{Key: candidateKey},
		)
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: candidateKey, Value: []byte(references[index].componentID),
		})
	}
	return references, conditions, mutations, nil
}

func componentTaskTerminalSecretMutations(
	intent ComponentTaskIntent,
	taskID string,
	terminalStatus TaskStatus,
) ([]Mutation, error) {
	mutations := make([]Mutation, 0, len(intent.Candidates)*3)
	for _, candidate := range intent.Candidates {
		currentID, currentPresent, err := componentCloudflareSecretID(candidate.Current)
		if err != nil {
			return nil, err
		}
		nextID, nextPresent, err := componentCloudflareSecretID(candidate.Candidate)
		if err != nil {
			return nil, err
		}
		if nextPresent {
			mutations = append(mutations, Mutation{
				Type: MutationDelete,
				Key:  componentCandidateSecretReferenceKey(nextID, taskID, candidate.Candidate.Desired.ID),
			})
		}
		if terminalStatus != TaskStatusCompleted || currentPresent == nextPresent && currentID == nextID {
			continue
		}
		if currentPresent {
			mutations = append(mutations, Mutation{
				Type: MutationDelete,
				Key:  componentActiveSecretReferenceKey(currentID, candidate.Current.Desired.ID),
			})
		}
		if nextPresent {
			mutations = append(mutations, Mutation{
				Type:  MutationPut,
				Key:   componentActiveSecretReferenceKey(nextID, candidate.Candidate.Desired.ID),
				Value: []byte(candidate.Candidate.Desired.ID),
			})
		}
	}
	return mutations, nil
}
