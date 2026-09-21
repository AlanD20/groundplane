package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type connectorSecretReferenceFence struct {
	conditions []etcdstore.Condition
}

type connectorSecretCandidate struct {
	reference     string
	projectIndex  *etcdstore.KeyValue
	platformIndex *etcdstore.KeyValue
}

func (repository *ConnectorRepository) loadConnectorSecretReferenceFence(
	ctx context.Context,
	projectID string,
	record connectorrecord.Record,
) (connectorSecretReferenceFence, error) {
	references := connectorSecretReferences(record)
	if len(references) == 0 {
		return connectorSecretReferenceFence{}, nil
	}
	indexKeys := make([]string, 0, len(references)*2)
	for _, reference := range references {
		indexKeys = append(indexKeys,
			secretrecord.SecretKeyIndexKey(core.SecretScopeProject, projectID, reference),
			secretrecord.SecretKeyIndexKey(core.SecretScopePlatform, "", reference),
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: indexKeys})
	if err != nil {
		return connectorSecretReferenceFence{}, err
	}
	if indexes == nil || indexes.ReadRevision <= 0 || len(indexes.Values) != len(indexKeys) {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindInternal,
			"Connector credential Secret index evidence is incomplete",
		)
	}
	defer etcdstore.ClearValues(indexes.Values)
	candidates := make([]connectorSecretCandidate, len(references))
	candidateKeys := make([]string, 0, len(references)*6)
	for index, reference := range references {
		projectIndex := indexes.Values[index*2]
		platformIndex := indexes.Values[index*2+1]
		if err := validateConnectorSecretIndex(indexKeys[index*2], projectIndex); err != nil {
			return connectorSecretReferenceFence{}, err
		}
		if err := validateConnectorSecretIndex(indexKeys[index*2+1], platformIndex); err != nil {
			return connectorSecretReferenceFence{}, err
		}
		candidates[index] = connectorSecretCandidate{
			reference: reference, projectIndex: projectIndex, platformIndex: platformIndex,
		}
		for _, selected := range []*etcdstore.KeyValue{projectIndex, platformIndex} {
			if selected == nil {
				continue
			}
			secretID := string(selected.Value)
			candidateKeys = append(candidateKeys,
				secretrecord.RecordKey(secretID),
				deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), secretID),
				secretrecord.ValueKey(secretID),
			)
		}
	}
	if len(candidateKeys) == 0 {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindSecretNotFound,
			"Connector credential Secret was not found in scope",
		)
	}
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: candidateKeys, Revision: indexes.ReadRevision,
	})
	if err != nil {
		return connectorSecretReferenceFence{}, err
	}
	if values == nil || values.ReadRevision != indexes.ReadRevision || len(values.Values) != len(candidateKeys) {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindInternal,
			"Connector credential Secret evidence is incomplete",
		)
	}
	defer etcdstore.ClearValues(values.Values)
	fence := connectorSecretReferenceFence{conditions: make([]etcdstore.Condition, 0, len(candidateKeys)+len(indexKeys))}
	offset := 0
	for _, candidate := range candidates {
		projectIndexKey := secretrecord.SecretKeyIndexKey(core.SecretScopeProject, projectID, candidate.reference)
		fence.conditions = append(
			fence.conditions,
			connectorSecretIndexCondition(projectIndexKey, candidate.projectIndex),
		)
		selected, selectedConditions, consumed, selectedErr := selectConnectorCredentialSecret(
			candidate.reference,
			projectID,
			core.SecretScopeProject,
			candidate.projectIndex,
			values.Values[offset:],
		)
		offset += consumed
		fence.conditions = append(fence.conditions, selectedConditions...)
		if selectedErr != nil {
			return connectorSecretReferenceFence{}, selectedErr
		}
		if selected {
			if candidate.platformIndex != nil {
				offset += 3
			}
			continue
		}
		platformIndexKey := secretrecord.SecretKeyIndexKey(core.SecretScopePlatform, "", candidate.reference)
		fence.conditions = append(
			fence.conditions,
			connectorSecretIndexCondition(platformIndexKey, candidate.platformIndex),
		)
		selected, selectedConditions, consumed, selectedErr = selectConnectorCredentialSecret(
			candidate.reference,
			projectID,
			core.SecretScopePlatform,
			candidate.platformIndex,
			values.Values[offset:],
		)
		offset += consumed
		fence.conditions = append(fence.conditions, selectedConditions...)
		if selectedErr != nil {
			return connectorSecretReferenceFence{}, selectedErr
		}
		if !selected {
			return connectorSecretReferenceFence{}, errs.New(
				errs.KindSecretNotFound,
				"Connector credential Secret was not found in scope",
			)
		}
	}
	if offset != len(values.Values) {
		return connectorSecretReferenceFence{}, errs.New(
			errs.KindInternal,
			"Connector credential Secret evidence is inconsistent",
		)
	}
	return fence, nil
}
