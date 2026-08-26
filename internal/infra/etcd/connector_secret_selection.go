package etcd

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func connectorSecretReferences(record ConnectorRecord) []string {
	unique := make(map[string]struct{}, len(record.Connector.Credentials))
	for _, credential := range record.Connector.Credentials {
		if credential.Kind == core.ConnectorCredentialSecretRef {
			unique[credential.SecretRef] = struct{}{}
		}
	}
	references := make([]string, 0, len(unique))
	for reference := range unique {
		references = append(references, reference)
	}
	sort.Strings(references)
	return references
}

func validateConnectorSecretIndex(key string, value *KeyValue) error {
	if value == nil {
		return nil
	}
	if value.Key != key || value.ModRevision <= 0 || ids.Validate(ids.KindSecret, string(value.Value)) != nil {
		return errs.New(errs.KindInternal, "Connector credential Secret index is corrupt")
	}
	return nil
}

func connectorSecretIndexCondition(key string, value *KeyValue) Condition {
	if value == nil {
		return Condition{Key: key}
	}
	return Condition{Key: key, ModRevision: value.ModRevision}
}

func selectConnectorCredentialSecret(
	reference string,
	projectID string,
	scope core.SecretScope,
	index *KeyValue,
	values []*KeyValue,
) (bool, []Condition, int, error) {
	if index == nil {
		return false, nil, 0, nil
	}
	if len(values) < 3 || values[0] == nil || values[0].Key != secretRecordKey(string(index.Value)) ||
		values[2] == nil || values[2].Key != secretValueKey(string(index.Value)) {
		return false, nil, 0, errs.New(errs.KindInternal, "Connector credential Secret evidence is corrupt")
	}
	record, err := decodeSecretRecord(values[0].Value)
	if err != nil || record.Secret.ID != string(index.Value) || record.Secret.Key != reference ||
		record.Secret.Scope != scope || (scope == core.SecretScopeProject && record.Secret.ProjectID != projectID) ||
		(scope == core.SecretScopePlatform && record.Secret.ProjectID != "") {
		return false, nil, 0, corruptSecretRecord()
	}
	conditions := []Condition{{Key: values[0].Key, ModRevision: values[0].ModRevision}}
	if values[1] != nil {
		if values[1].Key != deletionTombstoneKey(string(DeletionTargetSecret), record.Secret.ID) {
			return false, nil, 0, errs.New(
				errs.KindInternal,
				"Connector credential Secret tombstone evidence is corrupt",
			)
		}
		if err := validateSecretDeletionFence(values[1], record.Secret.ID); err != nil {
			return false, nil, 0, err
		}
		conditions = append(conditions, Condition{Key: values[1].Key, ModRevision: values[1].ModRevision})
		return false, conditions, 3, nil
	}
	conditions = append(conditions, Condition{
		Key: deletionTombstoneKey(string(DeletionTargetSecret), record.Secret.ID),
	})
	value, err := decodeSecretEncryptedValue(values[2].Value)
	if err != nil {
		return false, nil, 0, corruptSecretRecord()
	}
	defer clear(value.Ciphertext)
	if err := validateSecretValueBinding(record, value); err != nil {
		return false, nil, 0, corruptSecretRecord()
	}
	if record.Secret.Kind != core.SecretKindEnvVar {
		return false, nil, 0, errs.New(
			errs.KindValidationFailed,
			"Connector credential secret_ref must name an env_var Secret",
		)
	}
	conditions = append(conditions, Condition{Key: values[2].Key, ModRevision: values[2].ModRevision})
	return true, conditions, 3, nil
}
