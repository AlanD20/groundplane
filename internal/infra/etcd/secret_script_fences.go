package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func secretScriptSource(secretID string) ScriptSourceIdentity {
	return ScriptSourceIdentity{Kind: ScriptSourceSecretValue, SecretID: secretID, ValueGenerationID: secretID}
}

func secretScriptAbsenceConditions(secretID string) []Condition {
	source := secretScriptSource(secretID)
	return []Condition{
		{Key: scriptSourceCountKey(source)},
		{Key: scriptSourceForwardReferencePrefix + scriptSourceSuffix(source) + "/", Prefix: true},
	}
}

func classifySecretScriptReferences(secretID string, values []*KeyValue) error {
	if len(values) != 2 || (values[0] == nil) != (values[1] == nil) {
		return errs.New(errs.KindInternal, "Secret Script reference absence proof is inconsistent")
	}
	if values[0] == nil {
		return nil
	}
	source := secretScriptSource(secretID)
	count, err := decodeScriptSourceCount(values[0].Value)
	if err != nil || values[0].Key != scriptSourceCountKey(source) || count.Source != source ||
		count.ReferencedExecutionCount == 0 {
		return errs.New(errs.KindInternal, "Secret Script reference count is corrupt")
	}
	reference, err := decodeEnvelope[ScriptSourceReference](values[1].Value, "script-source-reference")
	if err != nil || reference.Source != source || scriptSourceForwardReferenceKey(reference) != values[1].Key {
		return errs.New(errs.KindInternal, "Secret Script source membership is corrupt")
	}
	return errs.New(errs.KindResourceInUse, "Secret is referenced by a Script")
}

// Both reads share one revision, and both absence conditions remain in the
// final removal transaction so a reservation cannot race the read proof.
func prepareSecretScriptAbsence(ctx context.Context, store hierarchyStore, secretID string) ([]Condition, error) {
	conditions := secretScriptAbsenceConditions(secretID)
	count, err := store.GetMany(ctx, GetManyRequest{Keys: []string{conditions[0].Key}})
	if err != nil {
		return nil, err
	}
	if count == nil || len(count.Values) != 1 || count.ReadRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "Secret Script count evidence is incomplete")
	}
	members, err := store.Range(ctx, RangeRequest{Prefix: conditions[1].Key, Limit: 1, Revision: count.ReadRevision})
	if err != nil {
		return nil, err
	}
	if members == nil || members.ReadRevision != count.ReadRevision || len(members.Values) > 1 ||
		(members.More && len(members.Values) == 0) {
		return nil, errs.New(errs.KindInternal, "Secret Script membership evidence is incomplete")
	}
	var first *KeyValue
	if len(members.Values) == 1 {
		first = &members.Values[0]
	}
	if err := classifySecretScriptReferences(secretID, []*KeyValue{count.Values[0], first}); err != nil {
		return nil, err
	}
	return conditions, nil
}
