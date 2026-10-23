package scriptsourcepublication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type retainedSourceStore struct {
	sourceStore
	value *keyvalue.KeyValue
}

func (store retainedSourceStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if request.Revision != 0 && request.Revision < 100 {
		return nil, errs.New(errs.KindCursorExpired, "requested history has been compacted")
	}
	var value *keyvalue.KeyValue
	if store.value != nil {
		copied := *store.value
		copied.Value = append([]byte(nil), copied.Value...)
		value = &copied
	}
	return &keyvalue.GetManyResult{ReadRevision: 100, Values: []*keyvalue.KeyValue{value}}, nil
}

// An unchanged immutable body must remain usable after its original write
// revision is compacted. A replaced, missing or wrong-digest source still fails.
func TestRetainedScriptSourceSurvivesCompactionWithoutAcceptingReplacement(t *testing.T) {
	scriptID, environmentID := ids.New(ids.KindScript), ids.New(ids.KindEnvironment)
	body := "echo ready\n"
	sum := sha256.Sum256([]byte(body))
	generation := scripts.BodyGenerationRecord{ScriptID: scriptID, Generation: 1,
		Body: body, BodySize: uint32(len(body)), BodySHA256: hex.EncodeToString(sum[:])}
	encoded, err := scripts.EncodeScriptBodyGeneration(generation)
	if err != nil {
		t.Fatal(err)
	}
	key := scripts.ScriptSetBodyGenerationKey(environmentID, "generation", scriptID, 1)
	member := ref.Member{Mode: ref.EvidenceExisting, SourceKey: key, Reference: ref.Reference{
		SourceModRevision: 7, SourceDigest: generation.BodySHA256, SourceOwnerID: environmentID,
		Source: ref.SourceIdentity{Kind: ref.SourceBody, EnvironmentID: environmentID,
			ScriptSetGeneration: "generation", ScriptID: scriptID, BodyGeneration: 1},
	}}
	for _, test := range []struct {
		name                            string
		revision                        int64
		missing, wrongDigest, wantError bool
	}{
		{"retained old write", 7, false, false, false},
		{"replaced write", 8, false, false, true},
		{"deleted", 0, true, false, true},
		{"wrong digest", 7, false, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := retainedSourceStore{
				value: &keyvalue.KeyValue{Key: key, ModRevision: test.revision, Value: encoded},
			}
			if test.missing {
				store.value = nil
			}
			candidate := member
			if test.wrongDigest {
				candidate.Reference.SourceDigest = hex.EncodeToString(make([]byte, 32))
			}
			err := (&Authority{store: store}).validateExistingSource(context.Background(), candidate)
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v wantError=%t", err, test.wantError)
			}
		})
	}
}
