package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testsecretmutations "github.com/AlanD20/groundplane/internal/infra/etcd/secretmutations"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: neither a missing count nor a missing forward membership proves
// absence by itself; corrupt source accounting must never authorize deletion.
func TestSecretScriptAbsenceRequiresBothCountAndMembership(t *testing.T) {
	for _, state := range []string{"absent", "referenced", "count-only", "membership-only", "corrupt-count"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			source := testscriptsourcereference.SourceIdentity{
				Kind:     testscriptsourcereference.SourceSecretValue,
				SecretID: ids.New(ids.KindSecret),
			}
			source.ValueGenerationID = source.SecretID
			reference := testscriptsourcereference.Reference{
				OperationID: ids.New(
					ids.KindOperation,
				), ScriptExecutionID: ids.NewULID(), Source: source,
				SourceOwnerID: testscriptsourceevidence.ScriptSourcePlatformOwner, SourceModRevision: 1,
			}
			count := testscriptsourcereference.Count{Source: source, ReferencedExecutionCount: 1}
			if state == "corrupt-count" {
				count.ReferencedExecutionCount = 0
			}
			countBytes, err := testrecordcodec.Encode("script-source-count", count)
			if err != nil {
				t.Fatal(err)
			}
			memberBytes, err := testrecordcodec.Encode("script-source-reference", reference)
			if err != nil {
				t.Fatal(err)
			}
			mutations := []testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: "/test/revision", Value: []byte("1")},
			}
			if state != "absent" && state != "membership-only" {
				mutations = append(
					mutations, testkeyvalue.Mutation{
						Type:  testkeyvalue.MutationPut,
						Key:   testscriptsourceevidence.ScriptSourceCountKey(source),
						Value: countBytes,
					},
				)
			}
			if state != "absent" && state != "count-only" {
				mutations = append(mutations, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationPut, Key: testscriptsourceevidence.ScriptSourceForwardReferenceKey(reference), Value: memberBytes,
				})
			}
			if _, err := store.Transact(ctx, nil, mutations); err != nil {
				t.Fatal(err)
			}
			before := store.revision
			conditions, err := testsecretmutations.PrepareSecretScriptAbsence(ctx, store, source.SecretID)
			if store.revision != before {
				t.Fatal("absence proof changed source records")
			}
			switch state {
			case "absent":
				if err != nil || len(conditions) != 3 || conditions[0].Key != testscriptsourceevidence.ScriptSourceCountKey(source) ||
					!conditions[1].Prefix ||
					conditions[0].ModRevision != 0 ||
					conditions[1].ModRevision != 0 ||
					conditions[2].Key != tasksecretpinrecord.SecretPrefix(source.SecretID) ||
					!conditions[2].Prefix ||
					conditions[2].ModRevision != 0 {
					t.Fatalf("absence did not retain both transactional fences: %v", err)
				}
			case "referenced":
				if !isKind(err, errs.KindResourceInUse) {
					t.Fatalf("active reference = %v", err)
				}
			default:
				if !isKind(err, errs.KindInternal) || conditions != nil {
					t.Fatalf("corrupt reference evidence authorized deletion: %v", err)
				}
			}
		})
	}
}
