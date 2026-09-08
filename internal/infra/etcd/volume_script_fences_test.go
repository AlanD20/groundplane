package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: neither a missing count nor a missing forward membership proves
// absence by itself; corrupt source accounting must never authorize deletion.
func TestVolumeScriptAbsenceRequiresBothCountAndMembership(t *testing.T) {
	for _, state := range []string{"absent", "referenced", "count-only", "membership-only", "corrupt-count"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			source := volumeScriptSource(ids.New(ids.KindVolume))
			reference := ScriptSourceReference{
				OperationID: ids.New(ids.KindOperation), ScriptExecutionID: ids.NewULID(), Source: source,
				SourceOwnerID: ids.New(ids.KindEnvironment), SourceModRevision: 1,
			}
			count := ScriptSourceCount{Source: source, ReferencedExecutionCount: 1}
			if state == "corrupt-count" {
				count.ReferencedExecutionCount = 0
			}
			countBytes, err := encodeEnvelope("script-source-count", count)
			if err != nil {
				t.Fatal(err)
			}
			memberBytes, err := encodeEnvelope("script-source-reference", reference)
			if err != nil {
				t.Fatal(err)
			}
			mutations := []Mutation{{Type: MutationPut, Key: "/test/revision", Value: []byte("1")}}
			if state != "absent" && state != "membership-only" {
				mutations = append(
					mutations,
					Mutation{Type: MutationPut, Key: scriptSourceCountKey(source), Value: countBytes},
				)
			}
			if state != "absent" && state != "count-only" {
				mutations = append(mutations, Mutation{
					Type: MutationPut, Key: scriptSourceForwardReferenceKey(reference), Value: memberBytes,
				})
			}
			if _, err := store.Transact(ctx, nil, mutations); err != nil {
				t.Fatal(err)
			}
			before := store.revision
			conditions, err := prepareVolumeScriptAbsence(ctx, store, source.VolumeID, 0)
			if store.revision != before {
				t.Fatal("absence proof changed source records")
			}
			switch state {
			case "absent":
				if err != nil || len(conditions) != 2 || conditions[0].Key != scriptSourceCountKey(source) ||
					!conditions[1].Prefix || conditions[0].ModRevision != 0 || conditions[1].ModRevision != 0 {
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
