package etcd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: source preparation may change reference counts, but the existing
// final primary comparison must not also admit a changed execution context.
func TestScriptContextPrimaryFenceAllowsCountsButRejectsContextEdit(t *testing.T) {
	store, sources, _, _, _ := manualScriptLifecycleFixture(t)
	prepared := sources.Script.Record
	prepared.ActiveReferences++
	key := testscripts.ScriptSetScriptKey(prepared.EnvironmentID, prepared.ScriptSetGeneration, prepared.Desired.ID)
	write := func(record testscripts.Record) {
		t.Helper()
		value, err := testscripts.EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		result, err := store.Transact(
			context.Background(),
			nil,
			[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}},
		)
		if err != nil || !result.Succeeded {
			t.Fatalf("prepare fixture primary: %v", err)
		}
	}
	write(prepared)
	primary, err := preparedScriptPrimary(context.Background(), store, sources)
	if err != nil || primary.Key != key || primary.ModRevision != store.revision {
		t.Fatalf("reference-count-only update rejected: %v", err)
	}
	prepared.Desired.Execution = &core.ScriptExecution{
		Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
	}
	write(prepared)
	if _, err := preparedScriptPrimary(context.Background(), store, sources); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("context edit escaped final primary comparison: %v", err)
	}
}
