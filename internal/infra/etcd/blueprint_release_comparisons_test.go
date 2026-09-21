package etcd

import (
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"testing"
)

func TestBlueprintReleaseComparisonsPreserveFrozenSourceRevisions(t *testing.T) {
	// Rationale: a new Attach and its hook share one absent-key compare; a
	// retained Attach changing revision must fail, including after a retry.
	publication := BlueprintReleasePublication{conditions: []testkeyvalue.Condition{
		{Key: "attach", ModRevision: 7}, {Key: "head", ModRevision: 9}, {Key: "head", ModRevision: 9},
	}}
	merged, err := publication.withExistingComparisons([]testkeyvalue.Condition{{Key: "attach", ModRevision: 7}})
	if err != nil || len(merged.conditions) != 1 || merged.conditions[0].Key != "head" {
		t.Fatalf("equal shared comparisons: %v, %v", merged.conditions, err)
	}
	if len(publication.conditions) != 3 {
		t.Fatal("comparison assembly mutated frozen publication")
	}
	if _, err := publication.withExistingComparisons([]testkeyvalue.Condition{{Key: "attach", ModRevision: 8}}); err == nil {
		t.Fatal("accepted changed Attach revision on publication retry")
	}
}
