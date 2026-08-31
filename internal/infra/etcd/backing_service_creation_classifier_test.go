package etcd

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestClassifyBackingServiceCreationUsesCurrentConditionLayout(t *testing.T) {
	publication := environmentBlueprintPublicationEvidence{
		rootRevision: 11, descriptorRevision: 12, locatorRevision: 13,
	}
	values := make([]*KeyValue, 30)
	values[5] = &KeyValue{ModRevision: publication.rootRevision}
	values[6] = &KeyValue{ModRevision: publication.descriptorRevision}
	values[7] = &KeyValue{ModRevision: publication.locatorRevision}

	err := classifyBackingServiceCreation(BackingServiceCreation{}, publication, len(values))(1, values)
	if err == nil || !strings.Contains(err.Error(), "Backing-service creation raced") {
		t.Fatalf("classification error = %v, want final race classification", err)
	}
}

// Rationale: PostgreSQL backing creation used to publish two catalog
// Components, adding six compares and seven mutations to the exact plan.
func TestPostgreSQLBackingServiceCreationTransactionBudget(t *testing.T) {
	t.Parallel()
	marker := testDirectMarker()
	corrected := &idempotencyMutationPlan{
		conditions: make([]Condition, 43),
		mutations:  make([]Mutation, 44),
	}
	if got := environmentBlueprintTransactionOperationCount(corrected, marker); got != 90 {
		t.Fatalf("corrected PostgreSQL backing shape operation count = %d, want 90", got)
	}
	if err := validateEnvironmentDesiredPublicationBudget(corrected, marker); err != nil {
		t.Fatalf("corrected PostgreSQL backing shape rejected: %v", err)
	}

	legacy := &idempotencyMutationPlan{
		conditions: make([]Condition, 49),
		mutations:  make([]Mutation, 51),
	}
	if got := environmentBlueprintTransactionOperationCount(legacy, marker); got != 103 {
		t.Fatalf("legacy PostgreSQL backing shape operation count = %d, want 103", got)
	}
	if err := validateEnvironmentDesiredPublicationBudget(legacy, marker); err == nil ||
		!strings.Contains(err.Error(), "96 compare-and-mutation limit") {
		t.Fatalf("legacy PostgreSQL backing shape error = %v, want transaction-limit rejection", err)
	}
}

func TestValidateBackingServiceComponentsRejectsNonEmpty(t *testing.T) {
	t.Parallel()
	err := validateBackingServiceComponents([]ComponentRecord{{}})
	if err == nil {
		t.Fatal("validateBackingServiceComponents() accepted non-empty Components")
	}
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed ||
		!strings.Contains(err.Error(), "zero Environment Components") {
		t.Fatalf("validateBackingServiceComponents() error = %v, want validation rejection", err)
	}
}
