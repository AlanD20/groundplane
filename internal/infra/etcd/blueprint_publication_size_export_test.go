package etcd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintTransactionSize struct {
	Comparisons int
	Mutations   int
	Bytes       int
}

type BlueprintPublicationSizeAudit struct {
	Publication BlueprintTransactionSize
	Assignment  BlueprintTransactionSize
}

func (fixture *ExecutedArtifactFixture) AuditBlueprintPublicationSize() *BlueprintPublicationSizeAudit {
	audit := &BlueprintPublicationSizeAudit{}
	fixture.publicationSize = audit
	wrapped := &blueprintSizeOrdinaryStore{Store: fixture.store, audit: audit}
	fixture.Tasks.store = wrapped
	fixture.Ledger.store = wrapped
	return audit
}

type blueprintSizeOrdinaryStore struct {
	Store
	audit *BlueprintPublicationSizeAudit
}

func (wrapped *blueprintSizeOrdinaryStore) Transact(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return TransactionResult{}, errs.New(errs.KindValidationFailed, "fixture exceeded ordinary transaction bound")
	}
	shape, err := blueprintPhysicalTransactionSize(conditions, mutations)
	if err != nil {
		return TransactionResult{}, err
	}
	for _, mutation := range mutations {
		if mutation.Type == MutationPut && strings.HasPrefix(mutation.Key, taskAssignmentRootPrefix) {
			wrapped.audit.Assignment = shape
		}
	}
	return wrapped.Store.Transact(ctx, conditions, mutations)
}

type blueprintSizePublicationStore struct {
	hierarchyStore
	audit *BlueprintPublicationSizeAudit
}

func (wrapped blueprintSizePublicationStore) TransactEnvironmentBlueprint(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	if wrapped.audit != nil {
		if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
			return TransactionResult{}, err
		}
		shape, err := blueprintPhysicalTransactionSize(conditions, mutations)
		if err != nil {
			return TransactionResult{}, err
		}
		wrapped.audit.Publication = shape
	}
	return wrapped.hierarchyStore.Transact(ctx, conditions, mutations)
}

func blueprintPhysicalTransactionSize(conditions []Condition, mutations []Mutation) (BlueprintTransactionSize, error) {
	wire := &store{root: "/groundplane"}
	prepared, err := wire.prepareTransaction(conditions, mutations)
	if err != nil {
		return BlueprintTransactionSize{}, err
	}
	physicalMutations := make([]string, len(mutations))
	for index, mutation := range mutations {
		physicalMutations[index], err = wire.physicalKey(mutation.Key)
		if err != nil {
			return BlueprintTransactionSize{}, err
		}
	}
	request := transactionRequest(conditions, mutations, prepared.physicalConditions, physicalMutations)
	return BlueprintTransactionSize{
		Comparisons: len(request.Compare),
		Mutations:   len(request.Success),
		Bytes:       request.Size(),
	}, nil
}

func (fixture *ExecutedArtifactFixture) AssertBlueprintPublicationRecordSizes(
	t *testing.T, task TaskRecord, claim TaskAssignment,
) {
	t.Helper()
	ctx := context.Background()
	publication := task.Params[TaskReleasePublicationParam]
	read, err := fixture.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releasePublicationKey(publication), releaseManifestStagingKey(publication),
		taskAssignmentKey(claim.Assignment.Record.AgentID, task.ID),
	}})
	if err != nil || len(read.Values) != 3 || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		t.Fatalf("size evidence: %v", err)
	}
	markerBytes := len(read.Values[0].Value)
	if markerBytes > maximumReleaseRenderInputBytes ||
		bytes.Contains(read.Values[0].Value, []byte(`"normalized_compose"`)) ||
		bytes.Contains(read.Values[0].Value, []byte(`"current_artifact"`)) {
		t.Fatal("marker embeds bulk native recovery inputs")
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil {
		t.Fatal(err)
	}
	largestInput := 0
	for _, member := range manifest.Members {
		stored, err := fixture.store.Get(ctx, releaseRenderInputStagingKey(publication, member.ReleaseID))
		if err != nil || stored.Entry == nil || len(stored.Entry.Value) > maximumReleaseRenderInputBytes {
			t.Fatalf("bounded member input: %v", err)
		}
		largestInput = max(largestInput, len(stored.Entry.Value))
	}
	authority := claim.Assignment.Record.RestorationAuthority
	nativeBytes := 0
	for _, native := range authority.NativePredecessors {
		nativeBytes += len(native.CurrentArtifact) + len(native.RetainedPriorArtifact)
	}
	if nativeBytes > MaximumTaskRecordBytes || len(read.Values[2].Value) > MaximumTaskRecordBytes {
		t.Fatal("native assignment exceeds its existing durable bound")
	}
	t.Logf("marker=%d largest-render=%d native-witness=%d assignment-record=%d publication=%+v claim=%+v",
		markerBytes, largestInput, nativeBytes, len(read.Values[2].Value),
		fixture.publicationSize.Publication, fixture.publicationSize.Assignment)
}
