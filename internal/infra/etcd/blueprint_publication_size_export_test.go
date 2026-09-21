package etcd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	testkeyvalue.Store

	audit *BlueprintPublicationSizeAudit
}

func (wrapped *blueprintSizeOrdinaryStore) Transact(
	ctx context.Context, conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if len(conditions)+len(mutations) > testkeyvalue.MaximumOperations {
		return testkeyvalue.TransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"fixture exceeded ordinary transaction bound",
		)
	}
	shape, err := blueprintPhysicalTransactionSize(conditions, mutations)
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	for _, mutation := range mutations {
		if mutation.Type == testkeyvalue.MutationPut && strings.HasPrefix(mutation.Key, "/v1/runtime/assignments/") {
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
	ctx context.Context, conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if wrapped.audit != nil {
		if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		shape, err := blueprintPhysicalTransactionSize(conditions, mutations)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		wrapped.audit.Publication = shape
	}
	return wrapped.hierarchyStore.Transact(ctx, conditions, mutations)
}

func blueprintPhysicalTransactionSize(
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (BlueprintTransactionSize, error) {
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
	publication := task.Params[testreleaserender.TaskReleasePublicationParam]
	read, err := fixture.store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testreleases.ReleasePublicationKey(publication),
				testreleases.ReleaseManifestStagingKey(publication),
				testtaskjournal.TaskAssignmentKey(claim.Assignment.Record.AgentID, task.ID),
			},
		},
	)
	if err != nil || len(read.Values) != 3 || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		t.Fatalf("size evidence: %v", err)
	}
	markerBytes := len(read.Values[0].Value)
	if markerBytes > testreleases.MaximumReleaseRenderInputBytes ||
		bytes.Contains(read.Values[0].Value, []byte(`"normalized_compose"`)) ||
		bytes.Contains(read.Values[0].Value, []byte(`"current_artifact"`)) {
		t.Fatal("marker embeds bulk native recovery inputs")
	}
	manifest, err := testreleases.DecodeReleaseRecord[testreleases.ReleaseStagedManifest](
		read.Values[1].Value,
		"release-staged-manifest",
	)
	if err != nil {
		t.Fatal(err)
	}
	largestInput := 0
	for _, member := range manifest.Members {
		stored, err := fixture.store.Get(ctx, testreleases.ReleaseRenderInputStagingKey(publication, member.ReleaseID))
		if err != nil || stored.Entry == nil || len(stored.Entry.Value) > testreleases.MaximumReleaseRenderInputBytes {
			t.Fatalf("bounded member input: %v", err)
		}
		largestInput = max(largestInput, len(stored.Entry.Value))
	}
	authority := claim.Assignment.Record.RestorationAuthority
	nativeBytes := 0
	for _, native := range authority.NativePredecessors {
		nativeBytes += len(native.CurrentArtifact) + len(native.RetainedPriorArtifact)
	}
	if nativeBytes > testtaskjournal.MaximumTaskRecordBytes ||
		len(read.Values[2].Value) > testtaskjournal.MaximumTaskRecordBytes {
		t.Fatal("native assignment exceeds its existing durable bound")
	}
	t.Logf("marker=%d largest-render=%d native-witness=%d assignment-record=%d publication=%+v claim=%+v",
		markerBytes, largestInput, nativeBytes, len(read.Values[2].Value),
		fixture.publicationSize.Publication, fixture.publicationSize.Assignment)
}
