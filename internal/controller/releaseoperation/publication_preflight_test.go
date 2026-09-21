package releaseoperation

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: exercise the publication entry point with real ledger/repository
// objects; an unavailable candidate or predecessor must never reach persistence.
func TestPublishUnavailableImageResolutionDoesNotStage(t *testing.T) {
	for _, missingPrior := range []bool{false, true} {
		t.Run(map[bool]string{false: "candidate", true: "predecessor"}[missingPrior], func(t *testing.T) {
			store := &publicationStoreWitness{t: t}
			tasks, err := etcd.NewTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := etcd.NewReleaseLedger(store, tasks)
			if err != nil {
				t.Fatal(err)
			}
			images := &missingPublicationImage{prior: missingPrior}
			service := &Service{ledger: ledger, agents: publicationAgent{}, images: images, now: time.Now}
			candidate := releaseCandidateInput{
				selection: workloadseal.Selection{
					Requested: &workloadseal.Requested{Reference: "app:candidate", Replicas: 2},
				},
			}
			if missingPrior {
				candidate.priorWorkload = &domain.WorkloadSeal{
					RequestedReference: "app:old",
					LocalImageID:       "sha256:" + strings.Repeat("a", 64),
					ReplicaCount:       3,
				}
			}
			response, err := service.publish(
				context.Background(), testreleasequeries.ReleasePlanningScope{}, etcd.ReleaseDesiredService,
				"service",
				1,
				"",
				[]releaseCandidateInput{
					candidate,
				}, testidempotency.IdempotencyLocator{}, testidempotency.ProtectedIntentRecord{}, idempotentintent.ProtectedEvidence{},
			)
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindWorkloadImageResolutionUnavailable {
				t.Fatalf("missing image error = %v", err)
			}
			if images.calls != 1 || store.writes != 0 || response.Status != 0 || len(response.Body) != 0 {
				t.Fatal("failed preflight published state or response")
			}
		})
	}
}

type publicationAgent struct{}

func (publicationAgent) GetSingleton(
	context.Context,
) (testkeyvalue.Versioned[testlocalagents.LocalAgentRecord], error) {
	return testkeyvalue.Versioned[testlocalagents.LocalAgentRecord]{
		Record: testlocalagents.LocalAgentRecord{ID: "agent"},
	}, nil
}

type missingPublicationImage struct {
	calls int
	prior bool
}

func (resolver *missingPublicationImage) ResolveWorkloadImages(
	_ context.Context,
	_ string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	resolver.calls++
	expected := 1
	if resolver.prior {
		expected = 2
	}
	if len(selectors) != expected || resolver.prior && selectors[1].GetLocalImageId() == "" {
		return nil, errs.New(errs.KindInternal, "incomplete publication image batch")
	}
	return nil, errs.New(errs.KindWorkloadImageResolutionUnavailable, "Agent image resolution unavailable")
}

// This store contains no implementation of publication: every persistence
// operation is witnessed so moving staging ahead of preflight fails the test.
type publicationStoreWitness struct {
	t      *testing.T
	writes int
}

func (store *publicationStoreWitness) unexpected() {
	store.t.Helper()
	store.t.Fatal("publication reached persistence before successful image preflight")
}
func (store *publicationStoreWitness) Health(context.Context) error { store.unexpected(); return nil }
func (store *publicationStoreWitness) Get(context.Context, string) (*testkeyvalue.GetResult, error) {
	store.unexpected()
	return nil, nil
}

func (store *publicationStoreWitness) GetMany(
	context.Context,
	testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	store.unexpected()
	return nil, nil
}
func (store *publicationStoreWitness) Put(context.Context, string, []byte) (int64, error) {
	store.writes++
	store.unexpected()
	return 0, nil
}
func (store *publicationStoreWitness) Delete(context.Context, string) (int64, error) {
	store.writes++
	store.unexpected()
	return 0, nil
}

func (store *publicationStoreWitness) Range(
	context.Context,
	testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	store.unexpected()
	return nil, nil
}

func (store *publicationStoreWitness) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *publicationStoreWitness) Transact(
	context.Context,
	[]testkeyvalue.Condition,
	[]testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.writes++
	store.unexpected()
	return testkeyvalue.TransactionResult{}, nil
}
func (store *publicationStoreWitness) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	store.unexpected()
	return nil, nil
}
func (store *publicationStoreWitness) Snapshot(context.Context, io.Writer) error {
	store.unexpected()
	return nil
}
func (store *publicationStoreWitness) Close() error { return nil }

func (store *publicationStoreWitness) ValidateBlueprintTaskTerminal(
	context.Context,
	etcd.BlueprintTaskTerminalTransaction,
) error {
	store.unexpected()
	return errs.New(errs.KindInternal, "unexpected Blueprint terminal validation")
}

func (store *publicationStoreWitness) TransactBlueprintTaskTerminal(
	context.Context,
	etcd.BlueprintTaskTerminalTransaction,
) (testkeyvalue.TransactionResult, error) {
	store.writes++
	store.unexpected()
	return testkeyvalue.TransactionResult{}, errs.New(errs.KindInternal, "unexpected Blueprint terminal transaction")
}
