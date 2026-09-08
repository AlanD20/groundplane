package etcd

import (
	"context"
	"testing"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type blueprintEpochMutationRejectingStore struct {
	*releaseRenderInputTestStore
	epochKey               string
	lastEpochMutationCount int
}

func (store *blueprintEpochMutationRejectingStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	epochMutations := 0
	for _, mutation := range mutations {
		if mutation.Type == MutationPut && mutation.Key == store.epochKey {
			epochMutations++
			if epochMutations > 1 {
				return TransactionResult{}, errs.New(errs.KindInternal, "duplicate Blueprint epoch mutation")
			}
		}
	}
	store.lastEpochMutationCount = epochMutations
	return store.releaseRenderInputTestStore.Transact(ctx, conditions, mutations)
}

// Consume the production terminal envelope while preserving the fixture's
// duplicate-epoch guard and the underlying store's atomic revision.
func (store *blueprintEpochMutationRejectingStore) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope BlueprintTaskTerminalTransaction,
) (TransactionResult, error) {
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return TransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	return store.Transact(ctx, conditions, mutations)
}

func (store *blueprintEpochMutationRejectingStore) ValidateBlueprintTaskTerminal(
	_ context.Context,
	envelope BlueprintTaskTerminalTransaction,
) error {
	return envelope.ValidateBudget("")
}

// blueprintServingPredecessorFixture supplies the sealed singleton runtime that
// distinguishes an applied serving predecessor from configured-only intent.
func blueprintServingPredecessorFixture(
	t *testing.T,
	projection EnvironmentComposeProjection,
	taskID, releaseID string,
	workload *domain.WorkloadSeal,
) (EnvironmentComposeProjection, string) {
	t.Helper()
	projection.RevisionID = taskID
	projection.RenderGeneration = 1
	artifact := new(agentpb.ComposeArtifact)
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	service := artifact.Services[0]
	service.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
	service.ImageReference = workload.LocalImageID
	service.ExpectedReplicas = uint32(workload.ReplicaCount)
	service.ExpectedLabels = []*agentpb.LabelPair{
		{Key: "com.groundplane.release-id", Value: releaseID},
		{Key: "com.groundplane.runtime-role", Value: "singleton"},
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	projection.ComposeArtifact = value
	return projection, artifact.GetArtifactId()
}

// The publication marker retains the exact executed candidate artifact, whose
// identity is independent of the prior applied singleton selected for recovery.
func blueprintExecutedSingletonFixture(
	t *testing.T,
	predecessor []byte,
	artifactID, releaseID string,
	workload domain.WorkloadSeal,
) []byte {
	t.Helper()
	artifact := new(agentpb.ComposeArtifact)
	if err := proto.Unmarshal(predecessor, artifact); err != nil {
		t.Fatal(err)
	}
	artifact.ArtifactId = artifactID
	artifact.Services[0].ImageReference = workload.LocalImageID
	artifact.Services[0].ExpectedReplicas = uint32(workload.ReplicaCount)
	artifact.Services[0].ExpectedLabels = []*agentpb.LabelPair{
		{Key: "com.groundplane.release-id", Value: releaseID},
		{Key: "com.groundplane.runtime-role", Value: "singleton"},
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
