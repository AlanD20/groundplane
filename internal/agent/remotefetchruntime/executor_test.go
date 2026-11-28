package remotefetchruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReconciledFetchOnlyObservesPinnedImage(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	plan := imagefetch.Plan{
		Repository: "ghcr.io/owner/app", ManifestDigest: "sha256:" + strings.Repeat("a", 64),
		ConfigDigest: "sha256:" + strings.Repeat("b", 64), Architecture: "amd64",
	}
	plan.Requested = plan.Reference()
	_, digest, err := imagefetch.Encode(plan)
	if err != nil {
		t.Fatal(err)
	}
	attempt := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		Number: 1, AssignedAt: now, Deadline: now.Add(time.Minute), Direction: workexecution.Forward,
		Executor: workexecution.Identity{
			Executor: workexecution.Agent, HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1,
		},
		Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: digest},
	}
	host := &testHost{}
	journal := &testJournal{}
	executor, err := New(journal, host)
	if err != nil {
		t.Fatal(err)
	}
	exchange := &testExchange{}
	if err := executor.Run(context.Background(), attempt, plan, exchange); err != nil {
		t.Fatal(err)
	}
	if host.fetches != 0 || host.observations != 1 || exchange.local != plan.ConfigDigest || !journal.settled {
		t.Fatalf("reconciled result did not preserve the read-only boundary: host=%+v local=%s", host, exchange.local)
	}
}

type testHost struct{ fetches, observations int }

func (host *testHost) FetchPublicImage(_ context.Context, plan imagefetch.Plan) (string, error) {
	host.fetches++
	return plan.ConfigDigest, nil
}

func (host *testHost) ObservePublicImage(_ context.Context, plan imagefetch.Plan) (string, error) {
	host.observations++
	return plan.ConfigDigest, nil
}

type testJournal struct{ settled bool }

func (*testJournal) BeginRemoteImage(context.Context, workexecution.Attempt) (bool, *string, error) {
	return true, nil, nil
}

func (journal *testJournal) SettleTerminalDelivery(context.Context, string) error {
	journal.settled = true
	return nil
}

type testExchange struct{ local string }

func (*testExchange) StartAttempt(_ context.Context, attempt workexecution.Attempt) (*agentpb.WorkStartAck, error) {
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.WorkStartAck{
		Attempt:   identity,
		StartedAt: timestamppb.New(attempt.AssignedAt),
		Reconcile: true,
	}, nil
}

func (exchange *testExchange) ReportRemoteImage(_ context.Context, request *agentpb.RemoteImageResultRequest) error {
	exchange.local = request.LocalImageId
	return nil
}
