package managedconfighelper

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestCommittedPublishIsAnExactReplay(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	request := transactionRequest("mct_committedreplay", []byte(".:53 {}\n"))
	if _, err := applyAt(context.Background(), root, request); err != nil {
		t.Fatal(err)
	}
	if _, err := applyAt(context.Background(), root, terminalRequest(
		request, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT,
	)); err != nil {
		t.Fatal(err)
	}
	replayed, err := applyAt(context.Background(), root, request)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY {
		t.Fatalf("disposition = %s", replayed.GetDisposition())
	}
}
