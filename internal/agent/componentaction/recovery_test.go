package componentaction

import (
	context "context"
	sha256 "crypto/sha256"
	testing "testing"

	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func TestRegisteredComponentActionRuntimeRecoversExactManagedConfigTransaction(t *testing.T) {
	candidate := sha256.Sum256([]byte("candidate Corefile"))
	previous := sha256.Sum256([]byte("previous Corefile"))
	request := &agentpb.ManagedConfigHelperRequest{
		Schema: 1, TransactionId: "mct_exact", Operation: agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
		Sha256: candidate[:], ExpectedPreviousSha256: previous[:],
	}
	executor := &componentActionRecoveringManagedExecutor{response: &agentpb.ManagedConfigHelperResponse{
		Schema: 1, TransactionId: request.GetTransactionId(), Operation: request.GetOperation(),
		Disposition: agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_RECOVERED,
		LiveSha256:  candidate[:], PreviousSha256: previous[:],
	}}
	actionRuntime := &Runtime{managedHelper: executor}
	state, err := actionRuntime.executeManagedConfig(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if executor.calls != 2 || !state.Live.Present || state.Live.SHA256 != candidate ||
		!state.Previous.Present || state.Previous.SHA256 != previous {
		t.Fatalf("managed-config recovery calls = %d, state = %#v", executor.calls, state)
	}
}
