package managedconfighelper

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: an empty predecessor digest is the wire-level assertion that initial publication owns an absent path.
func TestPublishSucceedsWhenExpectedPredecessorIsAbsent(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(root, "coredns", "Corefile")
	candidate := []byte("forward . 8.8.8.8\n")
	request := transactionRequest("mct_absentok", candidate)

	response, err := applyAt(context.Background(), root, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED ||
		len(response.GetPreviousSha256()) != 0 || string(mustRead(t, live)) != string(candidate) {
		t.Fatalf("publish absent predecessor = %#v, %v", response, err)
	}

	rollback := terminalRequest(request, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK)
	if _, err := applyAt(context.Background(), root, rollback); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(live); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rolled-back initially absent target stat error = %v", err)
	}
}

// Rationale: ownership metadata cannot own an absent target, so a clean first
// activation must replace an orphan marker and remain safely reversible.
func TestPublishClaimsAbsentTargetDespiteOrphanedOwnershipMarker(t *testing.T) {
	root := t.TempDir()
	request := transactionRequest("mct_cleanstart", []byte("forward . 8.8.8.8\n"))
	if err := os.Mkdir(filepath.Join(root, lockRoot), 0o755); err != nil {
		t.Fatal(err)
	}
	ownerPath := filepath.Join(root, filepath.FromSlash(targetOwnerPath(request.RelativePath)))
	if err := os.WriteFile(ownerPath, []byte("mct_orphaned"), 0o600); err != nil {
		t.Fatal(err)
	}

	response, err := applyAt(context.Background(), root, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED ||
		len(response.GetPreviousSha256()) != 0 ||
		string(mustRead(t, filepath.Join(root, "coredns", "Corefile"))) != string(request.Content) ||
		string(mustRead(t, ownerPath)) != request.TransactionId {
		t.Fatalf("publish over orphan owner = %#v, %v", response, err)
	}

	rollback := terminalRequest(request, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK)
	if _, err := applyAt(context.Background(), root, rollback); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{filepath.Join(root, "coredns", "Corefile"), ownerPath} {
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rolled-back initially absent state %q stat error = %v", name, err)
		}
	}
}

// Rationale: initial publication must not overwrite or begin a transaction over unexpected physical state.
func TestPublishRejectsExistingTargetWhenExpectedPredecessorIsAbsent(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(root, "coredns", "Corefile")
	unexpected := []byte("forward . 1.1.1.1\n")
	if err := os.WriteFile(live, unexpected, 0o644); err != nil {
		t.Fatal(err)
	}
	request := transactionRequest("mct_absentno", []byte("forward . 8.8.8.8\n"))

	if _, err := applyAt(context.Background(), root, request); err == nil {
		t.Fatal("publish unexpectedly accepted an existing target")
	}
	if value := mustRead(t, live); string(value) != string(unexpected) {
		t.Fatalf("existing target changed to %q", value)
	}
	transaction := filepath.Join(root, transactionRoot, request.TransactionId)
	if _, err := os.Stat(transaction); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected transaction stat error = %v", err)
	}
}

// Rationale: clean first activation may recover exact candidate bytes left live
// before transaction acknowledgement, without inventing a predecessor artifact.
func TestPublishRecoversExactLiveCandidateWithoutPredecessor(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	candidate := []byte("forward . 8.8.8.8\n")
	live := filepath.Join(root, "coredns", "Corefile")
	if err := os.WriteFile(live, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	request := transactionRequest("mct_absentrecover", candidate)

	response, err := applyAt(context.Background(), root, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_RECOVERED ||
		len(response.GetPreviousSha256()) != 0 {
		t.Fatalf("recover exact candidate = %#v, %v", response, err)
	}
	replayed, err := applyAt(context.Background(), root, request)
	if err != nil ||
		replayed.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY ||
		len(replayed.GetPreviousSha256()) != 0 {
		t.Fatalf("replay recovered candidate = %#v, %v", replayed, err)
	}
	rollback := terminalRequest(request, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK)
	if _, err := applyAt(context.Background(), root, rollback); err != nil {
		t.Fatal(err)
	}
	if _, err := applyAt(context.Background(), root, rollback); err != nil {
		t.Fatalf("rollback replay = %v", err)
	}
	if _, err := os.Stat(live); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rolled-back recovered target stat error = %v", err)
	}
}

// Rationale: exact candidate bytes do not authorize a second Task attempt to
// adopt and later remove the first transaction's committed managed file.
func TestDistinctTransactionCannotAdoptOwnedExactCandidate(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	candidate := []byte("forward . 8.8.8.8\n")
	live := filepath.Join(root, "coredns", "Corefile")
	if err := os.WriteFile(live, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	owner := transactionRequest("mct_absentowner", candidate)
	if _, err := applyAt(context.Background(), root, owner); err != nil {
		t.Fatal(err)
	}
	challenger := transactionRequest("mct_absentchallenger", candidate)
	if _, err := applyAt(context.Background(), root, challenger); err == nil {
		t.Fatal("distinct transaction adopted an owned exact candidate")
	}
	if _, err := applyAt(context.Background(), root, terminalRequest(
		owner,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT,
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := applyAt(context.Background(), root, terminalRequest(
		challenger,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK,
	)); err == nil {
		t.Fatal("unpublished challenger rollback unexpectedly succeeded")
	}
	if value := mustRead(t, live); string(value) != string(candidate) {
		t.Fatalf("committed owner candidate = %q", value)
	}
}

// Rationale: compensation may remove an absent-predecessor candidate only
// while the exact bytes published by that transaction remain live.
func TestRollbackRejectsChangedLiveTargetAfterExactCandidateRecovery(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	candidate := []byte("forward . 8.8.8.8\n")
	live := filepath.Join(root, "coredns", "Corefile")
	if err := os.WriteFile(live, candidate, 0o644); err != nil {
		t.Fatal(err)
	}
	request := transactionRequest("mct_changedrollback", candidate)
	if _, err := applyAt(context.Background(), root, request); err != nil {
		t.Fatal(err)
	}
	newer := []byte("forward . 9.9.9.9\n")
	if err := os.WriteFile(live, newer, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := applyAt(context.Background(), root, terminalRequest(
		request,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK,
	)); err == nil {
		t.Fatal("rollback unexpectedly replaced a changed live target")
	}
	if value := mustRead(t, live); string(value) != string(newer) {
		t.Fatalf("newer live target changed to %q", value)
	}
}

// Rationale: replacement is authorized only by the exact digest of an existing regular predecessor.
func TestPublishSucceedsWithExactExistingPredecessorDigest(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(root, "coredns", "Corefile")
	previous := []byte("forward . 1.1.1.1\n")
	if err := os.WriteFile(live, previous, 0o644); err != nil {
		t.Fatal(err)
	}
	request := transactionRequest("mct_exactyes", []byte("forward . 8.8.8.8\n"))
	previousDigest := sha256.Sum256(previous)
	request.ExpectedPreviousSha256 = previousDigest[:]

	response, err := applyAt(context.Background(), root, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED ||
		string(response.GetPreviousSha256()) != string(previousDigest[:]) ||
		string(mustRead(t, live)) != string(request.Content) {
		t.Fatalf("publish exact predecessor = %#v, %v", response, err)
	}
}

func TestTransactionPublishRollbackCommitAndReplay(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(root, "coredns", "Corefile")
	previous := []byte("forward . 1.1.1.1\n")
	if err := os.WriteFile(live, previous, 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(live)
	if err != nil {
		t.Fatal(err)
	}
	candidate := []byte("forward . 8.8.8.8\n")
	request := transactionRequest("mct_aaaaaaaa", candidate)
	previousDigest := sha256.Sum256(previous)
	request.ExpectedPreviousSha256 = previousDigest[:]
	response, err := applyAt(context.Background(), root, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED {
		t.Fatalf("publish = %#v, %v", response, err)
	}
	after, err := os.Stat(live)
	if err != nil || os.SameFile(before, after) {
		t.Fatalf("publish did not replace the live inode: %v", err)
	}
	response, err = applyAt(context.Background(), root, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY {
		t.Fatalf("publish replay = %#v, %v", response, err)
	}
	terminal := terminalRequest(request, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK)
	response, err = applyAt(context.Background(), root, terminal)
	if err != nil || string(mustRead(t, live)) != string(previous) {
		t.Fatalf("rollback = %#v, %v", response, err)
	}
	response, err = applyAt(context.Background(), root, terminal)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY {
		t.Fatalf("rollback replay = %#v, %v", response, err)
	}

	request = transactionRequest("mct_bbbbbbbb", candidate)
	request.ExpectedPreviousSha256 = previousDigest[:]
	if _, err := applyAt(context.Background(), root, request); err != nil {
		t.Fatal(err)
	}
	terminal = terminalRequest(request, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT)
	if _, err := applyAt(context.Background(), root, terminal); err != nil {
		t.Fatal(err)
	}
	response, err = applyAt(context.Background(), root, terminal)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY ||
		string(mustRead(t, live)) != string(candidate) {
		t.Fatalf("commit replay = %#v, %v", response, err)
	}
}

// Rationale: a crash before the final directory rename may leave only bounded
// staging files; the deterministic transaction id must remain retryable.
func TestPublishRebuildsPartialPreparation(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	request := transactionRequest("mct_partialprep", []byte("forward . 8.8.8.8\n"))
	staging := filepath.Join(root, transactionRoot, request.TransactionId+preparationSuffix)
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "request.pb"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(staging, ".candidate.0123456789abcdef.tmp"),
		[]byte("partial"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	response, err := applyAt(context.Background(), root, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED {
		t.Fatalf("publish after partial preparation = %#v, %v", response, err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging stat after publication = %v", err)
	}
	if value := mustRead(t, filepath.Join(root, "coredns", "Corefile")); string(value) != string(request.Content) {
		t.Fatalf("live candidate = %q", value)
	}
}

// Rationale: once complete immutable metadata is atomically visible, replay
// must finish publication after a crash before the live target write.
func TestPublishRecoversAfterPreparedTransactionBecomesVisible(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootPath, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	previous := []byte("forward . 1.1.1.1\n")
	live := filepath.Join(rootPath, "coredns", "Corefile")
	if err := os.WriteFile(live, previous, 0o644); err != nil {
		t.Fatal(err)
	}
	request := transactionRequest("mct_prepared", []byte("forward . 8.8.8.8\n"))
	previousDigest := sha256.Sum256(previous)
	request.ExpectedPreviousSha256 = previousDigest[:]
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureTrustedDirectory(root, transactionRoot); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := prepareTransaction(
		context.Background(),
		root,
		filepath.ToSlash(filepath.Join(transactionRoot, request.TransactionId)),
		request,
		previous,
		true,
		"",
		false,
	); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if value := mustRead(t, live); string(value) != string(previous) {
		t.Fatalf("live value changed before replay = %q", value)
	}

	response, err := applyAt(context.Background(), rootPath, request)
	if err != nil ||
		response.GetDisposition() != agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_RECOVERED {
		t.Fatalf("prepared replay = %#v, %v", response, err)
	}
	if value := mustRead(t, live); string(value) != string(request.Content) {
		t.Fatalf("recovered live candidate = %q", value)
	}
}

// Rationale: an older transaction prepared against an absent target must not
// adopt or remove a later transaction's committed same-byte candidate.
func TestPreparedAbsentTransactionRejectsCommittedSuccessorOwnership(t *testing.T) {
	rootPath := t.TempDir()
	older := transactionRequest("mct_olderabsent", []byte("forward . 8.8.8.8\n"))
	successor := transactionRequest("mct_successor", older.Content)
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureTrustedDirectory(root, transactionRoot); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := prepareTransaction(
		context.Background(),
		root,
		filepath.ToSlash(filepath.Join(transactionRoot, older.TransactionId)),
		older,
		nil,
		false,
		"",
		false,
	); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := applyAt(context.Background(), rootPath, successor); err != nil {
		t.Fatal(err)
	}
	if _, err := applyAt(context.Background(), rootPath, terminalRequest(
		successor,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT,
	)); err != nil {
		t.Fatal(err)
	}

	if _, err := applyAt(context.Background(), rootPath, older); err == nil {
		t.Error("older transaction adopted its committed successor")
	}
	if _, err := applyAt(context.Background(), rootPath, terminalRequest(
		older,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK,
	)); err == nil {
		t.Error("older transaction removed its committed successor")
	}
	live := filepath.Join(rootPath, "coredns", "Corefile")
	if value := mustRead(t, live); string(value) != string(successor.Content) {
		t.Fatalf("successor candidate = %q", value)
	}
	ownerPath := filepath.Join(rootPath, filepath.FromSlash(targetOwnerPath(successor.RelativePath)))
	if owner := mustRead(t, ownerPath); string(owner) != successor.TransactionId {
		t.Fatalf("successor owner = %q", owner)
	}
}

func TestConcurrentPublishSerializesPredecessorAssertion(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "coredns"), 0o755); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(root, "coredns", "Corefile")
	previous := []byte("forward . 1.1.1.1\n")
	if err := os.WriteFile(live, previous, 0o644); err != nil {
		t.Fatal(err)
	}
	previousDigest := sha256.Sum256(previous)
	requests := []*agentpb.ManagedConfigHelperRequest{
		transactionRequest("mct_cccccccc", []byte("forward . 8.8.8.8\n")),
		transactionRequest("mct_dddddddd", []byte("forward . 9.9.9.9\n")),
	}
	for _, request := range requests {
		request.ExpectedPreviousSha256 = previousDigest[:]
	}
	start := make(chan struct{})
	errorsByIndex := make([]error, len(requests))
	var wait sync.WaitGroup
	for index, request := range requests {
		wait.Add(1)
		go func(index int, request *agentpb.ManagedConfigHelperRequest) {
			defer wait.Done()
			<-start
			_, errorsByIndex[index] = applyAt(context.Background(), root, request)
		}(index, request)
	}
	close(start)
	wait.Wait()
	succeeded := 0
	for _, err := range errorsByIndex {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("concurrent publishes succeeded = %d, errors = %v", succeeded, errorsByIndex)
	}
}

func transactionRequest(transactionID string, content []byte) *agentpb.ManagedConfigHelperRequest {
	digest := sha256.Sum256(content)
	return &agentpb.ManagedConfigHelperRequest{
		Schema: SchemaVersion, ArtifactId: "cfg_candidate", RelativePath: "coredns/Corefile",
		Content: append([]byte(nil), content...), Sha256: digest[:],
		Operation:     agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
		TransactionId: transactionID, Generation: 7,
	}
}

func terminalRequest(
	request *agentpb.ManagedConfigHelperRequest,
	operation agentpb.ManagedConfigOperation,
) *agentpb.ManagedConfigHelperRequest {
	return &agentpb.ManagedConfigHelperRequest{
		Schema:       request.Schema,
		ArtifactId:   request.ArtifactId,
		RelativePath: request.RelativePath,
		Sha256: append(
			[]byte(nil),
			request.Sha256...),
		ExpectedPreviousSha256: append([]byte(nil), request.ExpectedPreviousSha256...),
		Operation:              operation,
		TransactionId:          request.TransactionId,
		Generation:             request.Generation,
	}
}

func mustRead(t *testing.T, name string) []byte {
	t.Helper()
	value, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
