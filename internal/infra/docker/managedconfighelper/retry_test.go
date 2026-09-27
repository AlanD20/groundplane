package managedconfighelper

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DNS-04: lost terminal acknowledgement must not turn an exact retry into a
// predecessor conflict; retry rollback must still restore the original bytes.
func TestRetryPublishedConfigurationPreservesRestorationAuthority(t *testing.T) {
	for _, stage := range []string{"published", "committed", "retry interrupted before ownership transfer"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			first := transactionRequest("mct_first", []byte("candidate\n"))
			before := []byte("predecessor\n")
			digest := sha256.Sum256(before)
			first.ExpectedPreviousSha256 = digest[:]
			live := filepath.Join(root, first.RelativePath)
			if err := os.MkdirAll(filepath.Dir(live), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(live, before, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := applyAt(context.Background(), root, first); err != nil {
				t.Fatal(err)
			}
			if stage == "committed" {
				if _, err := applyAt(context.Background(), root, terminalRequest(first, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT)); err != nil {
					t.Fatal(err)
				}
			}
			retry := transactionRequest("mct_retry", first.Content)
			retry.ExpectedPreviousSha256 = digest[:]
			retry.RetryOfTransactionId = first.TransactionId
			if stage == "retry interrupted before ownership transfer" {
				opened, err := os.OpenRoot(root)
				if err != nil {
					t.Fatal(err)
				}
				err = prepareTransaction(
					context.Background(),
					opened,
					filepath.ToSlash(filepath.Join(transactionRoot, retry.TransactionId)),
					retry,
					before,
					true,
					"",
					false,
				)
				closeErr := opened.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("prepare interrupted retry: %v, %v", err, closeErr)
				}
			}
			for range 2 {
				response, err := applyAt(context.Background(), root, retry)
				if err != nil || !digestEqual(before, response.GetPreviousSha256()) ||
					!digestEqual(first.Content, response.GetLiveSha256()) {
					t.Fatalf("retry proof = %v, %v", response, err)
				}
			}
			if _, err := applyAt(context.Background(), root, terminalRequest(retry, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT)); err != nil {
				t.Fatal(err)
			}
			third := transactionRequest("mct_third", first.Content)
			third.ExpectedPreviousSha256 = digest[:]
			third.RetryOfTransactionId = retry.TransactionId
			if _, err := applyAt(context.Background(), root, third); err != nil {
				t.Fatal(err)
			}
			if _, err := applyAt(context.Background(), root, terminalRequest(third, agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK)); err != nil {
				t.Fatal(err)
			}
			if string(mustRead(t, live)) != string(before) {
				t.Fatal("retry restored candidate instead of original predecessor")
			}
		})
	}
}

// DNS-04: identical file content is not enough to adopt a different owner's
// transaction or a changed generation, and rejected retries must not alter it.
func TestRetryRejectsChangedAuthority(t *testing.T) {
	for _, change := range []string{"owner", "generation", "predecessor"} {
		t.Run(change, func(t *testing.T) {
			root := t.TempDir()
			first := transactionRequest("mct_first", []byte("candidate\n"))
			if _, err := applyAt(context.Background(), root, first); err != nil {
				t.Fatal(err)
			}
			retry := transactionRequest("mct_retry", first.Content)
			retry.RetryOfTransactionId = first.TransactionId
			switch change {
			case "owner":
				retry.RetryOfTransactionId = "mct_unrelated"
			case "generation":
				retry.Generation++
			case "predecessor":
				digest := sha256.Sum256([]byte("different"))
				retry.ExpectedPreviousSha256 = digest[:]
			}
			if _, err := applyAt(context.Background(), root, retry); err == nil {
				t.Fatal("accepted changed retry authority")
			}
			if string(mustRead(t, filepath.Join(root, first.RelativePath))) != string(first.Content) {
				t.Fatal("rejection changed live bytes")
			}
			if string(mustRead(t, filepath.Join(root, targetOwnerPath(first.RelativePath)))) != first.TransactionId {
				t.Fatal("rejection changed owner")
			}
		})
	}
}
