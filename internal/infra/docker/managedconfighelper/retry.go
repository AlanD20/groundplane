package managedconfighelper

import (
	"context"
	"os"
	"path"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// retrySource accepts only the preceding attempt's exact sealed file input.
// Matching live bytes alone never authorize taking another transaction's file.
func retrySource(root *os.Root, request *agentpb.ManagedConfigHelperRequest) (string, error) {
	tx := path.Join(transactionRoot, request.RetryOfTransactionId)
	manifest, err := readManifest(root, tx)
	if err != nil {
		return "", err
	}
	if manifest.TransactionId != request.RetryOfTransactionId {
		return "", errs.New(errs.KindStateConflict, "managed-config retry source identity changed")
	}
	manifest.TransactionId = request.TransactionId
	manifest.RetryOfTransactionId = request.RetryOfTransactionId
	if !sameTransaction(manifest, request) || !exists(root, path.Join(tx, "published")) ||
		exists(root, path.Join(tx, "rolledback")) {
		return "", errs.New(errs.KindStateConflict, "managed-config retry source does not match its captured input")
	}
	return tx, nil
}

func publishRetry(
	ctx context.Context,
	root *os.Root,
	tx string,
	request *agentpb.ManagedConfigHelperRequest,
) (*agentpb.ManagedConfigHelperResponse, error) {
	source, err := retrySource(root, request)
	if err != nil {
		return nil, err
	}
	previous, existed, err := readPrevious(root, source)
	if err != nil {
		return nil, err
	}
	defer clear(previous)
	if existed != (len(request.ExpectedPreviousSha256) != 0) ||
		existed && !digestEqual(previous, request.ExpectedPreviousSha256) {
		return nil, errs.New(errs.KindStateConflict, "managed-config retry predecessor proof changed")
	}
	owner, ownerExists, err := readPreviousOwner(root, source)
	if err != nil {
		return nil, err
	}
	if err := prepareTransaction(ctx, root, tx, request, previous, existed, owner, ownerExists); err != nil {
		return nil, err
	}
	// The original candidate is already live. Transfer only its exact ownership;
	// retain the original predecessor for this attempt's independently proven rollback.
	return replayPublish(ctx, root, tx, request)
}
