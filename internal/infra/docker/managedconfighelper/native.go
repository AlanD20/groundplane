package managedconfighelper

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ApplyNative adds exact predecessor-file ownership to the physical atomic
// writer. Native operation admission and helper termination remain caller-owned.
func ApplyNative(
	ctx context.Context,
	request *agentpb.ManagedConfigHelperRequest,
	previousOwner string,
) (*agentpb.ManagedConfigHelperResponse, error) {
	if previousOwner != "" && !validTransactionID(previousOwner) {
		return nil, errs.New(errs.KindValidationFailed, "captured managed-file predecessor owner is invalid")
	}
	return applyWithOwner(ctx, ManagedRoot, request, &previousOwner, false)
}

// RestoreNative is explicit captured recovery, not replay of a forward write.
// It may restore a physically committed file when the enclosing native operation
// subsequently failed. The original transaction evidence is never rewritten.
func RestoreNative(
	ctx context.Context,
	request *agentpb.ManagedConfigHelperRequest,
	previousOwner string,
) (*agentpb.ManagedConfigHelperResponse, error) {
	if request == nil || request.Operation != agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK ||
		previousOwner != "" && !validTransactionID(previousOwner) {
		return nil, errs.New(errs.KindValidationFailed, "captured managed-file restoration is invalid")
	}
	return applyWithOwner(ctx, ManagedRoot, request, &previousOwner, true)
}

type NativeState struct {
	Present bool
	SHA256  [sha256.Size]byte
	Owner   string
}

func InspectNative(ctx context.Context, relativePath string) (NativeState, error) {
	if ctx == nil || !validRelativePath(relativePath) {
		return NativeState{}, errs.New(errs.KindValidationFailed, "managed-file inspection path is invalid")
	}
	if err := ctx.Err(); err != nil {
		return NativeState{}, err
	}
	root, err := os.OpenRoot(ManagedRoot)
	if err != nil {
		return NativeState{}, errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	content, found, err := readRegular(root, relativePath)
	if err != nil {
		return NativeState{}, err
	}
	defer clear(content)
	owner, ownerFound, err := readTargetOwner(root, relativePath)
	if err != nil {
		return NativeState{}, err
	}
	if !found && ownerFound {
		return NativeState{}, errs.New(errs.KindStateConflict, "managed-file ownership exists without its file")
	}
	state := NativeState{Present: found}
	if found {
		state.SHA256, state.Owner = sha256.Sum256(content), owner
		if !ownerFound {
			return NativeState{}, errs.New(errs.KindStateConflict, "managed file has no captured owner")
		}
	}
	return state, nil
}

func restoreCaptured(
	ctx context.Context,
	root *os.Root,
	request *agentpb.ManagedConfigHelperRequest,
	previousOwner string,
) (*agentpb.ManagedConfigHelperResponse, error) {
	tx := path.Join(transactionRoot, request.TransactionId)
	_, err := root.Lstat(path.Join(tx, "request.pb"))
	if errors.Is(err, os.ErrNotExist) {
		// The granted file stage may never have begun. Absence of its journal
		// is not proof: verify the exact captured predecessor and owner.
		live, found, readErr := readRegular(root, request.RelativePath)
		if readErr != nil {
			return nil, readErr
		}
		defer clear(live)
		owner, ownerFound, readErr := readTargetOwner(root, request.RelativePath)
		if readErr != nil {
			return nil, readErr
		}
		if found != (len(request.ExpectedPreviousSha256) != 0) ||
			found && !digestEqual(live, request.ExpectedPreviousSha256) ||
			!targetOwnerMatches(owner, ownerFound, previousOwner, previousOwner != "") {
			return nil, errs.New(
				errs.KindStateConflict,
				"unstarted file restoration does not match its captured predecessor",
			)
		}
		return responseFor(
			root,
			request,
			live,
			found,
			agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY,
		)
	}
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	manifest, err := readManifest(root, tx)
	if err != nil || !sameTransaction(manifest, request) {
		return nil, errs.New(errs.KindStateConflict, "captured file restoration has a different transaction manifest")
	}
	previous, found, err := readPrevious(root, tx)
	if err != nil {
		return nil, err
	}
	defer clear(previous)
	owner, ownerFound, err := readPreviousOwner(root, tx)
	if err != nil {
		return nil, err
	}
	if found != (len(request.ExpectedPreviousSha256) != 0) ||
		found && !digestEqual(previous, request.ExpectedPreviousSha256) ||
		!targetOwnerMatches(owner, ownerFound, previousOwner, previousOwner != "") {
		return nil, errs.New(
			errs.KindStateConflict,
			"captured file restoration source or previous owner differs from its immutable input",
		)
	}
	if err := rollback(ctx, root, request, previous, found, owner, ownerFound); err != nil {
		return nil, err
	}
	if err := writeAtomic(ctx, root, path.Join(tx, "restored"), []byte("restored\n"), 0o600); err != nil {
		return nil, err
	}
	return responseFor(
		root,
		request,
		previous,
		found,
		agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED,
	)
}
