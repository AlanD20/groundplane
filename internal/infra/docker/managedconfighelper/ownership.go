package managedconfighelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func rollback(
	ctx context.Context,
	root *os.Root,
	request *agentpb.ManagedConfigHelperRequest,
	previous []byte,
	previousExists bool,
	previousOwner string,
	previousOwnerExists bool,
) error {
	live, liveExists, err := readRegular(root, request.RelativePath)
	if err != nil {
		return err
	}
	defer clear(live)
	owner, ownerExists, err := readTargetOwner(root, request.RelativePath)
	if err != nil {
		return err
	}
	candidateLive := liveExists && digestEqual(live, request.Sha256)
	previousLive := liveExists == previousExists && (!liveExists || bytesEqual(live, previous))
	transactionOwnsTarget := targetOwnerMatches(owner, ownerExists, request.TransactionId, true)
	previousOwnsTarget := targetOwnerMatches(owner, ownerExists, previousOwner, previousOwnerExists)
	if !candidateLive && !(previousLive && (transactionOwnsTarget || previousOwnsTarget)) {
		return errs.New(errs.KindStateConflict, "managed-config rollback target changed")
	}
	if previousLive && previousOwnsTarget {
		return nil
	}
	if candidateLive {
		if !transactionOwnsTarget {
			return errs.New(errs.KindStateConflict, "managed-config rollback ownership changed")
		}
		if previousExists {
			if err := writeAtomic(ctx, root, request.RelativePath, previous, 0o644); err != nil {
				return err
			}
		} else {
			if err := root.Remove(request.RelativePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return errs.Wrap(errs.KindInternal, err)
			}
			if err := syncDirectory(root, path.Dir(request.RelativePath)); err != nil {
				return err
			}
		}
	}
	if previousOwnerExists {
		return writeTargetOwner(ctx, root, request.RelativePath, previousOwner)
	}
	if err := root.Remove(targetOwnerPath(request.RelativePath)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return syncDirectory(root, lockRoot)
}

func readPreviousOwner(root *os.Root, tx string) (string, bool, error) {
	if exists(root, path.Join(tx, "previous.owner.absent")) {
		return "", false, nil
	}
	value, found, err := readRegular(root, path.Join(tx, "previous.owner"))
	if err != nil || !found || !validTransactionID(string(value)) {
		clear(value)
		return "", false, errs.New(errs.KindStateConflict, "managed-config previous owner is missing")
	}
	owner := string(value)
	clear(value)
	return owner, true, nil
}

func readOrphanOwner(root *os.Root, tx string) (string, bool, error) {
	value, found, err := readRegular(root, path.Join(tx, "orphan.owner"))
	if err != nil || !found {
		return "", false, err
	}
	defer clear(value)
	owner := string(value)
	if !validTransactionID(owner) {
		return "", false, errs.New(errs.KindStateConflict, "managed-config orphan owner is invalid")
	}
	return owner, true, nil
}

func readTargetOwner(root *os.Root, relativePath string) (string, bool, error) {
	value, found, err := readRegular(root, targetOwnerPath(relativePath))
	if err != nil || !found {
		return "", false, err
	}
	defer clear(value)
	owner := string(value)
	if !validTransactionID(owner) {
		return "", false, errs.New(errs.KindStateConflict, "managed-config target owner is invalid")
	}
	return owner, true, nil
}

func writeTargetOwner(ctx context.Context, root *os.Root, relativePath string, transactionID string) error {
	return writeAtomic(ctx, root, targetOwnerPath(relativePath), []byte(transactionID), 0o600)
}

func targetOwnerPath(relativePath string) string {
	digest := sha256.Sum256([]byte(relativePath))
	return path.Join(lockRoot, hex.EncodeToString(digest[:])+".owner")
}

func targetOwnerMatches(actual string, actualExists bool, expected string, expectedExists bool) bool {
	return actualExists == expectedExists && (!actualExists || actual == expected)
}
