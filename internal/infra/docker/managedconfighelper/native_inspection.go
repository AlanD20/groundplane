package managedconfighelper

import (
	"context"
	"errors"
	"os"
	"path"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// InspectNativeCommit observes the exact file transaction's finalization. A
// matching file digest alone cannot complete a different operation's write.
func InspectNativeCommit(ctx context.Context, request *agentpb.ManagedConfigHelperRequest) (bool, error) {
	if ctx == nil {
		return false, errs.New(errs.KindValidationFailed, "managed-file commit inspection requires context")
	}
	owned, err := validateRequest(request)
	if err != nil {
		return false, err
	}
	defer clear(owned.Content)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	root, err := os.OpenRoot(ManagedRoot)
	if err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	tx := path.Join(transactionRoot, owned.TransactionId)
	_, err = root.Lstat(path.Join(tx, "request.pb"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	manifest, err := readManifest(root, tx)
	if err != nil || !sameTransaction(manifest, owned) {
		return false, errs.New(errs.KindStateConflict, "managed-file commit inspection has a different captured transaction")
	}
	_, err = root.Lstat(path.Join(tx, "committed"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	live, found, err := readRegular(root, owned.RelativePath)
	if err != nil {
		return false, err
	}
	defer clear(live)
	owner, ownerFound, err := readTargetOwner(root, owned.RelativePath)
	if err != nil {
		return false, err
	}
	if !found || !digestEqual(live, owned.Sha256) || !targetOwnerMatches(owner, ownerFound, owned.TransactionId, true) {
		return false, errs.New(errs.KindStateConflict, "committed managed file no longer belongs to its captured transaction")
	}
	return true, nil
}
