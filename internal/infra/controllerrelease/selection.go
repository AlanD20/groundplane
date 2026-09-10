package controllerrelease

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// OpenOptional permits a foreground development Controller without native
// bootstrap. An existing but incomplete/unsafe installation still fails closed.
func OpenOptional(ctx context.Context) (*Store, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if _, err := os.Lstat(upgrade.ReleaseDirectory); errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, fileError(err)
	}
	store, err := Open(ctx)
	return store, err == nil, err
}

// Candidate is the deployment-owned selector, not a second upload API. Its
// pointer and the selected manifest/binary all pass the private store boundary.
func (store *Store) Candidate(ctx context.Context) (upgrade.Release, bool, error) {
	raw, err := store.read(ctx, store.root, "candidate.json", 256)
	if errors.Is(err, fs.ErrNotExist) {
		return upgrade.Release{}, false, nil
	}
	if err != nil {
		return upgrade.Release{}, false, err
	}
	pointer, err := jcs.Decode[struct {
		Release upgrade.Digest `json:"release"`
	}](raw)
	if err != nil {
		return upgrade.Release{}, false, err
	}
	manifest, err := store.Inspect(ctx, pointer.Release)
	if err != nil {
		return upgrade.Release{}, false, err
	}
	return upgrade.Release{Release: pointer.Release, Manifest: manifest}, true, nil
}

// Selected returns the last qualified release. During an unfinished operation,
// new Agent image selections are refused; recovery uses its frozen Task input.
func (store *Store) Selected(ctx context.Context) (upgrade.Release, bool, error) {
	journal, found, err := store.Current(ctx)
	if err != nil {
		return upgrade.Release{}, false, err
	}
	if found {
		if !journal.Phase.Settled() {
			return upgrade.Release{}, false, errs.New(
				errs.KindResourceInUse,
				"controller update still owns release selection",
			)
		}
		if journal.Phase == upgrade.PhaseHealthy {
			selected := upgrade.Release{Release: journal.Release, Manifest: journal.Manifest}
			if err := selected.Validate(); err != nil {
				return upgrade.Release{}, false, err
			}
			return selected, true, nil
		}
	}
	raw, err := store.read(ctx, store.root, "selected.json", upgrade.MaxManifestBytes+256)
	if errors.Is(err, fs.ErrNotExist) {
		return upgrade.Release{}, false, nil
	}
	if err != nil {
		return upgrade.Release{}, false, err
	}
	selected, err := jcs.Decode[upgrade.Release](raw)
	if err != nil {
		return upgrade.Release{}, false, err
	}
	if err := selected.Validate(); err != nil {
		return upgrade.Release{}, false, err
	}
	return selected, true, nil
}

// retainSelection runs under the journal lock BEFORE replacing a Healthy
// journal. A crash on either side leaves the same successful release selected;
// failed/cancelled trials never promote their manifest or erase its predecessor.
func (store *Store) retainSelection(ctx context.Context, journal upgrade.Journal) error {
	if journal.Phase != upgrade.PhaseHealthy {
		return nil
	}
	selected := upgrade.Release{Release: journal.Release, Manifest: journal.Manifest}
	if err := selected.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(selected)
	if err != nil {
		return fileError(err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	return store.atomicWrite(ctx, store.root, "selected.json", 0o400, func(output io.Writer) error {
		_, err := output.Write(canonical)
		return fileError(err)
	})
}
