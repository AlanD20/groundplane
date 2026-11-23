package consolerelease

import (
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func deduplicateRetired(retired []retiredRelease, active Artifact, predecessor *Artifact) []retiredRelease {
	seen := map[string]struct{}{active.ManifestSHA256: {}}
	if predecessor != nil {
		seen[predecessor.ManifestSHA256] = struct{}{}
	}
	result := make([]retiredRelease, 0, len(retired))
	for index := len(retired) - 1; index >= 0; index-- {
		item := retired[index]
		if _, duplicate := seen[item.Artifact.ManifestSHA256]; duplicate {
			continue
		}
		seen[item.Artifact.ManifestSHA256] = struct{}{}
		result = append(result, item)
	}
	slices.Reverse(result)
	return result
}

func splitExpiredRetired(
	releases []retiredRelease, cutoff time.Time, keepIdentity string,
) (kept []retiredRelease, expired []Artifact) {
	for _, retired := range releases {
		if retired.RetiredAt.After(cutoff) || retired.Artifact.ManifestSHA256 == keepIdentity {
			kept = append(kept, retired)
			continue
		}
		expired = append(expired, retired.Artifact)
	}
	return kept, expired
}

func planActivation(current activeReceipt, artifact Artifact, at time.Time) (activeReceipt, []Artifact, error) {
	if current.Active == artifact {
		return current, nil, nil
	}
	retired := slices.Clone(current.Retired)
	if current.Predecessor != nil && *current.Predecessor != artifact {
		retired = append(retired, retiredRelease{Artifact: *current.Predecessor, RetiredAt: at})
	}
	previous := current.Active
	retired = deduplicateRetired(retired, artifact, &previous)
	kept, expired := splitExpiredRetired(retired, at.Add(-retiredAssetWindow), artifact.ManifestSHA256)
	if len(kept) > maximumRetired {
		return activeReceipt{}, nil, errs.New(
			errs.KindResourceInUse,
			"Console retention is full; wait for the old-asset window before another activation",
		)
	}
	next := activeReceipt{Schema: 1, Active: artifact, Predecessor: &previous, Retired: kept, ActivatedAt: at}
	return next, expired, nil
}

func (store *Store) admitArchive(identity string, incoming, ceiling int64) error {
	err := store.admit(identity, incoming, ceiling)
	if kind, _ := errs.KindOf(err); kind != errs.KindResourceInUse {
		return err
	}
	next := store.receipt
	cutoff := store.now().UTC().Add(-retiredAssetWindow)
	kept, expired := splitExpiredRetired(next.Retired, cutoff, identity)
	if len(expired) == 0 {
		return err
	}
	next.Retired = kept
	if err := store.commitReceipt(next); err != nil {
		return err
	}
	if err := store.removeExpired(expired); err != nil {
		return err
	}
	return store.admit(identity, incoming, ceiling)
}

func (store *Store) commitReceipt(next activeReceipt) error {
	if err := store.writeReceipt(next); err != nil {
		return err
	}
	if err := store.loadReceipt(next); err != nil {
		_ = store.writeReceipt(store.receipt)
		return err
	}
	return nil
}

func (store *Store) removeExpired(expired []Artifact) error {
	for _, artifact := range expired {
		if err := os.RemoveAll(store.releasePath(artifact.ManifestSHA256)); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
	}
	return nil
}

type compositeFS struct{ releases []releaseFS }

func (assets compositeFS) Open(name string) (fs.File, error) {
	if len(assets.releases) == 0 {
		return nil, fs.ErrNotExist
	}
	file, err := assets.releases[0].assets.Open(name)
	if err == nil || !errors.Is(err, fs.ErrNotExist) || !oldAssetPath(name) {
		return file, err
	}
	for _, release := range assets.releases[1:] {
		file, err = release.assets.Open(name)
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			return file, err
		}
	}
	return nil, fs.ErrNotExist
}

func oldAssetPath(name string) bool {
	if !strings.HasPrefix(name, "assets/") || !validAssetPath(name) {
		return false
	}
	base := path.Base(name)
	stem := strings.TrimSuffix(base, path.Ext(base))
	separator := strings.LastIndexByte(stem, '-')
	if separator <= 0 || len(stem)-separator-1 != 8 {
		return false
	}
	_, err := hex.DecodeString(stem[separator+1:])
	return err == nil
}
