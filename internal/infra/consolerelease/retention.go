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

func splitExpiredRetired(releases []retiredRelease, cutoff time.Time) (kept []retiredRelease, expired []Artifact) {
	for _, retired := range releases {
		if retired.RetiredAt.After(cutoff) {
			kept = append(kept, retired)
			continue
		}
		expired = append(expired, retired.Artifact)
	}
	return kept, expired
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
