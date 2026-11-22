package softwarepreparation

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (preparer *Preparer) prepareConsoleRelease(
	ctx context.Context,
	release *ResolvedRelease,
	workspace string,
) (consolerelease.Artifact, error) {
	if release == nil || preparer.console == nil {
		return consolerelease.Artifact{}, errs.New(errs.KindStateConflict, "Console release authority is unavailable")
	}
	version := releaseVersion(release.OriginalRef)
	bundleName := fmt.Sprintf("groundplane-console-%s.tar.gz", version)
	paths := make(map[string]string, len(release.Assets))
	var bundleDigest string
	for _, asset := range release.Assets {
		destination := filepath.Join(workspace, asset.Name)
		if err := preparer.downloadAsset(ctx, release.OriginalRef, asset, destination); err != nil {
			return consolerelease.Artifact{}, err
		}
		paths[asset.Name] = destination
		if asset.Name == bundleName {
			bundleDigest = asset.SHA256
		}
	}
	if bundleDigest == "" || paths[bundleName] == "" || paths[bundleName+".sha256"] == "" {
		return consolerelease.Artifact{}, errs.New(
			errs.KindStateConflict,
			"Console release bundle authority is incomplete",
		)
	}
	if err := verifyReleaseChecksum(paths[bundleName], paths[bundleName+".sha256"], bundleName); err != nil {
		return consolerelease.Artifact{}, err
	}
	return preparer.console.StageArchive(
		ctx, paths[bundleName], release.OriginalRef, bundleDigest, release.ResolvedSHA,
	)
}

func preparerConsoleArtifactInvalid(artifact consolerelease.Artifact) bool { return !artifact.Valid() }
