package consolerelease

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	DefaultRoot        = "/var/lib/groundplane/console"
	retiredAssetWindow = 7 * 24 * time.Hour
	maximumStoreBytes  = 512 << 20
	maximumRetired     = 64
	activeReceiptName  = "active.json"
)

type retiredRelease struct {
	Artifact  Artifact  `json:"artifact"`
	RetiredAt time.Time `json:"retired_at"`
}

type activeReceipt struct {
	Schema      int              `json:"schema"`
	Active      Artifact         `json:"active"`
	Predecessor *Artifact        `json:"predecessor,omitempty"`
	Retired     []retiredRelease `json:"retired,omitempty"`
	ActivatedAt time.Time        `json:"activated_at"`
}

type releaseFS struct {
	artifact Artifact
	assets   fs.FS
	size     int64
}

// Store is the one serving and activation boundary. Every filesystem admitted
// here has been verified against a canonical manifest.
type Store struct {
	mu        sync.RWMutex
	root      string
	bootstrap fs.FS
	version   string
	apiHash   string
	receipt   activeReceipt
	releases  []releaseFS
	now       func() time.Time
}

func New(root string, bootstrap fs.FS, version string) (*Store, error) {
	clean := filepath.Clean(root)
	if !filepath.IsAbs(clean) || clean != root || bootstrap == nil || !releasePattern.MatchString(version) {
		return nil, errs.New(errs.KindInternal, "Console release store configuration is invalid")
	}
	return &Store{root: clean, bootstrap: bootstrap, version: version, now: time.Now}, nil
}

// BindAPIDocument finishes startup after every typed route is registered. A bad
// active receipt fails closed; embedded assets are only a one-time migration.
func (store *Store) BindAPIDocument(document []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(document) == 0 {
		return errs.New(errs.KindInternal, "Controller API document is unavailable for Console compatibility")
	}
	store.apiHash = APIHash(document)
	if err := store.ensureRoots(); err != nil {
		return err
	}
	if err := store.recoverReceiptWrite(); err != nil {
		return err
	}
	receipt, found, err := store.readReceipt()
	if err != nil {
		return err
	}
	if !found {
		artifact, err := store.stageFS(store.bootstrap, store.version, strings.Repeat("0", 40), store.apiHash)
		if err != nil {
			return err
		}
		receipt = activeReceipt{Schema: 1, Active: artifact, ActivatedAt: store.now().UTC().Truncate(time.Second)}
		if err := store.writeReceipt(receipt); err != nil {
			return err
		}
	}
	return store.loadReceipt(receipt)
}

func (store *Store) Assets() (fs.FS, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.apiHash == "" || len(store.releases) == 0 {
		return nil, errs.New(errs.KindRequestUnavailable, "verified Console assets are unavailable")
	}
	if store.receipt.Active.APISHA256 != store.apiHash {
		return nil, errs.Newf(errs.KindRequestUnavailable,
			"installed Console requires API %s but this Controller provides %s; apply a matching Console release",
			store.receipt.Active.APISHA256, store.apiHash)
	}
	releases := append([]releaseFS(nil), store.releases...)
	return compositeFS{releases: releases}, nil
}

func (store *Store) Current() (Artifact, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.receipt.Active, store.receipt.Active.Valid()
}

func (store *Store) Verify(artifact Artifact) error {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if !artifact.Valid() {
		return errs.New(errs.KindValidationFailed, "Console artifact identity is invalid")
	}
	_, err := store.openRelease(artifact)
	return err
}

// VerifyActivation checks staged bytes and retention capacity without changing
// the selection. All may stage a Console for the incoming Controller's API.
func (store *Store) VerifyActivation(artifact Artifact) error {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if _, err := store.openRelease(artifact); err != nil {
		return err
	}
	_, _, err := planActivation(store.receipt, artifact, store.now().UTC().Truncate(time.Second))
	return err
}

// Compatible proves that an already verified release targets the API served by
// this process. Future paired releases may be staged under their predecessor,
// but they cannot activate until the matching Controller is running.
func (store *Store) Compatible(artifact Artifact) error {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.apiHash == "" || artifact.APISHA256 != store.apiHash {
		return errs.Newf(errs.KindStateConflict,
			"Console release requires API %s but this Controller provides %s", artifact.APISHA256, store.apiHash)
	}
	_, err := store.openRelease(artifact)
	return err
}

// StageArchive authenticates the archive bytes against already captured release
// metadata, then installs the canonical contents without changing active assets.
func (store *Store) StageArchive(
	ctx context.Context,
	archivePath string,
	expectedRelease string,
	expectedBundleSHA256 string,
	expectedSourceCommit string,
) (Artifact, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.apiHash == "" || !releasePattern.MatchString(expectedRelease) ||
		!digestPattern.MatchString(expectedBundleSHA256) || !commitPattern(expectedSourceCommit) {
		return Artifact{}, errs.New(errs.KindStateConflict, "Console store or captured artifact is invalid")
	}
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	defer archive.Close()
	hasher := sha256.New()
	written, err := io.Copy(hasher, io.LimitReader(archive, maximumBundleBytes+1))
	if err != nil || written <= 0 || written > maximumBundleBytes {
		return Artifact{}, errs.New(errs.KindStateConflict, "Console release bundle is empty or oversized")
	}
	actualBundleSHA256 := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if actualBundleSHA256 != expectedBundleSHA256 {
		return Artifact{}, errs.New(errs.KindStateConflict, "Console release bundle differs from captured authority")
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	compressed, err := gzip.NewReader(archive)
	if err != nil {
		return Artifact{}, errs.New(errs.KindStateConflict, "Console release bundle is not valid gzip")
	}
	defer compressed.Close()
	stage, err := os.MkdirTemp(filepath.Join(store.root, "staging"), "bundle-")
	if err != nil {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(stage)
		}
	}()
	manifest, manifestHash, size, err := extractBundle(ctx, tar.NewReader(compressed), stage)
	if err != nil {
		return Artifact{}, err
	}
	actual := Artifact{Release: manifest.Release, BundleSHA256: actualBundleSHA256,
		ManifestSHA256: manifestHash, APISHA256: manifest.APISHA256}
	if actual.Release != expectedRelease {
		return Artifact{}, errs.New(errs.KindStateConflict, "Console release bundle differs from captured authority")
	}
	if manifest.SourceCommit != expectedSourceCommit {
		return Artifact{}, errs.New(
			errs.KindStateConflict,
			"Console release source commit differs from captured authority",
		)
	}
	if err := store.admitArchive(manifestHash, size, maximumStoreBytes); err != nil {
		return Artifact{}, err
	}
	destination := store.releasePath(manifestHash)
	if _, err := os.Lstat(destination); err == nil {
		loaded, loadErr := store.openRelease(actual)
		return loaded.artifact, loadErr
	} else if !errors.Is(err, os.ErrNotExist) {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Rename(stage, destination); err != nil {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	keep = true
	if err := syncDirectory(filepath.Join(store.root, "releases")); err != nil {
		return Artifact{}, err
	}
	return actual, nil
}

func (store *Store) Activate(artifact Artifact) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if artifact.APISHA256 != store.apiHash {
		return errs.Newf(errs.KindStateConflict,
			"Console release requires API %s but this Controller provides %s", artifact.APISHA256, store.apiHash)
	}
	if _, err := store.openRelease(artifact); err != nil {
		return err
	}
	if store.receipt.Active == artifact {
		return nil
	}
	next, expired, err := planActivation(store.receipt, artifact, store.now().UTC().Truncate(time.Second))
	if err != nil {
		return err
	}
	if err := store.commitReceipt(next); err != nil {
		return err
	}
	// Release directories remain available until the replacement receipt commits.
	return store.removeExpired(expired)
}

func (store *Store) stageFS(source fs.FS, release, commit, apiHash string) (Artifact, error) {
	stage, err := os.MkdirTemp(filepath.Join(store.root, "staging"), "bootstrap-")
	if err != nil {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(stage)
		}
	}()
	files := make([]File, 0, 64)
	err = fs.WalkDir(source, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == "." || entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || !validAssetPath(name) {
			return errs.New(errs.KindStateConflict, "embedded Console contains an unsafe asset")
		}
		value, readErr := fs.ReadFile(source, name)
		if readErr != nil || len(value) == 0 || len(value) > maximumFileBytes {
			return errs.New(errs.KindStateConflict, "embedded Console asset is unreadable or oversized")
		}
		target := filepath.Join(stage, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(target, value, 0o400); err != nil {
			return err
		}
		files = append(files, File{Path: name, Size: int64(len(value)), SHA256: digest(value)})
		return nil
	})
	if err != nil {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	sort.Slice(files, func(left, right int) bool { return files[left].Path < files[right].Path })
	manifest := Manifest{Schema: 1, Release: release, SourceCommit: commit, APISHA256: apiHash,
		CreatedAt: store.now().UTC().Truncate(time.Second), Files: files}
	raw, identity, err := encodeManifest(manifest)
	if err != nil {
		return Artifact{}, err
	}
	if err := os.WriteFile(filepath.Join(stage, manifestName), raw, 0o400); err != nil {
		return Artifact{}, errs.Wrap(errs.KindInternal, err)
	}
	artifact := Artifact{Release: release, BundleSHA256: identity, ManifestSHA256: identity, APISHA256: apiHash}
	destination := store.releasePath(identity)
	if err := os.Rename(stage, destination); err != nil {
		if !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, os.ErrExist) {
			return Artifact{}, errs.Wrap(errs.KindInternal, err)
		}
	} else {
		keep = true
	}
	return artifact, nil
}

func (store *Store) ensureRoots() error {
	for _, directory := range []string{store.root, filepath.Join(store.root, "releases"), filepath.Join(store.root, "staging")} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		info, err := os.Lstat(directory)
		status, ok := infoSys(info)
		if err != nil || !ok || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || status.Uid != uint32(os.Geteuid()) {
			return errs.New(errs.KindStateConflict, "Console release store ownership is unsafe")
		}
	}
	return nil
}

func infoSys(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	status, ok := info.Sys().(*syscall.Stat_t)
	return status, ok
}

func (store *Store) releasePath(identity string) string {
	return filepath.Join(store.root, "releases", strings.TrimPrefix(identity, "sha256:"))
}

func (store *Store) openRelease(artifact Artifact) (releaseFS, error) {
	if !artifact.Valid() {
		return releaseFS{}, errs.New(errs.KindValidationFailed, "Console artifact identity is invalid")
	}
	directory := store.releasePath(artifact.ManifestSHA256)
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return releaseFS{}, errs.New(errs.KindStateConflict, "verified Console release directory is unavailable")
	}
	manifestRaw, err := os.ReadFile(filepath.Join(directory, manifestName))
	if err != nil {
		return releaseFS{}, errs.New(errs.KindStateConflict, "verified Console manifest is unavailable")
	}
	manifest, identity, err := decodeManifest(manifestRaw)
	if err != nil || identity != artifact.ManifestSHA256 || manifest.Release != artifact.Release ||
		manifest.APISHA256 != artifact.APISHA256 {
		return releaseFS{}, errs.New(errs.KindStateConflict, "verified Console release identity changed")
	}
	assets := os.DirFS(directory)
	if err := verifyFiles(assets, manifest); err != nil {
		return releaseFS{}, err
	}
	var size int64
	for _, file := range manifest.Files {
		size += file.Size
	}
	return releaseFS{artifact: artifact, assets: assets, size: size + int64(len(manifestRaw))}, nil
}

func (store *Store) loadReceipt(receipt activeReceipt) error {
	if err := validateReceipt(receipt); err != nil {
		return err
	}
	artifacts := []Artifact{receipt.Active}
	if receipt.Predecessor != nil {
		artifacts = append(artifacts, *receipt.Predecessor)
	}
	for _, retired := range receipt.Retired {
		artifacts = append(artifacts, retired.Artifact)
	}
	releases := make([]releaseFS, 0, len(artifacts))
	for _, artifact := range artifacts {
		release, err := store.openRelease(artifact)
		if err != nil {
			return err
		}
		releases = append(releases, release)
	}
	store.receipt, store.releases = receipt, releases
	return nil
}

func validateReceipt(receipt activeReceipt) error {
	if receipt.Schema != 1 || !receipt.Active.Valid() || receipt.ActivatedAt.Before(time.Unix(0, 0)) ||
		receipt.ActivatedAt.Nanosecond() != 0 || len(receipt.Retired) > maximumRetired {
		return errs.New(errs.KindStateConflict, "active Console receipt is invalid")
	}
	seen := map[string]struct{}{receipt.Active.ManifestSHA256: {}}
	if receipt.Predecessor != nil {
		if !receipt.Predecessor.Valid() || *receipt.Predecessor == receipt.Active {
			return errs.New(errs.KindStateConflict, "Console predecessor receipt is invalid")
		}
		seen[receipt.Predecessor.ManifestSHA256] = struct{}{}
	}
	for _, retired := range receipt.Retired {
		if !retired.Artifact.Valid() || retired.RetiredAt.Before(time.Unix(0, 0)) ||
			retired.RetiredAt.Nanosecond() != 0 {
			return errs.New(errs.KindStateConflict, "retired Console receipt is invalid")
		}
		if _, duplicate := seen[retired.Artifact.ManifestSHA256]; duplicate {
			return errs.New(errs.KindStateConflict, "Console receipt repeats a release")
		}
		seen[retired.Artifact.ManifestSHA256] = struct{}{}
	}
	return nil
}

func (store *Store) readReceipt() (activeReceipt, bool, error) {
	value, err := os.ReadFile(filepath.Join(store.root, activeReceiptName))
	if errors.Is(err, os.ErrNotExist) {
		return activeReceipt{}, false, nil
	}
	if err != nil || len(value) == 0 || len(value) > 128<<10 {
		return activeReceipt{}, false, errs.New(errs.KindStateConflict, "active Console receipt is unreadable")
	}
	receipt, err := decodeReceipt(value)
	if err != nil {
		return activeReceipt{}, false, errs.New(errs.KindStateConflict, "active Console receipt is corrupt")
	}
	return receipt, true, nil
}

func decodeReceipt(value []byte) (activeReceipt, error) {
	if len(value) == 0 || len(value) > 128<<10 {
		return activeReceipt{}, errs.New(errs.KindStateConflict, "active Console receipt is unreadable")
	}
	var receipt activeReceipt
	if err := json.Unmarshal(value, &receipt); err != nil {
		return activeReceipt{}, errs.New(errs.KindStateConflict, "active Console receipt is corrupt")
	}
	return receipt, validateReceipt(receipt)
}

// recoverReceiptWrite completes a fully verified selector written before a
// crash between fsync and rename. A partial private staging file is discarded
// so the previous active receipt remains authoritative and future activation is
// not permanently blocked.
func (store *Store) recoverReceiptWrite() error {
	staging := filepath.Join(store.root, ".active-next")
	info, err := os.Lstat(staging)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	status, ok := infoSys(info)
	if err != nil || !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 ||
		status.Uid != uint32(os.Geteuid()) || status.Nlink != 1 {
		return errs.New(errs.KindStateConflict, "staged Console receipt ownership is unsafe")
	}
	value, readErr := os.ReadFile(staging)
	receipt, decodeErr := decodeReceipt(value)
	if readErr != nil || decodeErr != nil {
		if err := os.Remove(staging); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		return syncDirectory(store.root)
	}
	if err := store.loadReceipt(receipt); err != nil {
		return err
	}
	if err := os.Rename(staging, filepath.Join(store.root, activeReceiptName)); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return syncDirectory(store.root)
}

func (store *Store) writeReceipt(receipt activeReceipt) error {
	if err := validateReceipt(receipt); err != nil {
		return err
	}
	value, err := json.Marshal(receipt)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	staging := filepath.Join(store.root, ".active-next")
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return errs.New(errs.KindResourceInUse, "another Console activation is incomplete")
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	_, writeErr := file.Write(value)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = os.Remove(staging)
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Rename(staging, filepath.Join(store.root, activeReceiptName)); err != nil {
		_ = os.Remove(staging)
		return errs.Wrap(errs.KindInternal, err)
	}
	return syncDirectory(store.root)
}

func syncDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	err = errors.Join(handle.Sync(), handle.Close())
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func (store *Store) admit(identity string, incoming, ceiling int64) error {
	entries, err := os.ReadDir(filepath.Join(store.root, "releases"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	var total int64
	for _, entry := range entries {
		if !entry.IsDir() {
			return errs.New(errs.KindStateConflict, "Console release store contains a foreign entry")
		}
		if entry.Name() == strings.TrimPrefix(identity, "sha256:") {
			incoming = 0
		}
		err := filepath.WalkDir(
			filepath.Join(store.root, "releases", entry.Name()),
			func(_ string, item fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if item.Type().IsRegular() {
					info, infoErr := item.Info()
					if infoErr != nil {
						return infoErr
					}
					total += info.Size()
				}
				return nil
			},
		)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
	}
	if total+incoming > ceiling {
		return errs.New(
			errs.KindResourceInUse,
			"Console release store is full; retained active or old-tab assets were not deleted",
		)
	}
	return nil
}
