package consolerelease

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// WriteSourceBundle packages Console files from the same verified build output
// as the incoming Controller. The caller still stages and verifies the archive
// before making it available for activation.
func WriteSourceBundle(ctx context.Context, dist, destination string, manifest Manifest) (string, error) {
	if !commitPattern(manifest.SourceCommit) || manifest.Release != "console/ref/"+manifest.SourceCommit ||
		!digestPattern.MatchString(manifest.APISHA256) || manifest.CreatedAt.Nanosecond() != 0 {
		return "", errs.New(errs.KindValidationFailed, "source Console bundle authority is invalid")
	}
	info, err := os.Lstat(dist)
	if err != nil || !info.IsDir() {
		return "", errs.New(errs.KindStateConflict, "source Console build output is unavailable")
	}
	root, err := os.OpenRoot(dist)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer output.Close()
	hasher := sha256.New()
	compressed := gzip.NewWriter(io.MultiWriter(output, hasher))
	defer compressed.Close()
	archive := tar.NewWriter(compressed)
	defer archive.Close()
	if err := writeSourceArchive(ctx, root.FS(), archive, manifest); err != nil {
		return "", err
	}
	if err := archive.Close(); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	if err := compressed.Close(); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	if err := output.Close(); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeSourceArchive(ctx context.Context, assets fs.FS, archive *tar.Writer, manifest Manifest) error {
	manifest.Schema = manifestSchema
	manifest.Files = nil
	const versionName = "console-version.json"
	version := []byte(`{"release":"` + manifest.Release + `"}`)
	var total int64
	entries := 0
	appendFile := func(path string, value []byte) error {
		size := int64(len(value))
		if size <= 0 || len(manifest.Files) >= maximumFiles || total+size > maximumBundleBytes {
			return errs.New(errs.KindStateConflict, "source Console bundle exceeds its file or size bound")
		}
		total += size
		manifest.Files = append(manifest.Files, File{Path: path, Size: size, SHA256: digest(value)})
		return writeSourceMember(archive, path, value)
	}
	if err := fs.WalkDir(assets, ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errs.Wrap(errs.KindInternal, walkErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		entries++
		if entries > 2*maximumFiles || (path != "." && !validAssetPath(path)) {
			return errs.New(errs.KindStateConflict, "source Console bundle contains an unsafe path")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumFileBytes {
			return errs.New(errs.KindStateConflict, "source Console bundle contains an unsafe file")
		}
		file, err := assets.Open(path)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		defer file.Close()
		value, err := io.ReadAll(io.LimitReader(file, info.Size()+1))
		if err != nil || int64(len(value)) != info.Size() {
			return errs.New(errs.KindStateConflict, "source Console bundle changed while reading")
		}
		if path == versionName {
			if !slices.Equal(value, version) {
				return errs.New(errs.KindStateConflict, "source Console version asset differs from captured authority")
			}
			return nil
		}
		return appendFile(path, value)
	}); err != nil {
		return err
	}
	if err := appendFile(versionName, version); err != nil {
		return err
	}
	slices.SortFunc(manifest.Files, func(a, b File) int { return strings.Compare(a.Path, b.Path) })
	encoded, _, err := encodeManifest(manifest)
	if err != nil {
		return err
	}
	if total+int64(len(encoded)) > maximumBundleBytes {
		return errs.New(errs.KindStateConflict, "source Console bundle exceeds its unpacked size bound")
	}
	return writeSourceMember(archive, manifestName, encoded)
}

func writeSourceMember(archive *tar.Writer, name string, value []byte) error {
	if err := archive.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(value))}); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if _, err := archive.Write(value); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
