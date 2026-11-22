package consolerelease

import (
	"archive/tar"
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func extractBundle(ctx context.Context, reader *tar.Reader, destination string) (Manifest, string, int64, error) {
	seen := make(map[string]struct{})
	var manifestRaw []byte
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return Manifest{}, "", 0, err
		}
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > maximumFileBytes {
			return Manifest{}, "", 0, errs.New(
				errs.KindStateConflict,
				"Console release bundle contains an unsafe member",
			)
		}
		name := header.Name
		if name != manifestName && !validAssetPath(name) || name == "" || path.Clean(name) != name ||
			len(seen) >= maximumFiles+1 {
			return Manifest{}, "", 0, errs.New(errs.KindStateConflict, "Console release bundle path is unsafe")
		}
		if _, duplicate := seen[name]; duplicate {
			return Manifest{}, "", 0, errs.New(errs.KindStateConflict, "Console release bundle repeats a member")
		}
		seen[name] = struct{}{}
		value, err := io.ReadAll(io.LimitReader(reader, header.Size+1))
		if err != nil || int64(len(value)) != header.Size {
			return Manifest{}, "", 0, errs.New(errs.KindStateConflict, "Console release bundle member is truncated")
		}
		total += header.Size
		if total > maximumBundleBytes {
			return Manifest{}, "", 0, errs.New(
				errs.KindStateConflict,
				"Console release bundle exceeds its unpacked bound",
			)
		}
		if name == manifestName {
			manifestRaw = value
			continue
		}
		target := filepath.Join(destination, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return Manifest{}, "", 0, errs.Wrap(errs.KindInternal, err)
		}
		if err := os.WriteFile(target, value, 0o400); err != nil {
			return Manifest{}, "", 0, errs.Wrap(errs.KindInternal, err)
		}
	}
	manifest, identity, err := decodeManifest(manifestRaw)
	if err != nil {
		return Manifest{}, "", 0, err
	}
	if err := verifyFiles(os.DirFS(destination), manifest); err != nil {
		return Manifest{}, "", 0, err
	}
	if len(seen) != len(manifest.Files)+1 {
		return Manifest{}, "", 0, errs.New(
			errs.KindStateConflict,
			"Console release bundle inventory differs from its manifest",
		)
	}
	if err := os.WriteFile(filepath.Join(destination, manifestName), manifestRaw, 0o400); err != nil {
		return Manifest{}, "", 0, errs.Wrap(errs.KindInternal, err)
	}
	return manifest, identity, total, nil
}

func verifyFiles(assets fs.FS, manifest Manifest) error {
	for _, expected := range manifest.Files {
		value, err := fs.ReadFile(assets, expected.Path)
		if err != nil || int64(len(value)) != expected.Size || digest(value) != expected.SHA256 {
			return errs.Newf(errs.KindStateConflict, "Console asset %q differs from its manifest", expected.Path)
		}
	}
	return nil
}
