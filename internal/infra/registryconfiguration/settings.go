// Package registryconfiguration reads the root-owned registry bootstrap inputs.
package registryconfiguration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Root = "/etc/groundplane/registry"
const DataRoot = "/var/lib/groundplane/registry"

type Settings struct {
	Image       string `json:"image"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Certificate []byte `json:"-"`
}

func Read(ctx context.Context) (Settings, error) {
	var settings Settings
	value, err := ReadFile(ctx, "credentials.json")
	if err != nil {
		return settings, err
	}
	defer clear(value)
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&settings); err != nil || !imageref.IsDigestPinned(settings.Image) ||
		settings.Username != "groundplane" || len(settings.Password) < 32 || len(settings.Password) > 128 {
		return Settings{}, errs.New(errs.KindInternal, "registry bootstrap credentials are invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Settings{}, errs.New(errs.KindInternal, "registry bootstrap credentials contain trailing input")
	}
	settings.Certificate, err = ReadFile(ctx, "tls.crt")
	return settings, err
}

func ReadFile(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name != "credentials.json" && name != "tls.crt" && name != "client/config.json" {
		return nil, errs.New(errs.KindInternal, "registry input is not a managed file")
	}
	root, err := os.OpenRoot(Root)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || stat.Uid != 0 || info.Size() > 64<<10 {
		return nil, errs.New(errs.KindStateConflict, "registry input ownership changed")
	}
	file, err := root.Open(filepath.Clean(name))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer file.Close()
	return io.ReadAll(io.LimitReader(file, 64<<10))
}
