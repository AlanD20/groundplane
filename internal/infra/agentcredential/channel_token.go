package agentcredential

import (
	"context"
	"encoding/base64"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

func ReadChannelToken(ctx context.Context, tokenPath string, expectedUID uint32) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(tokenPath) || filepath.Clean(tokenPath) != tokenPath {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid channel token path")
	}
	directory, err := os.OpenRoot(filepath.Dir(tokenPath))
	if err != nil {
		return nil, errs.New(errs.KindInternal, "agent: read channel token")
	}
	defer directory.Close()
	name := filepath.Base(tokenPath)
	entry, err := directory.Lstat(name)
	if err != nil || !secureTokenFile(entry, expectedUID) {
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid channel token file")
	}
	file, err := directory.Open(name)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "agent: read channel token")
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(entry, opened) || !secureTokenFile(opened, expectedUID) {
		// Best effort: preserve the validation failure if closing also fails.
		_ = file.Close()
		return nil, errs.New(errs.KindValidationFailed, "agent: invalid channel token file")
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, agentprotocol.EncodedTokenBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		clear(encoded)
		return nil, errs.New(errs.KindInternal, "agent: read channel token")
	}
	defer clear(encoded)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(encoded) != agentprotocol.EncodedTokenBytes {
		return nil, errs.New(errs.KindValidationFailed, "agent: channel token has invalid encoding")
	}
	token := make([]byte, agentprotocol.RawTokenBytes)
	written, err := base64.RawURLEncoding.Strict().Decode(token, encoded)
	if err != nil || written != agentprotocol.RawTokenBytes {
		clear(token)
		return nil, errs.New(errs.KindValidationFailed, "agent: channel token has invalid encoding")
	}
	return token, nil
}

func secureTokenFile(info os.FileInfo, expectedUID uint32) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == expectedUID
}
