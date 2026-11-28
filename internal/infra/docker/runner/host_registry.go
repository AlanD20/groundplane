package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/registryconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func runnerDockerConfig(plan corerunner.Plan) string {
	return filepath.Join(plan.Paths.RunnerHome, ".groundplane-docker")
}

// Seed ordinary Docker credentials once. Trusted workflows may add other
// logins; maintenance must not overwrite their Docker configuration.
func prepareRunnerRegistry(ctx context.Context, plan corerunner.Plan) error {
	directory := runnerDockerConfig(plan)
	if err := ensureOwnedDirectory(directory, plan.Identity.UID, plan.Identity.GID, 0o700); err != nil {
		return err
	}
	path := filepath.Join(directory, "config.json")
	if plan.Egress.RegistrationOnly {
		return verifyScopedDockerConfig(path, plan.Identity.UID)
	}
	if plan.Egress.ScopedEndpoint.IsValid() {
		if err := requireScopedRegistryTrust(plan); err != nil {
			return err
		}
		return verifyScopedDockerConfig(path, plan.Identity.UID)
	}
	if err := prepareRunnerRegistryTrust(ctx, plan); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || stat.Uid != plan.Identity.UID {
			return errs.New(errs.KindStateConflict, "Runner Docker credentials ownership changed")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	value, err := registryconfiguration.ReadFile(ctx, "client/config.json")
	if err != nil {
		return err
	}
	defer clear(value)
	file, err := os.CreateTemp(directory, ".credentials-")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer os.Remove(file.Name()) // The stage is absent after successful publication.
	_, writeErr := file.Write(value)
	ownerErr := file.Chown(int(plan.Identity.UID), int(plan.Identity.GID))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, ownerErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	// Link publishes without replacing a file a job created concurrently.
	if err := os.Link(file.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func verifyScopedDockerConfig(path string, uid uint32) error {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.New(errs.KindStateConflict, "Runner Docker credentials cannot be verified")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return errs.New(errs.KindStateConflict, "Runner Docker credentials ownership changed")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid {
		return errs.New(errs.KindStateConflict, "Runner Docker credentials ownership changed")
	}
	const maxConfigBytes = 64 << 10
	value, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	defer clear(value)
	var config struct {
		Auths       map[string]struct{} `json:"auths"`
		CredHelpers map[string]string   `json:"credHelpers"`
	}
	if err != nil || len(value) > maxConfigBytes || json.Unmarshal(value, &config) != nil {
		return errs.New(errs.KindStateConflict, "Runner Docker credentials cannot be verified")
	}
	for key := range config.Auths {
		if legacyRegistryKey(key) {
			return errs.New(
				errs.KindStateConflict,
				"Remove legacy registry credentials before activating scoped Runner access",
			)
		}
	}
	for key := range config.CredHelpers {
		if legacyRegistryKey(key) {
			return errs.New(
				errs.KindStateConflict,
				"Remove legacy registry credentials before activating scoped Runner access",
			)
		}
	}
	return nil
}

func legacyRegistryKey(key string) bool {
	key = strings.TrimPrefix(strings.TrimPrefix(strings.ToLower(key), "https://"), "http://")
	authority, _, _ := strings.Cut(key, "/")
	return authority == imagefetch.RegistryAuthority || authority == imagefetch.RegistryHostname
}

func runnerRegistryCA(plan corerunner.Plan) string {
	return filepath.Join(plan.Paths.RunnerHome, ".config", "docker", "certs.d", imagefetch.RegistryAuthority, "ca.crt")
}

func prepareRunnerRegistryTrust(ctx context.Context, plan corerunner.Plan) error {
	directory := plan.Paths.RunnerHome
	for _, part := range []string{".config", "docker", "certs.d", imagefetch.RegistryAuthority} {
		directory = filepath.Join(directory, part)
		if err := ensureOwnedDirectory(directory, plan.Identity.UID, plan.Identity.GID, 0o700); err != nil {
			return err
		}
	}
	certificate, err := registryconfiguration.ReadFile(ctx, "tls.crt")
	if err != nil {
		return err
	}
	path := runnerRegistryCA(plan)
	if info, err := os.Lstat(path); err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || stat.Uid != plan.Identity.UID {
			return errs.New(errs.KindStateConflict, "Runner registry trust ownership changed")
		}
		value, err := os.ReadFile(path)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if !bytes.Equal(value, certificate) {
			return errs.New(errs.KindStateConflict, "Runner registry trust changed")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	file, err := os.CreateTemp(directory, ".ca-")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer os.Remove(file.Name()) // Publication removes the stage; failed stages stay private.
	_, writeErr := file.Write(certificate)
	ownerErr := file.Chown(int(plan.Identity.UID), int(plan.Identity.GID))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, ownerErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
