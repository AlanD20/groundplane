// Package softwarestaging imports verified software artifacts for activation.
// Native Controller payloads are staged without execution; Agent images are
// imported without replacing their running container.
package softwarestaging

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/controllerrelease"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

const ownerLabel = "groundplane.software.staging"

type Stager struct {
	store         *controllerrelease.Store
	workspaceRoot string
}

func New(store *controllerrelease.Store, workspaceRoot string) (*Stager, error) {
	if store == nil || !filepath.IsAbs(workspaceRoot) || filepath.Clean(workspaceRoot) != workspaceRoot {
		return nil, errs.New(errs.KindInternal, "native software staging is not configured")
	}
	return &Stager{store: store, workspaceRoot: workspaceRoot}, nil
}

func (stager *Stager) Stage(
	ctx context.Context,
	record preparationrecord.Record,
	agentImage string,
) (_ upgrade.Release, resultErr error) {
	if preparationrecord.ValidateRecord(record) != nil || record.Progress.Phase != preparation.PhaseVerified ||
		record.Progress.Result.Controller == nil || preparation.ValidateResult(record.Progress.Result, record.Source, false) != nil {
		return upgrade.Release{}, errs.New(errs.KindStateConflict, "software preparation is not completely verified")
	}
	selected := record.Progress.Result.Controller
	registry := registryimages.Local{}
	plan, err := registry.Resolve(ctx, selected.Artifact.Reference)
	if err != nil {
		return upgrade.Release{}, err
	}
	if plan.ManifestDigest != selected.Artifact.ManifestDigest || plan.ConfigDigest != selected.Artifact.ConfigDigest ||
		plan.Architecture != selected.Artifact.Platform.Architecture {
		return upgrade.Release{}, errs.New(errs.KindStateConflict, "prepared Controller registry identity changed")
	}
	if _, err := registry.Fetch(ctx, plan, func(imagefetch.Progress) error { return ctx.Err() }); err != nil {
		return upgrade.Release{}, err
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return upgrade.Release{}, errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	name := "groundplane-stage-" + record.TaskID
	if existing, err := engine.ContainerInspect(ctx, name, client.ContainerInspectOptions{}); err == nil {
		if existing.Container.Config == nil || existing.Container.Config.Labels[ownerLabel] != record.TaskID ||
			existing.Container.State == nil || existing.Container.State.Running {
			return upgrade.Release{}, errs.New(errs.KindStateConflict, "software staging container ownership is unsafe")
		}
		if _, err := engine.ContainerRemove(ctx, existing.Container.ID, client.ContainerRemoveOptions{}); err != nil {
			return upgrade.Release{}, errs.Wrap(errs.KindInternal, err)
		}
	} else if !errdefs.IsNotFound(err) {
		return upgrade.Release{}, errs.Wrap(errs.KindInternal, err)
	}
	created, err := engine.ContainerCreate(ctx, client.ContainerCreateOptions{Name: name,
		Config: &container.Config{Image: plan.Reference(), Entrypoint: []string{"/groundplane-never-execute"},
			Labels: map[string]string{ownerLabel: record.TaskID}},
		HostConfig: &container.HostConfig{NetworkMode: "none", ReadonlyRootfs: true},
	})
	if err != nil {
		return upgrade.Release{}, errs.Wrap(errs.KindInternal, err)
	}
	if created.ID == "" {
		return upgrade.Release{}, errs.New(errs.KindInternal, "software staging container identity is missing")
	}
	// There is deliberately no ContainerStart or exec operation in this module.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		_, err := engine.ContainerRemove(cleanup, created.ID, client.ContainerRemoveOptions{})
		resultErr = errors.Join(resultErr, err)
	}()
	workspace, err := stager.workspace(record.TaskID)
	if err != nil {
		return upgrade.Release{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(workspace)) }()
	for _, file := range []struct {
		name    string
		maximum int64
		digest  string
	}{
		{"controller-release.json", upgrade.MaxManifestBytes, selected.MetadataSHA256},
		{"controller", controllerrelease.MaximumBinaryBytes, selected.BinarySHA256},
		{"groundplane", controllerrelease.MaximumBinaryBytes, selected.CLISHA256},
	} {
		copied, err := engine.CopyFromContainer(
			ctx,
			created.ID,
			client.CopyFromContainerOptions{SourcePath: "/" + file.name},
		)
		if err != nil {
			return upgrade.Release{}, errs.Wrap(errs.KindInternal, err)
		}
		err = copyPayloadFile(ctx, copied.Content, workspace, file.name, file.maximum, file.digest)
		closeErr := copied.Content.Close()
		if err != nil || closeErr != nil {
			return upgrade.Release{}, errors.Join(err, closeErr)
		}
	}
	metadata, err := os.ReadFile(filepath.Join(workspace, "controller-release.json"))
	if err != nil {
		return upgrade.Release{}, errs.Wrap(errs.KindInternal, err)
	}
	binary, err := os.Open(filepath.Join(workspace, "controller"))
	if err != nil {
		return upgrade.Release{}, errs.Wrap(errs.KindInternal, err)
	}
	defer binary.Close()
	return stager.store.Stage(ctx, metadata, binary, agentImage)
}

func (stager *Stager) workspace(taskID string) (string, error) {
	root, err := os.Lstat(stager.workspaceRoot)
	if err != nil || !safeDirectory(root) {
		return "", errs.New(errs.KindStateConflict, "software staging root ownership is unsafe")
	}
	directory := filepath.Join(stager.workspaceRoot, ".controller-stage-"+taskID)
	if info, err := os.Lstat(directory); err == nil {
		if !safeDirectory(info) {
			return "", errs.New(errs.KindStateConflict, "software staging workspace ownership is unsafe")
		}
		if err := os.RemoveAll(directory); err != nil {
			return "", errs.Wrap(errs.KindInternal, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return directory, nil
}

func safeDirectory(info os.FileInfo) bool {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func copyPayloadFile(
	ctx context.Context,
	archive io.Reader,
	directory, name string,
	maximum int64,
	expected string,
) error {
	reader := tar.NewReader(archive)
	header, err := reader.Next()
	if err != nil || header == nil || header.Name != name || header.Typeflag != tar.TypeReg ||
		header.Size <= 0 || header.Size > maximum || header.Linkname != "" || strings.ContainsRune(header.Name, '/') {
		return errs.New(errs.KindValidationFailed, "Controller bundle payload is unsafe")
	}
	output, err := os.OpenFile(filepath.Join(directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	for {
		if err := ctx.Err(); err != nil {
			_ = output.Close()
			return err
		}
		count, readErr := reader.Read(buffer)
		if count > 0 {
			if _, err := io.MultiWriter(output, hash).Write(buffer[:count]); err != nil {
				_ = output.Close()
				return errs.Wrap(errs.KindInternal, err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = output.Close()
			return errs.Wrap(errs.KindInternal, readErr)
		}
	}
	if err := output.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if fmt.Sprintf("sha256:%x", hash.Sum(nil)) != expected {
		return errs.New(errs.KindValidationFailed, "Controller bundle payload digest changed")
	}
	if _, err := reader.Next(); err != io.EOF {
		return errs.New(errs.KindValidationFailed, "Controller bundle payload has unexpected archive members")
	}
	return nil
}
