package softwarestaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

// Cleanup accounts only for the stopped copy-helper and private scratch of the
// captured preparation. It never imports again, changes a running container,
// removes a staged immutable release or deletes an installed image.
func (stager *Stager) Cleanup(ctx context.Context, record preparationrecord.Record) error {
	if err := preparationrecord.ValidateRecord(record); err != nil {
		return err
	}
	if !record.Source.Selection.IncludesController() {
		return nil
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	name := "groundplane-stage-" + record.TaskID
	existing, err := engine.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err == nil {
		if existing.Container.Config == nil || existing.Container.Config.Labels[ownerLabel] != record.TaskID || existing.Container.State == nil || existing.Container.State.Running {
			return errs.New(errs.KindStateConflict, "software staging helper is not a safely stopped owned container")
		}
		if _, err := engine.ContainerRemove(ctx, existing.Container.ID, client.ContainerRemoveOptions{}); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
	} else if !errdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, err)
	}
	root, err := os.Lstat(stager.workspaceRoot)
	if err != nil || !safeDirectory(root) {
		return errs.New(errs.KindStateConflict, "software staging cleanup root ownership is unsafe")
	}
	directory := filepath.Join(stager.workspaceRoot, ".controller-stage-"+record.TaskID)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if !safeDirectory(info) {
		return errs.New(errs.KindStateConflict, "software staging cleanup workspace ownership is unsafe")
	}
	return os.RemoveAll(directory)
}
