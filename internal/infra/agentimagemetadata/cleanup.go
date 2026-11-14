package agentimagemetadata

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

const ownerLabel = "groundplane.software.inspection"
const neverExecute = "/groundplane-never-execute"

func name(owner string) string { return "groundplane-agent-metadata-" + owner }

func validOwner(owner string) error {
	if ids.Validate(ids.KindTask, owner) != nil && ids.Validate(ids.KindOperation, owner) != nil {
		return invalid()
	}
	return nil
}

func Cleanup(ctx context.Context, owner string) error {
	if err := validOwner(owner); err != nil {
		return err
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	return cleanup(ctx, engine, owner)
}

func cleanup(ctx context.Context, engine *client.Client, owner string) error {
	existing, err := engine.ContainerInspect(ctx, name(owner), client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	config, state, host := existing.Container.Config, existing.Container.State, existing.Container.HostConfig
	if config == nil || state == nil || host == nil || state.Running || config.Labels[ownerLabel] != owner ||
		len(config.Entrypoint) != 1 || config.Entrypoint[0] != neverExecute ||
		string(host.NetworkMode) != "none" || len(host.Mounts) != 0 || len(host.Binds) != 0 || !host.ReadonlyRootfs {
		return errs.New(errs.KindStateConflict, "Agent metadata helper is not the owned stopped inspection container")
	}
	_, err = engine.ContainerRemove(ctx, existing.Container.ID, client.ContainerRemoveOptions{})
	if err != nil && !errdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
