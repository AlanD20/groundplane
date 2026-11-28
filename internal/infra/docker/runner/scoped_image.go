package runner

import (
	"context"
	"runtime"

	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (control *HostControl) ScopedImage(
	ctx context.Context,
	plan corerunner.Plan,
) (buildversion.ContainerIdentity, error) {
	if err := validateCall(ctx, plan); err != nil {
		return buildversion.ContainerIdentity{}, err
	}
	if control.local == nil || !plan.Egress.ScopedEndpoint.IsValid() {
		return buildversion.ContainerIdentity{}, errs.New(
			errs.KindStateConflict,
			"Scoped Runner image requires the owning Docker host",
		)
	}
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return buildversion.ContainerIdentity{}, err
	}
	defer engine.Close()
	image, err := engine.ImageInspect(ctx, plan.Container.ImageRef)
	if err != nil {
		return buildversion.ContainerIdentity{}, dockerError(ctx, "inspect pinned scoped Runner image", err)
	}
	if image.Config == nil || image.Config.Labels["com.groundplane.runner.scoped-api"] != "v1" ||
		image.Config.Labels["com.groundplane.version"] == "" || image.Config.Labels["org.opencontainers.image.revision"] == "" ||
		image.Os != runtime.GOOS || image.Architecture != runtime.GOARCH {
		return buildversion.ContainerIdentity{}, errs.New(
			errs.KindStateConflict,
			"Pinned Runner image lacks the verified scoped CLI protocol",
		)
	}
	return installedRunnerSoftware(ctx, engine, image.ID, plan.Container.ImageRef)
}
