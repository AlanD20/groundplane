package composehelpercontainer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validatePreparedServiceImage(service *agentpb.ComposeService) error {
	managed := service.GetOwnerComponentId() != "" || len(service.GetImageConfigDigest()) != 0 ||
		service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY
	if !managed {
		if (service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT || service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON) &&
			!workloadimage.LocalIDValid(service.GetImageReference()) {
			return errs.New(errs.KindValidationFailed, "startup workload lacks sealed local identity")
		}
		return nil
	}
	if len(service.GetImageIndexDigest()) != sha256.Size || len(service.GetImageChildDigest()) != sha256.Size ||
		len(service.GetImageConfigDigest()) != sha256.Size ||
		!imageref.IsDigestPinned(service.GetImageReference()) ||
		service.GetImageReference() != service.GetImageRepository()+"@sha256:"+hex.EncodeToString(
			service.GetImageChildDigest(),
		) ||
		!workloadimage.LocalIDValid("sha256:"+hex.EncodeToString(service.GetImageConfigDigest())) ||
		service.GetImageOs() != "linux" ||
		service.GetImageArchitecture() != "amd64" && service.GetImageArchitecture() != "arm64" ||
		service.GetImageArchitecture() == "amd64" && service.GetImageVariant() != "" ||
		service.GetImageArchitecture() == "arm64" && service.GetImageVariant() != "" &&
			service.GetImageVariant() != "v8" {
		return errs.New(errs.KindValidationFailed, "startup managed image lacks sealed platform authority")
	}
	return nil
}

// prepareServiceImage consumes sealed runtime authority, never a catalog or tag.
func (executor *Executor) prepareServiceImage(ctx context.Context, service *agentpb.ComposeService) error {
	managed := service.GetOwnerComponentId() != "" || len(service.GetImageConfigDigest()) != 0
	if !managed && !workloadimage.LocalIDValid(service.GetImageReference()) {
		return nil
	}
	observed, err := executor.engine.ImageInspect(ctx, service.GetImageReference())
	if err == nil {
		return verifyServiceImage(observed, service, managed)
	}
	if !managed || !containerderrdefs.IsNotFound(err) {
		return operationError(ctx, "inspect sealed service image", err)
	}
	pull, err := executor.engine.ImagePull(ctx, service.GetImageReference(), client.ImagePullOptions{})
	if err != nil {
		return operationError(ctx, "pull sealed managed image", err)
	}
	if pull == nil {
		return errs.New(errs.KindInternal, "managed image pull returned no result")
	}
	waitErr := pull.Wait(ctx)
	closeErr := pull.Close()
	if err := errors.Join(waitErr, closeErr); err != nil {
		return operationError(ctx, "complete managed image pull", err)
	}
	observed, err = executor.engine.ImageInspect(ctx, service.GetImageReference())
	if err != nil {
		return operationError(ctx, "inspect pulled managed image", err)
	}
	return verifyServiceImage(observed, service, true)
}

func verifyServiceImage(observed client.ImageInspectResult, service *agentpb.ComposeService, managed bool) error {
	if !managed {
		if observed.ID != service.GetImageReference() {
			return errs.New(errs.KindStateConflict, "workload image differs from sealed local identity")
		}
		return nil
	}
	if observed.Os != service.GetImageOs() || observed.Architecture != service.GetImageArchitecture() ||
		observed.Variant != service.GetImageVariant() {
		return errs.New(errs.KindStateConflict, "managed image differs from sealed platform identity")
	}
	return managedimage.Verify(
		observed.ID,
		observed.Descriptor,
		"sha256:"+hex.EncodeToString(service.GetImageChildDigest()),
		"sha256:"+hex.EncodeToString(service.GetImageConfigDigest()),
		ocispec.Platform{
			OS:           service.GetImageOs(),
			Architecture: service.GetImageArchitecture(),
			Variant:      service.GetImageVariant(),
		},
	)
}
