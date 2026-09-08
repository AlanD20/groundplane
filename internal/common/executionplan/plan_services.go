package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateServices(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) error {
	previous := ""
	for _, service := range artifact.Services {
		if service == nil || (validateID(ids.KindService, service.ServiceId) != nil &&
			validateID(ids.KindComponent, service.ServiceId) != nil) {
			return errs.New(errs.KindValidationFailed, "Compose artifact service id is invalid")
		}
		identity := service.ServiceId + "\x00" + service.ComposeName
		if previous >= identity {
			return errs.New(
				errs.KindValidationFailed,
				"Compose artifact services must be uniquely sorted by id and name",
			)
		}
		previous = identity
		if err := validateComposeName(service.ComposeName); err != nil {
			return err
		}
		if err := validateLabels(plan, artifact, "service", service.ServiceId, service.ExpectedLabels); err != nil {
			return err
		}
		componentLabel := ""
		for _, label := range service.ExpectedLabels {
			switch label.GetKey() {
			case labelComponentID:
				componentLabel = label.GetValue()
			}
		}
		if service.GetOwnerComponentId() != componentLabel ||
			(service.GetOwnerComponentId() != "" && validateID(ids.KindComponent, service.GetOwnerComponentId()) != nil) {
			return errs.New(errs.KindValidationFailed, "Compose artifact service Component ownership is invalid")
		}
		if service.GetOwnerComponentId() != "" ||
			plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY &&
				artifact.GetOwnerKind() == agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM {
			if !validManagedComposeImage(service) {
				return errs.New(errs.KindValidationFailed, "component Compose service image identity is invalid")
			}
		}
		switch service.Role {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED:
			if service.Slot != "" || len(service.ProxyConfigJson) != 0 || len(service.ProxyConfigSha256) != 0 {
				return errs.New(errs.KindValidationFailed, "ordinary Compose service carries release runtime metadata")
			}
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
			if service.Slot != "blue" && service.Slot != "green" || len(service.ProxyConfigJson) != 0 ||
				len(service.ProxyConfigSha256) != 0 {
				return errs.New(errs.KindValidationFailed, "workload slot service metadata is invalid")
			}
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
			if service.Slot != "" || len(service.ProxyConfigJson) != 0 || len(service.ProxyConfigSha256) != 0 {
				return errs.New(errs.KindValidationFailed, "recreate singleton service metadata is invalid")
			}
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
			digest := sha256.Sum256(service.ProxyConfigJson)
			if service.Slot != "" || len(service.ProxyConfigJson) == 0 ||
				len(service.ProxyConfigSha256) != sha256.Size ||
				subtle.ConstantTimeCompare(service.ProxyConfigSha256, digest[:]) != 1 {
				return errs.New(errs.KindValidationFailed, "stable proxy service metadata is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Compose service runtime role is unsupported")
		}
	}
	return nil
}

func validManagedComposeImage(service *agentpb.ComposeService) bool {
	if !validComponentDigest(service.GetImageIndexDigest()) || !validComponentDigest(service.GetImageChildDigest()) ||
		!validComponentDigest(
			service.GetImageConfigDigest(),
		) || !imageref.IsDigestPinned(service.GetImageReference()) ||
		service.GetImageReference() != service.GetImageRepository()+"@sha256:"+hex.EncodeToString(
			service.GetImageChildDigest(),
		) ||
		service.GetImageOs() != "linux" {
		return false
	}
	if service.GetImageArchitecture() != "amd64" && service.GetImageArchitecture() != "arm64" ||
		service.GetImageArchitecture() == "amd64" && service.GetImageVariant() != "" ||
		service.GetImageArchitecture() == "arm64" && service.GetImageVariant() != "" &&
			service.GetImageVariant() != "v8" {
		return false
	}
	platform := service.GetImageOs() + "/" + service.GetImageArchitecture()
	if service.GetImageVariant() != "" {
		platform += "/" + service.GetImageVariant()
	}
	labels := map[string]string{
		labelImageIndexDigest:  "sha256:" + hex.EncodeToString(service.GetImageIndexDigest()),
		labelImageChildDigest:  "sha256:" + hex.EncodeToString(service.GetImageChildDigest()),
		labelImageConfigDigest: "sha256:" + hex.EncodeToString(service.GetImageConfigDigest()),
		labelImagePlatform:     platform,
	}
	for _, label := range service.GetExpectedLabels() {
		if value, expected := labels[label.GetKey()]; expected {
			if value != label.GetValue() {
				return false
			}
			delete(labels, label.GetKey())
		}
	}
	return len(labels) == 0
}
