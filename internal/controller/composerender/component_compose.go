package composerender

import (
	"encoding/hex"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	environmentfile "github.com/AlanD20/groundplane/internal/controller/environmentfile"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// EnvironmentComponentEnvironmentFile is one service-specific generated env
// file. Values contain reusable Secret identities only; value bytes are
// resolved into the transient materialization channel when a Task is sent.
type EnvironmentComponentEnvironmentFile struct {
	ComponentID string
	ServiceID   string
	ServiceName string
	Destination string
	Values      []componentsdk.ManagedSecretEnvironment
}

// EnvironmentComponentComposeProjection is the complete deterministic
// addition made by enabled Environment components to one parsed Compose
// project. Project is an owned clone; the caller's parsed project is unchanged.
type EnvironmentComponentComposeProjection struct {
	Project          *composetypes.Project
	Services         []composeidentity.Resource
	PlainFiles       []componentrender.GeneratedEnvironmentFile
	EnvironmentFiles []EnvironmentComponentEnvironmentFile
}

// ProjectEnvironmentComponents renders the compiled-in Environment component
// catalog and projects its services into the same Compose namespace as authored
// services. It does not resolve secrets or construct Task identities.
func ProjectEnvironmentComponents(
	project *composetypes.Project,
	environment core.Environment,
	catalog []componentrender.EnvironmentComponentRegistration,
) (EnvironmentComponentComposeProjection, error) {
	if project == nil || ids.Validate(ids.KindEnvironment, environment.ID) != nil ||
		!filepath.IsAbs(environment.VolumeDir) || filepath.Clean(environment.VolumeDir) != environment.VolumeDir ||
		filepath.Base(environment.VolumeDir) != environment.ID {
		return EnvironmentComponentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Environment Component Compose input is invalid",
		)
	}
	rendered, err := componentrender.RenderEnvironmentComponents(environment, catalog)
	if err != nil {
		return EnvironmentComponentComposeProjection{}, err
	}
	projected := cloneComposeProjectServices(project)
	result := EnvironmentComponentComposeProjection{Project: projected}
	mountedFiles := make(map[string]map[string]struct{})

	for _, generated := range rendered.Services {
		if _, collision := projected.Services[generated.Name]; collision {
			return EnvironmentComponentComposeProjection{}, errs.New(
				errs.KindNameConflict,
				"Component generated Service conflicts with an active Compose service",
			)
		}
		if _, collision := projected.DisabledServices[generated.Name]; collision {
			return EnvironmentComponentComposeProjection{}, errs.New(
				errs.KindNameConflict,
				"Component generated Service conflicts with a disabled Compose service",
			)
		}
		platform, reference, selected := generated.Definition.Image.Select(runtime.GOOS, runtime.GOARCH)
		if !selected {
			return EnvironmentComponentComposeProjection{}, errs.New(
				errs.KindInternal,
				"Component image platform is unavailable",
			)
		}
		image := composeidentity.ComponentImage{Repository: generated.Definition.Image.Repository,
			IndexDigest: generated.Definition.Image.IndexDigest, Reference: reference, Platform: platform}
		service, environmentFile, err := projectEnvironmentComponentService(environment, generated, image)
		if err != nil {
			return EnvironmentComponentComposeProjection{}, err
		}
		for zoneName := range service.Networks {
			if _, exists := projected.Networks[zoneName]; !exists {
				return EnvironmentComponentComposeProjection{}, errs.New(
					errs.KindInternal,
					"Component generated Service Zone is absent from the Compose project",
				)
			}
		}
		for _, mount := range generated.Definition.Mounts {
			owners := mountedFiles[mount.Source]
			if owners == nil {
				owners = make(map[string]struct{})
				mountedFiles[mount.Source] = owners
			}
			owners[generated.ComponentID] = struct{}{}
		}
		if err := validateManagedNetworkAliases(projected, service); err != nil {
			return EnvironmentComponentComposeProjection{}, err
		}
		projected.Services[generated.Name] = service
		result.Services = append(result.Services, composeidentity.Resource{
			ID: generated.Definition.ID, Name: generated.Name, ComponentID: generated.ComponentID,
			ComponentImage: &image,
		})
		if environmentFile != nil {
			result.EnvironmentFiles = append(result.EnvironmentFiles, *environmentFile)
		}
	}
	for _, identity := range result.Services {
		for dependency := range projected.Services[identity.Name].DependsOn {
			if _, exists := projected.Services[dependency]; !exists {
				return EnvironmentComponentComposeProjection{}, errs.New(
					errs.KindInternal,
					"Component generated Service dependency is absent from the Compose project",
				)
			}
		}
	}

	for _, file := range rendered.Files {
		if !componentGeneratedFileIsMounted(file, mountedFiles) ||
			uint64(len(file.Content)) > entrymaterialization.MaximumContentBytes ||
			entrymaterialization.ValidateDesiredDestination(file.Path) != nil {
			return EnvironmentComponentComposeProjection{}, errs.New(
				errs.KindValidationFailed,
				"Component generated file is not a bounded mounted output",
			)
		}
		result.PlainFiles = append(result.PlainFiles, componentrender.GeneratedEnvironmentFile{
			ComponentID: file.ComponentID,
			Path:        file.Path,
			Content:     append([]byte(nil), file.Content...),
		})
	}
	return result, nil
}

func componentGeneratedFileIsMounted(
	file componentrender.GeneratedEnvironmentFile,
	mountedFiles map[string]map[string]struct{},
) bool {
	for source, owners := range mountedFiles {
		if _, owned := owners[file.ComponentID]; !owned {
			continue
		}
		if file.Path == source || strings.HasPrefix(file.Path, source+"/") {
			return true
		}
	}
	return false
}

func cloneComposeProjectServices(project *composetypes.Project) *composetypes.Project {
	cloned := *project
	cloned.Services = make(composetypes.Services, len(project.Services))
	for name, service := range project.Services {
		cloned.Services[name] = service
	}
	cloned.DisabledServices = make(composetypes.Services, len(project.DisabledServices))
	for name, service := range project.DisabledServices {
		cloned.DisabledServices[name] = service
	}
	return &cloned
}

func projectEnvironmentComponentService(
	environment core.Environment,
	generated componentrender.GeneratedEnvironmentService,
	image composeidentity.ComponentImage,
) (composetypes.ServiceConfig, *EnvironmentComponentEnvironmentFile, error) {
	definition := generated.Definition
	projected := composetypes.ServiceConfig{
		Name: definition.Name, Image: image.Reference,
		Command:  composetypes.ShellCommand(append([]string(nil), definition.Command...)),
		Networks: make(map[string]*composetypes.ServiceNetworkConfig, len(definition.Networks)),
		Expose:   composetypes.StringOrNumberList(append([]string(nil), definition.Expose...)),
		Restart:  definition.Restart,
	}
	if definition.NetworkMode == componentsdk.ManagedNetworkModeDefault {
		// Docker's built-in bridge needs no owned Zone. Leaving this empty
		// would let Compose create an unsealed project-default network.
		projected.NetworkMode = "bridge"
	}
	if health := definition.Healthcheck; health != nil {
		if err := health.Validate(); err != nil {
			return composetypes.ServiceConfig{}, nil, errs.New(
				errs.KindValidationFailed,
				"Component healthcheck is invalid",
			)
		}
		interval := composetypes.Duration(time.Duration(health.IntervalSeconds) * time.Second)
		timeout := composetypes.Duration(time.Duration(health.TimeoutSeconds) * time.Second)
		start := composetypes.Duration(time.Duration(health.StartPeriodSeconds) * time.Second)
		retries := uint64(health.Retries)
		projected.HealthCheck = &composetypes.HealthCheckConfig{
			Test:     append(composetypes.HealthCheckTest{"CMD"}, health.Command...),
			Interval: &interval, Timeout: &timeout, StartPeriod: &start, Retries: &retries,
		}
	}
	for _, network := range definition.Networks {
		if _, exists := environment.Zones[network.Name]; !exists {
			return composetypes.ServiceConfig{}, nil, errs.New(
				errs.KindInternal,
				"Component generated Service references a missing Environment Zone",
			)
		}
		projected.Networks[network.Name] = &composetypes.ServiceNetworkConfig{
			Aliases:         append([]string(nil), network.Aliases...),
			Ipv4Address:     network.StaticIPv4,
			GatewayPriority: network.GatewayPriority,
		}
	}
	for _, mount := range definition.Mounts {
		projected.Volumes = append(projected.Volumes, composetypes.ServiceVolumeConfig{
			Type:   composetypes.VolumeTypeBind,
			Source: filepath.Join(environment.VolumeDir, filepath.FromSlash(mount.Source)),
			Target: mount.Target, ReadOnly: mount.ReadOnly,
			Bind: &composetypes.ServiceVolumeBind{CreateHostPath: true},
		})
	}
	if len(definition.Dependencies) != 0 {
		projected.DependsOn = make(composetypes.DependsOnConfig, len(definition.Dependencies))
		for _, dependency := range definition.Dependencies {
			projected.DependsOn[dependency.ServiceName] = composetypes.ServiceDependency{
				Condition: string(dependency.Condition), Required: true,
			}
		}
	}
	if definition.Replicas > math.MaxInt {
		return composetypes.ServiceConfig{}, nil, errs.New(
			errs.KindInternal,
			"Component generated Service replica count exceeds the Compose integer range",
		)
	}
	replicas := int(definition.Replicas)
	projected.Deploy = &composetypes.DeployConfig{Replicas: &replicas}

	if len(definition.SecretEnvironment) == 0 {
		return projected, nil, nil
	}
	destination := environmentfile.ServiceEnvFileName(environment.ID, definition.Name)
	if entrymaterialization.ValidateDesiredDestination(destination) != nil {
		return composetypes.ServiceConfig{}, nil, errs.New(
			errs.KindInternal,
			"Component generated Environment destination is invalid",
		)
	}
	projected.EnvFiles = []composetypes.EnvFile{{
		Path: filepath.Join(environment.VolumeDir, filepath.FromSlash(destination)), Required: true,
	}}
	values := append([]componentsdk.ManagedSecretEnvironment(nil), definition.SecretEnvironment...)
	sort.Slice(values, func(left, right int) bool { return values[left].Name < values[right].Name })
	return projected, &EnvironmentComponentEnvironmentFile{
		ComponentID: generated.ComponentID,
		ServiceID:   definition.ID, ServiceName: definition.Name, Destination: destination,
		Values: values,
	}, nil
}

func bindEnvironmentComponentImage(
	identities []composeidentity.Resource,
	authored composetypes.ServiceConfig,
	service *agentpb.ComposeService,
) error {
	for _, identity := range identities {
		if identity.ID != service.GetServiceId() {
			continue
		}
		if identity.ComponentID == "" {
			if identity.ComponentImage != nil {
				return errs.New(errs.KindInternal, "native Service carries Component image authority")
			}
			return nil
		}
		if identity.ComponentImage == nil {
			return errs.New(errs.KindInternal, "Component Service image authority is absent")
		}
		image := *identity.ComponentImage
		platform, reference := image.Platform, image.Reference
		if validateSelectedComponentImage(image) != nil || authored.Image != reference {
			return errs.New(errs.KindInternal, "Component Service image differs from compiled authority")
		}
		service.ImageReference, service.ImageRepository = reference, image.Repository
		service.ImageIndexDigest = mustDecodePlatformDigest(image.IndexDigest)
		service.ImageChildDigest = mustDecodePlatformDigest(platform.ChildDigest)
		service.ImageConfigDigest = mustDecodePlatformDigest(platform.ConfigDigest)
		service.ImageOs, service.ImageArchitecture, service.ImageVariant = platform.OS, platform.Architecture, platform.Variant
		for key, value := range map[string]string{
			"com.groundplane.image-index-digest":  "sha256:" + image.IndexDigest,
			"com.groundplane.image-child-digest":  "sha256:" + platform.ChildDigest,
			"com.groundplane.image-config-digest": "sha256:" + platform.ConfigDigest,
			"com.groundplane.image-platform":      platform.OS + "/" + platform.Architecture + platformVariantSuffix(platform.Variant),
		} {
			authored.Labels[key] = value
			service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: value})
		}
		sort.Slice(service.ExpectedLabels, func(left, right int) bool {
			return service.ExpectedLabels[left].Key < service.ExpectedLabels[right].Key
		})
		return nil
	}
	return errs.New(errs.KindInternal, "Compose Service identity is absent")
}

func validateSelectedComponentImage(image composeidentity.ComponentImage) error {
	p := image.Platform
	if !validPlatformSHA256(image.IndexDigest) || !validPlatformSHA256(p.ChildDigest) ||
		!validPlatformSHA256(p.ConfigDigest) ||
		!imageref.IsDigestPinned(image.Reference) ||
		image.Reference != image.Repository+"@sha256:"+p.ChildDigest ||
		p.OS != "linux" ||
		p.Architecture != "amd64" && p.Architecture != "arm64" ||
		p.Architecture == "amd64" && p.Variant != "" ||
		p.Architecture == "arm64" && p.Variant != "" && p.Variant != "v8" {
		return errs.New(errs.KindInternal, "selected Component image authority is invalid")
	}
	return nil
}

func PinnedComponentServiceIdentity(
	service *agentpb.ComposeService,
	componentID string,
) (composeidentity.Resource, error) {
	image := composeidentity.ComponentImage{Repository: service.GetImageRepository(), Reference: service.GetImageReference(),
		IndexDigest: hex.EncodeToString(service.GetImageIndexDigest()), Platform: componentsdk.OCIPlatform{
			OS: service.GetImageOs(), Architecture: service.GetImageArchitecture(), Variant: service.GetImageVariant(),
			ChildDigest: hex.EncodeToString(
				service.GetImageChildDigest(),
			), ConfigDigest: hex.EncodeToString(service.GetImageConfigDigest()),
		}}
	if service.GetOwnerComponentId() != componentID || validateSelectedComponentImage(image) != nil {
		return composeidentity.Resource{}, errs.New(errs.KindInternal, "pinned Component image authority is invalid")
	}
	return composeidentity.Resource{ID: service.GetServiceId(), Name: service.GetComposeName(),
		ComponentID: componentID, ComponentImage: &image}, nil
}
