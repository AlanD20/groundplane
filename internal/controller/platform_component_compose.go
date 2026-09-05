package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"path/filepath"
	"strconv"
	"strings"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

const (
	PlatformComposeProjectName = "groundplane-infra"
	PlatformConfigRoot         = "/etc/groundplane"
)

type PlatformComponentComposeInput struct {
	ComponentID       string
	PlanID            string
	RenderGeneration  uint64
	ArtifactID        string
	Plan              componentsdk.EnvironmentPlan
	ImageRepository   string
	ImageIndexDigest  string
	ImageConfigDigest string
	ImageChildDigest  string
	ImageReference    string
	ImageOS           string
	ImageArchitecture string
	ImageVariant      string
}

func RenderPlatformComponentCompose(
	input PlatformComponentComposeInput,
) (*agentpb.ComposeArtifact, error) {
	if ids.Validate(ids.KindComponent, input.ComponentID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindConfig, input.ArtifactID) != nil ||
		input.RenderGeneration == 0 || len(input.Plan.Services) != 1 || len(input.Plan.Files) != 1 {
		return nil, errs.New(errs.KindInternal, "platform Component Compose input is invalid")
	}
	service := input.Plan.Services[0]
	file := input.Plan.Files[0]
	if ids.Validate(ids.KindService, service.ID) != nil || service.Name == "" ||
		input.ImageReference != input.ImageRepository+"@sha256:"+input.ImageChildDigest ||
		service.Image.Repository != input.ImageRepository || service.Image.IndexDigest != input.ImageIndexDigest ||
		!validPlatformSHA256(input.ImageConfigDigest) || input.ImageConfigDigest == input.ImageIndexDigest ||
		input.ImageConfigDigest == input.ImageChildDigest || input.ImageIndexDigest == input.ImageChildDigest ||
		len(service.Networks) != 0 || len(service.Mounts) != 1 ||
		!service.Mounts[0].ReadOnly || service.Replicas != 1 || !validGeneratedRelativePath(file.Path) ||
		len(file.Content) == 0 || service.ObservationAction == "" {
		return nil, errs.New(errs.KindValidationFailed, "platform Component managed Service is invalid")
	}
	mount := service.Mounts[0]
	if mount.Kind != componentsdk.ManagedMountKindDirectory || !filepath.IsAbs(mount.Target) ||
		filepath.Clean(mount.Target) != mount.Target || mount.Target == "/" ||
		!strings.HasPrefix(file.Path, strings.TrimSuffix(mount.Source, "/")+"/") {
		return nil, errs.New(errs.KindValidationFailed, "platform Component managed mount is invalid")
	}
	if service.Replicas > math.MaxInt {
		return nil, errs.New(errs.KindInternal, "platform Component replica count exceeds Compose limits")
	}
	labels := map[string]string{
		composeLabelKind:                      "service",
		composeLabelManaged:                   "true",
		composeLabelPlanID:                    input.PlanID,
		composeLabelRenderGen:                 strconv.FormatUint(input.RenderGeneration, 10),
		composeLabelServiceID:                 service.ID,
		"com.groundplane.image-index-digest":  "sha256:" + input.ImageIndexDigest,
		"com.groundplane.image-child-digest":  "sha256:" + input.ImageChildDigest,
		"com.groundplane.image-config-digest": "sha256:" + input.ImageConfigDigest,
		"com.groundplane.image-platform": input.ImageOS + "/" + input.ImageArchitecture + platformVariantSuffix(
			input.ImageVariant,
		),
	}
	replicas := int(service.Replicas)
	project := &composetypes.Project{
		Name: PlatformComposeProjectName,
		Services: composetypes.Services{
			service.Name: {
				Name: service.Name, Image: input.ImageReference,
				Command:     composetypes.ShellCommand(append([]string(nil), service.Command...)),
				NetworkMode: "host", Restart: service.Restart, Labels: labels,
				Deploy: &composetypes.DeployConfig{Replicas: &replicas},
				Volumes: []composetypes.ServiceVolumeConfig{{
					Type:   composetypes.VolumeTypeBind,
					Source: filepath.Join(PlatformConfigRoot, filepath.FromSlash(mount.Source)),
					Target: mount.Target, ReadOnly: true,
					Bind: &composetypes.ServiceVolumeBind{CreateHostPath: false},
				}},
			},
		},
	}
	canonical, err := project.MarshalYAML()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(canonical)
	expectedLabels := []*agentpb.LabelPair{
		{Key: "com.groundplane.image-child-digest", Value: "sha256:" + input.ImageChildDigest},
		{Key: "com.groundplane.image-config-digest", Value: "sha256:" + input.ImageConfigDigest},
		{Key: "com.groundplane.image-index-digest", Value: "sha256:" + input.ImageIndexDigest},
		{
			Key:   "com.groundplane.image-platform",
			Value: input.ImageOS + "/" + input.ImageArchitecture + platformVariantSuffix(input.ImageVariant),
		},
		{Key: composeLabelKind, Value: "service"},
		{Key: composeLabelManaged, Value: "true"},
		{Key: composeLabelPlanID, Value: input.PlanID},
		{Key: composeLabelRenderGen, Value: strconv.FormatUint(input.RenderGeneration, 10)},
		{Key: composeLabelServiceID, Value: service.ID},
	}
	return &agentpb.ComposeArtifact{
		ArtifactId:    input.ArtifactID,
		OwnerKind:     agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
		ProjectName:   PlatformComposeProjectName,
		CanonicalYaml: canonical, YamlSha256: digest[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: service.ID, ComposeName: service.Name,
			ExpectedLabels: expectedLabels, ExpectedReplicas: uint32(service.Replicas), HasHealthcheck: false,
			ImageReference: input.ImageReference, ImageRepository: input.ImageRepository,
			ImageIndexDigest:  mustDecodePlatformDigest(input.ImageIndexDigest),
			ImageConfigDigest: mustDecodePlatformDigest(input.ImageConfigDigest),
			ImageChildDigest:  mustDecodePlatformDigest(input.ImageChildDigest), ImageOs: input.ImageOS,
			ImageArchitecture: input.ImageArchitecture, ImageVariant: input.ImageVariant,
		}},
	}, nil
}

func validPlatformSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value == strings.Repeat("0", sha256.Size*2) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func platformVariantSuffix(variant string) string {
	if variant == "" {
		return ""
	}
	return "/" + variant
}

func mustDecodePlatformDigest(value string) []byte {
	decoded, _ := hex.DecodeString(value)
	return decoded
}
