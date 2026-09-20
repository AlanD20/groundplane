package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net"
	"net/netip"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	platformComponentTaskRenderInputPrefix = "/v1/records/platform-component-task-render-inputs/"
	maximumPlatformComponentRenderBytes    = 256 * 1024
)

type PlatformDNSHost struct {
	Address   string   `json:"address"`
	Hostnames []string `json:"hostnames"`
}

type PlatformComponentTaskRenderInput struct {
	PlanID                         string                      `json:"plan_id"`
	TaskID                         string                      `json:"task_id"`
	ComponentID                    string                      `json:"component_id"`
	DesiredSHA256                  string                      `json:"desired_sha256"`
	BaselineGeneration             uint64                      `json:"baseline_generation"`
	BaselineSHA256                 string                      `json:"baseline_sha256"`
	HostResolutionInputRevision    int64                       `json:"host_resolution_input_revision"`
	HostResolutionSHA256           string                      `json:"host_resolution_sha256"`
	Config                         core.CoreDNSComponentConfig `json:"config"`
	Hosts                          []PlatformDNSHost           `json:"hosts,omitempty"`
	GeneratedServiceID             string                      `json:"generated_service_id"`
	EnsureService                  bool                        `json:"ensure_service"`
	DisableService                 bool                        `json:"disable_service"`
	DefinitionSHA256               string                      `json:"definition_sha256"`
	CatalogSHA256                  string                      `json:"catalog_sha256"`
	ActionID                       string                      `json:"action_id"`
	ArtifactID                     string                      `json:"artifact_id"`
	ComposeArtifactID              string                      `json:"compose_artifact_id"`
	ComposeArtifact                *agentpb.ComposeArtifact    `json:"compose_artifact,omitempty"`
	RollbackComposeArtifact        *agentpb.ComposeArtifact    `json:"rollback_compose_artifact,omitempty"`
	OwnershipPlanID                string                      `json:"ownership_plan_id"`
	OwnershipGeneration            uint64                      `json:"ownership_generation"`
	PriorObservationModRevision    int64                       `json:"prior_observation_mod_revision"`
	PriorObservationRevision       uint64                      `json:"prior_observation_revision"`
	PredecessorTaskID              string                      `json:"predecessor_task_id,omitempty"`
	ExpectedPreviousArtifactSHA256 string                      `json:"expected_previous_artifact_sha256,omitempty"`
	ExpectedPreviousArtifactID     string                      `json:"expected_previous_artifact_id,omitempty"`
	ExpectedPreviousGeneration     uint64                      `json:"expected_previous_generation,omitempty"`
	ImageRepository                string                      `json:"image_repository"`
	ImageIndexDigest               string                      `json:"image_index_digest"`
	ImageConfigDigest              string                      `json:"image_config_digest"`
	ImageOS                        string                      `json:"image_os"`
	ImageArchitecture              string                      `json:"image_architecture"`
	ImageVariant                   string                      `json:"image_variant,omitempty"`
	ImageChildDigest               string                      `json:"image_child_digest"`
	ImageReference                 string                      `json:"image_reference"`
	ArtifactSHA256                 string                      `json:"artifact_sha256"`
	ArtifactLength                 uint64                      `json:"artifact_length"`
	PlanSHA256                     string                      `json:"plan_sha256"`
	ExecutionPlanSHA256            string                      `json:"execution_plan_sha256"`
}

func platformComponentTaskRenderInputKey(planID string) string {
	return platformComponentTaskRenderInputPrefix + planID
}

func (repository *ComponentRepository) GetPlatformComponentTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcdstore.Versioned[PlatformComponentTaskRenderInput], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[PlatformComponentTaskRenderInput]{}, err
	}
	if ids.Validate(ids.KindPlan, planID) != nil {
		return etcdstore.Versioned[PlatformComponentTaskRenderInput]{}, errs.New(
			errs.KindValidationFailed,
			"platform Component render-input Plan id is invalid",
		)
	}
	return getRecord(
		ctx,
		repository.store,
		platformComponentTaskRenderInputKey(planID),
		planID,
		errs.KindTaskNotFound,
		decodePlatformComponentTaskRenderInput,
		func(record PlatformComponentTaskRenderInput) string { return record.PlanID },
	)
}

func encodePlatformComponentTaskRenderInput(input PlatformComponentTaskRenderInput) ([]byte, error) {
	if err := validatePlatformComponentTaskRenderInput(input); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("platform_component_task_render_input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumPlatformComponentRenderBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "platform Component render input exceeds size limit")
	}
	return value, nil
}

func decodePlatformComponentTaskRenderInput(value []byte) (PlatformComponentTaskRenderInput, error) {
	input, err := recordcodec.Decode[PlatformComponentTaskRenderInput](value, "platform_component_task_render_input")
	if err != nil || validatePlatformComponentTaskRenderInput(input) != nil {
		return PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindInternal,
			"platform Component render input is corrupt",
		)
	}
	return clonePlatformComponentTaskRenderInput(input), nil
}

func validatePlatformComponentTaskRenderInput(input PlatformComponentTaskRenderInput) error {
	if ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindTask, input.TaskID) != nil ||
		ids.Validate(ids.KindComponent, input.ComponentID) != nil ||
		ids.Validate(ids.KindService, input.GeneratedServiceID) != nil || input.HostResolutionInputRevision <= 0 ||
		ids.Validate(ids.KindConfig, input.ArtifactID) != nil ||
		ids.Validate(ids.KindConfig, input.ComposeArtifactID) != nil || input.BaselineGeneration != 1 ||
		ids.Validate(ids.KindPlan, input.OwnershipPlanID) != nil || input.OwnershipGeneration == 0 ||
		input.PriorObservationModRevision < 0 ||
		(input.PriorObservationModRevision > 0 && input.PriorObservationRevision == 0) ||
		(input.PredecessorTaskID != "" && ids.Validate(ids.KindTask, input.PredecessorTaskID) != nil) ||
		(input.PriorObservationRevision == 0 && input.PredecessorTaskID != "") ||
		(input.PriorObservationRevision > 0 && input.PredecessorTaskID == "") ||
		(input.PriorObservationRevision == 0 && input.ExpectedPreviousArtifactSHA256 != "") ||
		(input.ExpectedPreviousArtifactSHA256 != "" && !recordcodec.ValidSHA256(input.ExpectedPreviousArtifactSHA256)) ||
		(input.ExpectedPreviousArtifactSHA256 == "") != (input.ExpectedPreviousArtifactID == "") ||
		(input.ExpectedPreviousArtifactSHA256 == "") != (input.ExpectedPreviousGeneration == 0) ||
		(input.ExpectedPreviousArtifactID != "" && ids.Validate(ids.KindConfig, input.ExpectedPreviousArtifactID) != nil) ||
		!recordcodec.ValidSHA256(
			input.DesiredSHA256,
		) || !recordcodec.ValidSHA256(input.BaselineSHA256) || !recordcodec.ValidSHA256(input.HostResolutionSHA256) ||
		!recordcodec.ValidSHA256(input.DefinitionSHA256) || !recordcodec.ValidSHA256(input.CatalogSHA256) ||
		!recordcodec.ValidSHA256(input.ArtifactSHA256) || input.ArtifactLength == 0 ||
		!recordcodec.ValidSHA256(input.PlanSHA256) || !recordcodec.ValidSHA256(input.ExecutionPlanSHA256) ||
		!recordcodec.ValidSHA256(input.ImageIndexDigest) || !validNonZeroSHA256(input.ImageConfigDigest) ||
		input.ImageConfigDigest == input.ImageIndexDigest || input.ImageConfigDigest == input.ImageChildDigest ||
		input.ImageIndexDigest == input.ImageChildDigest ||
		!recordcodec.ValidSHA256(input.ImageChildDigest) || !validSelectedPlatform(input) ||
		input.ArtifactLength > managedconfig.MaximumArtifactBytes || !validComponentActionToken(input.ActionID) ||
		input.EnsureService && input.DisableService {
		return errs.New(errs.KindValidationFailed, "platform Component render-input identity is invalid")
	}
	if input.ComposeArtifact != nil && !platformComponentArtifactMatches(
		input.ComposeArtifact,
		input.ComposeArtifactID,
		input.GeneratedServiceID,
		input.ImageConfigDigest,
	) {
		return errs.New(errs.KindValidationFailed, "platform Component Compose artifact is invalid")
	}
	if input.RollbackComposeArtifact != nil && (!input.EnsureService || input.DisableService ||
		input.ExpectedPreviousArtifactSHA256 == "" || input.ComposeArtifact == nil ||
		input.RollbackComposeArtifact.GetArtifactId() == input.ComposeArtifact.GetArtifactId() ||
		!platformComponentArtifactMatches(
			input.RollbackComposeArtifact,
			input.RollbackComposeArtifact.GetArtifactId(),
			input.GeneratedServiceID,
			"",
		)) {
		return errs.New(errs.KindValidationFailed, "platform Component rollback Compose artifact is invalid")
	}
	if input.EnsureService && input.ExpectedPreviousArtifactSHA256 != "" && input.RollbackComposeArtifact == nil {
		return errs.New(errs.KindValidationFailed, "platform Component serving predecessor artifact is missing")
	}
	if input.Config.UpstreamAuto && len(input.Config.UpstreamResolvers) != 0 ||
		!input.Config.UpstreamAuto && len(input.Config.UpstreamResolvers) == 0 {
		return errs.New(errs.KindValidationFailed, "platform Component resolver mode is invalid")
	}
	previousAddress := netip.Addr{}
	seenNames := make(map[string]netip.Addr)
	for _, host := range input.Hosts {
		address, err := netip.ParseAddr(host.Address)
		if err != nil || !address.Is4() || address.Is4In6() || address.IsUnspecified() || address.IsMulticast() ||
			previousAddress.IsValid() && address.Compare(previousAddress) <= 0 || len(host.Hostnames) == 0 {
			return errs.New(errs.KindValidationFailed, "platform Component DNS hosts are invalid or unsorted")
		}
		previousAddress = address
		previousName := ""
		for _, hostname := range host.Hostnames {
			if !validPlatformDNSName(hostname) || hostname <= previousName {
				return errs.New(errs.KindValidationFailed, "platform Component DNS names are invalid or unsorted")
			}
			if existing, found := seenNames[hostname]; found && existing != address {
				return errs.New(errs.KindValidationFailed, "platform Component DNS name maps to multiple addresses")
			}
			seenNames[hostname] = address
			previousName = hostname
		}
	}
	return nil
}

func validNonZeroSHA256(value string) bool {
	return recordcodec.ValidSHA256(value) && value != strings.Repeat("0", sha256.Size*2)
}

func validSelectedPlatform(input PlatformComponentTaskRenderInput) bool {
	if input.ImageRepository == "" || strings.ContainsAny(input.ImageRepository, "@ ") ||
		input.ImageOS != "linux" || input.ImageReference != input.ImageRepository+"@sha256:"+input.ImageChildDigest {
		return false
	}
	return input.ImageArchitecture == "amd64" && input.ImageVariant == "" ||
		input.ImageArchitecture == "arm64" && (input.ImageVariant == "" || input.ImageVariant == "v8")
}

func validComponentActionToken(value string) bool {
	if value == "" || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if !(character == '-' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
			return false
		}
	}
	return true
}

func validPlatformDNSName(value string) bool {
	if value == "" || value == "." || len(value) > 253 || strings.HasPrefix(value, ".") ||
		strings.HasSuffix(value, ".") || strings.HasPrefix(value, "*.") || net.ParseIP(value) != nil {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !(character == '-' || character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' || character >= '0' && character <= '9') {
				return false
			}
		}
	}
	return true
}

func clonePlatformComponentTaskRenderInput(input PlatformComponentTaskRenderInput) PlatformComponentTaskRenderInput {
	clone := input
	clone.Config = *core.CloneComponentConfig(core.ComponentConfig{CoreDNS: &input.Config}).CoreDNS
	clone.Hosts = make([]PlatformDNSHost, len(input.Hosts))
	if input.ComposeArtifact != nil {
		clone.ComposeArtifact = proto.Clone(input.ComposeArtifact).(*agentpb.ComposeArtifact)
	}
	if input.RollbackComposeArtifact != nil {
		clone.RollbackComposeArtifact = proto.Clone(input.RollbackComposeArtifact).(*agentpb.ComposeArtifact)
	}
	for index, host := range input.Hosts {
		clone.Hosts[index] = PlatformDNSHost{
			Address: host.Address, Hostnames: append([]string(nil), host.Hostnames...),
		}
	}
	return clone
}

func platformComponentArtifactMatches(
	artifact *agentpb.ComposeArtifact,
	artifactID string,
	serviceID string,
	imageConfigDigest string,
) bool {
	if artifact == nil || artifact.GetArtifactId() != artifactID ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM || artifact.GetOwnerId() != "" ||
		artifact.GetProjectName() != "groundplane-infra" || len(artifact.GetServices()) != 1 ||
		artifact.GetServices()[0].GetServiceId() != serviceID ||
		!validNonZeroDigestBytes(artifact.GetServices()[0].GetImageConfigDigest()) {
		return false
	}
	return imageConfigDigest == "" ||
		hex.EncodeToString(artifact.GetServices()[0].GetImageConfigDigest()) == imageConfigDigest
}

func validNonZeroDigestBytes(value []byte) bool {
	if len(value) != sha256.Size {
		return false
	}
	var nonzero byte
	for _, part := range value {
		nonzero |= part
	}
	return nonzero != 0
}

func canonicalPlatformDNSHosts(hosts []PlatformDNSHost) []PlatformDNSHost {
	result := make([]PlatformDNSHost, len(hosts))
	for index, host := range hosts {
		result[index] = PlatformDNSHost{Address: host.Address, Hostnames: append([]string(nil), host.Hostnames...)}
		sort.Strings(result[index].Hostnames)
	}
	sort.Slice(result, func(left, right int) bool {
		leftAddress := netip.MustParseAddr(result[left].Address)
		rightAddress := netip.MustParseAddr(result[right].Address)
		return leftAddress.Compare(rightAddress) < 0
	})
	return result
}

func decodeSHA256(value string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) {
		return digest, errs.New(errs.KindInternal, "platform Component digest is corrupt")
	}
	copy(digest[:], decoded)
	return digest, nil
}
