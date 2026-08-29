package etcd

import (
	"context"
	"encoding/hex"
	"net"
	"net/netip"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
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
	PlanID             string                      `json:"plan_id"`
	TaskID             string                      `json:"task_id"`
	ComponentID        string                      `json:"component_id"`
	DesiredSHA256      string                      `json:"desired_sha256"`
	BaselineGeneration uint64                      `json:"baseline_generation"`
	BaselineSHA256     string                      `json:"baseline_sha256"`
	Config             core.CoreDNSComponentConfig `json:"config"`
	Hosts              []PlatformDNSHost           `json:"hosts,omitempty"`
	GeneratedServiceID string                      `json:"generated_service_id"`
	EnsureService      bool                        `json:"ensure_service"`
	DisableService     bool                        `json:"disable_service"`
	DefinitionSHA256   string                      `json:"definition_sha256"`
	CatalogSHA256      string                      `json:"catalog_sha256"`
	ActionID           string                      `json:"action_id"`
	ArtifactID         string                      `json:"artifact_id"`
	ComposeArtifactID  string                      `json:"compose_artifact_id"`
	ArtifactSHA256     string                      `json:"artifact_sha256"`
	ArtifactLength     uint64                      `json:"artifact_length"`
}

func platformComponentTaskRenderInputKey(planID string) string {
	return platformComponentTaskRenderInputPrefix + planID
}

func (repository *ComponentRepository) GetPlatformComponentTaskRenderInput(
	ctx context.Context,
	planID string,
) (Versioned[PlatformComponentTaskRenderInput], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[PlatformComponentTaskRenderInput]{}, err
	}
	if ids.Validate(ids.KindPlan, planID) != nil {
		return Versioned[PlatformComponentTaskRenderInput]{}, errs.New(
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
	value, err := encodeEnvelope("platform_component_task_render_input", input)
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
	input, err := decodeEnvelope[PlatformComponentTaskRenderInput](value, "platform_component_task_render_input")
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
		ids.Validate(ids.KindService, input.GeneratedServiceID) != nil ||
		ids.Validate(ids.KindConfig, input.ArtifactID) != nil ||
		ids.Validate(ids.KindConfig, input.ComposeArtifactID) != nil || input.BaselineGeneration != 1 ||
		!validSHA256(input.DesiredSHA256) || !validSHA256(input.BaselineSHA256) ||
		!validSHA256(input.DefinitionSHA256) || !validSHA256(input.CatalogSHA256) ||
		!validSHA256(input.ArtifactSHA256) || input.ArtifactLength == 0 ||
		input.ArtifactLength > managedconfig.MaximumArtifactBytes || !validComponentActionToken(input.ActionID) ||
		input.EnsureService && input.DisableService {
		return errs.New(errs.KindValidationFailed, "platform Component render-input identity is invalid")
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
	for index, host := range input.Hosts {
		clone.Hosts[index] = PlatformDNSHost{
			Address: host.Address, Hostnames: append([]string(nil), host.Hostnames...),
		}
	}
	return clone
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
