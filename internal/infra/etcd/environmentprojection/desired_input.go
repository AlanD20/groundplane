package environmentprojection

import (
	"net/netip"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentDesiredInput is the immutable normalized operator input selected
// by one Environment desired head. It deliberately has no effective Compose
// artifact: fact-dependent runtime is compiled only after its producing work
// completes and is stored through the separate runtime projection boundary.
type EnvironmentDesiredInput struct {
	EnvironmentID    string                     `json:"environment_id"`
	RevisionID       string                     `json:"revision_id"`
	RenderGeneration uint64                     `json:"render_generation"`
	Input            core.BlueprintDesiredInput `json:"input"`
}

func NewEnvironmentDesiredInput(
	environmentID string,
	revisionID string,
	renderGeneration uint64,
	input core.BlueprintDesiredInput,
) (EnvironmentDesiredInput, error) {
	value := EnvironmentDesiredInput{
		EnvironmentID: environmentID, RevisionID: revisionID,
		RenderGeneration: renderGeneration, Input: core.CloneBlueprintDesiredInput(input),
	}
	if err := ValidateEnvironmentDesiredInput(value); err != nil {
		return EnvironmentDesiredInput{}, err
	}
	return value, nil
}

func CloneEnvironmentDesiredInput(source EnvironmentDesiredInput) EnvironmentDesiredInput {
	clone := source
	clone.Input = core.CloneBlueprintDesiredInput(source.Input)
	return clone
}

func ValidateEnvironmentDesiredInput(value EnvironmentDesiredInput) error {
	if recordcodec.ValidateID(ids.KindEnvironment, value.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, value.RevisionID) != nil || value.RenderGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Environment desired input identity is invalid")
	}
	input := value.Input
	if err := ValidateEnvironmentNormalizedCompose(input.NormalizedCompose); err != nil {
		return err
	}
	if err := core.ValidateNormalizedBlueprintFiles(input.RuntimeFiles); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	pool, err := netip.ParsePrefix(input.NetworkPool)
	if err != nil || !pool.Addr().Is4() || pool != pool.Masked() || pool.String() != input.NetworkPool {
		return errs.New(errs.KindValidationFailed, "Environment desired network pool is invalid")
	}
	if err := validateDesiredRequirements(input.Requires); err != nil {
		return err
	}
	if err := validateDesiredServiceExtensions(input.ServiceExtensions); err != nil {
		return err
	}
	if err := validateDesiredAttachments(input.Attachments); err != nil {
		return err
	}
	if err := validateDesiredEntries(input.Entries); err != nil {
		return err
	}
	if err := validateDesiredRoutes(input.Routes); err != nil {
		return err
	}
	if err := validateDesiredScripts(input.Scripts); err != nil {
		return err
	}
	if err := validateDesiredComponents(input.Components); err != nil {
		return err
	}
	if err := validateDesiredBackup(input.Backup); err != nil {
		return err
	}
	return validateDesiredReleaseGroups(input.ReleaseGroups)
}

func EncodeEnvironmentDesiredInputStorage(value EnvironmentDesiredInput) ([]byte, error) {
	if err := ValidateEnvironmentDesiredInput(value); err != nil {
		return nil, err
	}
	encoded, err := recordcodec.Encode("environment-desired-input", value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > EnvironmentBlueprintProjectionMaxBytes {
		clear(encoded)
		return nil, errs.New(errs.KindValidationFailed, "Environment desired input exceeds the 2 MiB ceiling")
	}
	return encoded, nil
}

func DecodeEnvironmentDesiredInputStorage(encoded []byte) (EnvironmentDesiredInput, error) {
	value, err := recordcodec.Decode[EnvironmentDesiredInput](encoded, "environment-desired-input")
	if err != nil {
		return EnvironmentDesiredInput{}, err
	}
	if err := ValidateEnvironmentDesiredInput(value); err != nil {
		return EnvironmentDesiredInput{}, CorruptEnvironmentDesiredInput()
	}
	return value, nil
}

func CorruptEnvironmentDesiredInput() error {
	return errs.New(errs.KindInternal, "Environment desired input is corrupt")
}

func validateDesiredRequirements(values []core.Requirement) error {
	if len(values) > 512 {
		return errs.New(errs.KindValidationFailed, "Environment desired requirements exceed the supported bound")
	}
	seen := make(map[core.RequirementTarget]struct{}, len(values))
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[value.Target]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired requirement is duplicated")
		}
		seen[value.Target] = struct{}{}
	}
	return nil
}

func validateDesiredServiceExtensions(values map[string]core.ServiceExtensionSpec) error {
	if len(values) > 512 {
		return errs.New(errs.KindValidationFailed, "Environment desired Service extensions exceed the supported bound")
	}
	for service, extension := range values {
		if service == "" {
			return errs.New(errs.KindValidationFailed, "Environment desired Service extension target is invalid")
		}
		if extension.Release != nil {
			probe := core.Service{
				ID: "desired", Name: service, Image: "desired",
				Strategy: extension.Release.DefaultStrategy, OnFailure: extension.Release.OnFailure,
			}
			if extension.Release.DefaultStrategy == "" ||
				extension.Release.OnFailure != extension.Release.OnFailure.WithDefault() || probe.Validate() != nil {
				return errs.New(errs.KindValidationFailed, "Environment desired Service release policy is invalid")
			}
		}
		for dependency, decision := range extension.DependsOn {
			if dependency == "" || dependency == service || decision.Validate() != nil ||
				!slices.IsSorted(decision.Phases) {
				return errs.New(errs.KindValidationFailed, "Environment desired Service dependency is invalid")
			}
		}
	}
	return nil
}

func validateDesiredAttachments(values map[string]core.AttachmentSpec) error {
	if len(values) > 512 {
		return errs.New(errs.KindValidationFailed, "Environment desired Attachments exceed the supported bound")
	}
	for name, attachment := range values {
		if name == "" || attachment.BackingProject == "" || attachment.BackingService == "" ||
			attachment.Service == "" {
			return errs.New(errs.KindValidationFailed, "Environment desired Attachment identity is invalid")
		}
		switch attachment.Credential.Mode {
		case "new":
			if attachment.Credential.Attach != "" || len(attachment.Grants) > 8 {
				return errs.New(errs.KindValidationFailed, "Environment desired Attachment credential is invalid")
			}
			seen := make(map[string]struct{}, len(attachment.Grants))
			for _, grant := range attachment.Grants {
				if grant == "" {
					return errs.New(errs.KindValidationFailed, "Environment desired Attachment grant is invalid")
				}
				if _, duplicate := seen[grant]; duplicate {
					return errs.New(errs.KindValidationFailed, "Environment desired Attachment grant is duplicated")
				}
				seen[grant] = struct{}{}
			}
		case "existing":
			owner, exists := values[attachment.Credential.Attach]
			if attachment.Credential.Attach == "" || len(attachment.Grants) != 0 || !exists ||
				owner.Credential.Mode != "new" || owner.BackingProject != attachment.BackingProject ||
				owner.BackingService != attachment.BackingService {
				return errs.New(errs.KindValidationFailed, "Environment desired Attachment credential owner is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Environment desired Attachment credential mode is invalid")
		}
	}
	return nil
}

func validateDesiredEntries(values map[string]core.EntrySpec) error {
	if len(values) > 512 {
		return errs.New(errs.KindValidationFailed, "Environment desired Entries exceed the supported bound")
	}
	for key, entry := range values {
		if entry.Secret && entry.Source.Literal != "" {
			return errs.New(errs.KindValidationFailed, "Environment desired input contains secret plaintext")
		}
		if _, err := core.ProjectEntrySpec(key, entry, "ev_00000000000000000000000000"); err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	return nil
}

func validateDesiredRoutes(values []core.RouteSpec) error {
	if len(values) > 512 {
		return errs.New(errs.KindValidationFailed, "Environment desired Routes exceed the supported bound")
	}
	for _, route := range values {
		if route.Target == "" || (core.Route{
			Host: route.Hostname, Path: route.Path, TargetServiceID: "desired",
			TargetPort: route.TargetPort, Exposure: route.Exposure,
		}).Validate() != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Route is invalid")
		}
	}
	return nil
}

func validateDesiredScripts(values map[string]core.ScriptSpec) error {
	if len(values) > 64 {
		return errs.New(errs.KindValidationFailed, "Environment desired Scripts exceed the supported bound")
	}
	slugs := make(map[string]struct{}, len(values))
	for key, script := range values {
		probe := core.Script{
			ID: "desired", Slug: script.Slug, ServiceName: script.Service,
			Body: script.Script, When: script.When, Order: script.Order,
		}
		if core.ValidateScriptLabel("script reconciliation key", key) != nil || probe.Validate() != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Script is invalid")
		}
		if _, duplicate := slugs[script.Slug]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Script slug is duplicated")
		}
		slugs[script.Slug] = struct{}{}
		if script.Execution != nil {
			if err := script.Execution.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateDesiredComponents(values map[string]core.ComponentSpec) error {
	if len(values) > 2 {
		return errs.New(errs.KindValidationFailed, "Environment desired Components exceed the supported bound")
	}
	for capability, component := range values {
		switch core.ComponentCapability(capability) {
		case core.ComponentCapabilityHTTPRouter:
			if component.Implementation != core.ComponentKindIngressCaddy || component.Settings.SecretID != "" ||
				(component.Enabled && len(component.Settings.ZoneIDs) == 0) {
				return errs.New(errs.KindValidationFailed, "Environment desired HTTP router Component is invalid")
			}
		case core.ComponentCapabilityEdgeTunnel:
			if component.Implementation != core.ComponentKindEdgeCloudflare ||
				component.ImplementationConfig.CaddyfileTemplate != "" || component.Settings.Alias != "" ||
				(component.Enabled && (component.Settings.SecretID == "" || len(component.Settings.ZoneIDs) == 0)) {
				return errs.New(errs.KindValidationFailed, "Environment desired edge tunnel Component is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Environment desired Component capability is invalid")
		}
		seenZones := make(map[string]struct{}, len(component.Settings.ZoneIDs))
		for _, zoneID := range component.Settings.ZoneIDs {
			if recordcodec.ValidateID(ids.KindNetwork, zoneID) != nil {
				return errs.New(errs.KindValidationFailed, "Environment desired Component Zone is invalid")
			}
			if _, duplicate := seenZones[zoneID]; duplicate {
				return errs.New(errs.KindValidationFailed, "Environment desired Component Zone is duplicated")
			}
			seenZones[zoneID] = struct{}{}
		}
	}
	return nil
}

func validateDesiredBackup(value *core.BackupSpec) error {
	if value == nil {
		return nil
	}
	if !value.Enabled && value.Frequency == "" && value.Keep == 0 && value.Encryption == "" &&
		value.Connector == "" && len(value.Sources) == 0 {
		return nil
	}
	if value.Frequency == "" || value.Keep < 1 || value.Keep > 9_007_199_254_740_991 ||
		value.Encryption == "" || len(value.Sources) == 0 || value.Enabled && value.Connector == "" {
		return errs.New(errs.KindValidationFailed, "Environment desired Backup policy is invalid")
	}
	seen := make(map[string]struct{}, len(value.Sources))
	for _, source := range value.Sources {
		switch source.Kind {
		case core.BackupSourceAttach, core.BackupSourceVolume:
			if source.Ref == "" {
				return errs.New(errs.KindValidationFailed, "Environment desired Backup source is invalid")
			}
		case core.BackupSourceConfig:
			if source.Ref != "" {
				return errs.New(errs.KindValidationFailed, "Environment desired Backup config source is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Environment desired Backup source kind is invalid")
		}
		key := string(source.Kind) + "\x00" + source.Ref
		if _, duplicate := seen[key]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Backup source is duplicated")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateDesiredReleaseGroups(values map[string]core.ReleaseGroupSpec) error {
	if len(values) > 512 {
		return errs.New(errs.KindValidationFailed, "Environment desired Release Groups exceed the supported bound")
	}
	for name, group := range values {
		if err := group.Validate(name); err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
		if len(group.Order) == 0 || group.OnFailure != group.OnFailure.WithDefault() {
			return errs.New(errs.KindValidationFailed, "Environment desired Release Group is not normalized")
		}
	}
	return nil
}
