package executionplan

import (
	"bytes"
	"context"
	"crypto/sha256"
	"maps"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/loader"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// ValidateAttachConsumerTransition proves a network-only mutation of captured
// native runtime. Desired Service edits are not a source for this operation.
func ValidateAttachConsumerTransition(ctx context.Context, input *agentpb.AttachConsumerTransition) error {
	if ctx == nil || input == nil || RejectUnknown(input) != nil || proto.Size(input) > MaximumPlanBytes ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil ||
		ids.Validate(ids.KindEnvironment, input.BackingEnvironmentId) != nil ||
		ids.Validate(ids.KindService, input.ServiceId) != nil {
		return invalidAttachConsumer()
	}
	if _, err := networkname.New(input.NetworkId); err != nil {
		return invalidAttachConsumer()
	}
	for _, artifact := range []*agentpb.ComposeArtifact{input.Baseline, input.Candidate} {
		if err := validateArtifactContent(artifact); err != nil {
			return err
		}
		if artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.OwnerId != input.EnvironmentId {
			return invalidAttachConsumer()
		}
	}
	baselineMetadata, candidateMetadata := proto.CloneOf(input.Baseline), proto.CloneOf(input.Candidate)
	baselineMetadata.ArtifactId, candidateMetadata.ArtifactId = "", ""
	baselineMetadata.CanonicalYaml, candidateMetadata.CanonicalYaml = nil, nil
	baselineMetadata.YamlSha256, candidateMetadata.YamlSha256 = nil, nil
	if !proto.Equal(baselineMetadata, candidateMetadata) {
		return invalidAttachConsumer()
	}
	expected, err := detachedConsumerProject(ctx, input)
	if err != nil {
		return err
	}
	actual, err := loadAttachConsumerProject(ctx, input.Candidate)
	if err != nil {
		return err
	}
	want, err := expected.MarshalYAML()
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	defer clear(want)
	got, err := actual.MarshalYAML()
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	defer clear(got)
	if !bytes.Equal(want, got) {
		return errs.New(errs.KindValidationFailed, "Detach consumer update changes more than the captured backing-network membership")
	}
	return nil
}

// PrepareAttachConsumerTransition derives only the selected network-edge
// removal from an acknowledged native artifact. It never renders desired state.
func PrepareAttachConsumerTransition(ctx context.Context, baseline *agentpb.ComposeArtifact,
	environmentID, backingEnvironmentID, serviceID, networkID, artifactID string,
) (*agentpb.AttachConsumerTransition, error) {
	if ctx == nil || baseline == nil || validateArtifactContent(baseline) != nil ||
		baseline.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || baseline.OwnerId != environmentID ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindEnvironment, backingEnvironmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil ||
		artifactID == baseline.ArtifactId {
		return nil, invalidAttachConsumer()
	}
	input := &agentpb.AttachConsumerTransition{EnvironmentId: environmentID, BackingEnvironmentId: backingEnvironmentID,
		ServiceId: serviceID, NetworkId: networkID, Baseline: proto.CloneOf(baseline)}
	project, err := detachedConsumerProject(ctx, input)
	if err != nil {
		return nil, err
	}
	canonical, err := project.MarshalYAML()
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	input.Candidate = proto.CloneOf(baseline)
	input.Candidate.ArtifactId, input.Candidate.CanonicalYaml = artifactID, canonical
	digest := sha256.Sum256(canonical)
	input.Candidate.YamlSha256 = digest[:]
	if err := ValidateAttachConsumerTransition(ctx, input); err != nil {
		clear(input.Candidate.CanonicalYaml)
		return nil, err
	}
	return input, nil
}

func detachedConsumerProject(ctx context.Context, input *agentpb.AttachConsumerTransition) (*composetypes.Project, error) {
	dockerName, err := networkname.New(input.NetworkId)
	if err != nil {
		return nil, invalidAttachConsumer()
	}
	expected, err := loadAttachConsumerProject(ctx, input.Baseline)
	if err != nil {
		return nil, err
	}
	key := "gp_attach_" + strings.ToLower(input.NetworkId)
	if network, found := expected.Networks[key]; found && (!bool(network.External) || network.Name != dockerName) {
		return nil, invalidAttachConsumer()
	}
	names, err := AttachConsumerNames(input)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		service, found := expected.Services[name]
		if !found {
			return nil, invalidAttachConsumer()
		}
		service.Networks = maps.Clone(service.Networks)
		delete(service.Networks, key)
		expected.Services[name] = service
	}
	used := false
	for _, service := range expected.Services {
		if _, found := service.Networks[key]; found {
			used = true
			break
		}
	}
	if !used {
		delete(expected.Networks, key)
	}
	return expected, nil
}

// AttachConsumerNames includes the captured workload slots and replicas of one
// logical Service. Its stable proxy and all unrelated Services stay untouched.
func AttachConsumerNames(input *agentpb.AttachConsumerTransition) ([]string, error) {
	if input == nil || input.Baseline == nil || ids.Validate(ids.KindService, input.ServiceId) != nil {
		return nil, invalidAttachConsumer()
	}
	var names []string
	for _, service := range input.Baseline.Services {
		if service == nil {
			return nil, invalidAttachConsumer()
		}
		if service.ServiceId != input.ServiceId || service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if validateComposeName(service.ComposeName) != nil || validateNativePredecessorWorkload(service) != nil {
			return nil, invalidAttachConsumer()
		}
		names = append(names, service.ComposeName)
	}
	slices.Sort(names)
	if len(names) == 0 || len(slices.Compact(slices.Clone(names))) != len(names) {
		return nil, invalidAttachConsumer()
	}
	return names, nil
}

func loadAttachConsumerProject(ctx context.Context, artifact *agentpb.ComposeArtifact) (*composetypes.Project, error) {
	project, err := loader.LoadWithContext(ctx, composetypes.ConfigDetails{WorkingDir: "/",
		ConfigFiles: []composetypes.ConfigFile{{Filename: "compose.yaml", Content: artifact.CanonicalYaml}}},
		func(options *loader.Options) {
			options.ResolvePaths = false
			options.SkipInterpolation = true
			options.SkipNormalization = true
			options.SkipResolveEnvironment = true
			options.SetProjectName(artifact.ProjectName, true)
		})
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	maps.Copy(project.Services, project.DisabledServices)
	project.DisabledServices = nil
	return project, nil
}

func invalidAttachConsumer() error {
	return errs.New(errs.KindValidationFailed, "Detach consumer transition requires captured native runtime and one owned backing-network edge")
}
