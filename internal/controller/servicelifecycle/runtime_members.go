package servicelifecycle

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	"sort"
	"strconv"
	"time"
)

// SplitRuntimeArtifacts keeps each physical member's original ownership and
// Compose content while assigning deterministic artifact identities to this action.
func SplitRuntimeArtifacts(
	taskID string,
	createdAt time.Time,
	serviceID string,
	encoded [][]byte,
) ([]*agentpb.ComposeArtifact, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindService, serviceID) != nil ||
		createdAt.IsZero() || createdAt.Location() != time.UTC {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle artifact identity is invalid")
	}
	var artifacts []*agentpb.ComposeArtifact
	seen := make(map[string]bool)
	for _, raw := range encoded {
		if len(raw) == 0 {
			continue
		}
		source := new(agentpb.ComposeArtifact)
		if proto.Unmarshal(raw, source) != nil || len(source.Services) == 0 {
			return nil, errs.New(errs.KindInternal, "Service removal runtime artifact is corrupt")
		}
		for _, member := range source.Services {
			if member == nil || member.ComposeName == "" || seen[member.ComposeName] || member.ServiceId != serviceID {
				return nil, errs.New(errs.KindStateConflict, "Service removal physical member is duplicated or foreign")
			}
			seen[member.ComposeName] = true
			artifact, err := SelectRuntimeMembers(
				proto.CloneOf(source),
				map[string]bool{member.ComposeName: true},
			)
			if err != nil {
				return nil, err
			}
			artifact.ArtifactId = ids.DeriveAt(ids.KindConfig, createdAt, taskID, member.ComposeName)
			artifacts = append(artifacts, artifact)
		}
	}
	if len(artifacts) == 0 || len(artifacts) > executionplan.MaximumServiceRuntimeMembers {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle runtime member count is invalid")
	}
	return artifacts, nil
}

func RuntimeSource(artifact *agentpb.ComposeArtifact, stepID string) (*agentpb.ServiceLifecycleSource, error) {
	if artifact == nil || len(artifact.Services) != 1 || artifact.Services[0] == nil ||
		ids.Validate(ids.KindStep, stepID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle source requires one physical member")
	}
	member := artifact.Services[0]
	source := &agentpb.ServiceLifecycleSource{
		ArtifactId: artifact.ArtifactId, ServiceId: member.ServiceId,
		ComposeNames: []string{member.ComposeName}, StepId: stepID,
	}
	for _, label := range member.ExpectedLabels {
		switch label.Key {
		case "com.groundplane.plan-id":
			source.SourcePlanId = label.Value
		case "com.groundplane.render-generation":
			generation, err := strconv.ParseUint(label.Value, 10, 64)
			if err != nil {
				return nil, errs.New(errs.KindInternal, "Service runtime generation is invalid")
			}
			source.SourceRenderGeneration = generation
		}
	}
	if ids.Validate(ids.KindPlan, source.SourcePlanId) != nil || source.SourceRenderGeneration == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Service runtime ownership labels are incomplete")
	}
	return source, nil
}

// SelectRuntimeMembers narrows an owned artifact to the selected Compose names.
func SelectRuntimeMembers(
	artifact *agentpb.ComposeArtifact,
	allowed map[string]bool,
) (*agentpb.ComposeArtifact, error) {
	if artifact == nil || len(allowed) == 0 {
		return nil, errs.New(errs.KindInternal, "lifecycle artifact selection is empty")
	}
	var document yaml.Node
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, errs.New(errs.KindInternal, "lifecycle Compose document is invalid")
	}
	root := document.Content[0]
	for index := 0; index+1 < len(root.Content); index += 2 {
		if root.Content[index].Value != "services" {
			continue
		}
		services := root.Content[index+1]
		kept := make([]*yaml.Node, 0, len(services.Content))
		for item := 0; item+1 < len(services.Content); item += 2 {
			if allowed[services.Content[item].Value] {
				kept = append(kept, services.Content[item], services.Content[item+1])
			}
		}
		services.Content = kept
	}
	encoded, err := yaml.Marshal(&document)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	services := make([]*agentpb.ComposeService, 0, len(allowed))
	for _, service := range artifact.Services {
		if allowed[service.ComposeName] {
			services = append(services, service)
			delete(allowed, service.ComposeName)
		}
	}
	if len(allowed) != 0 {
		return nil, errs.New(errs.KindInternal, "lifecycle artifact selection is incomplete")
	}
	sort.Slice(services, func(i, j int) bool { return services[i].ComposeName < services[j].ComposeName })
	artifact.Services = services
	artifact.CanonicalYaml = encoded
	digest := sha256.Sum256(encoded)
	artifact.YamlSha256 = digest[:]
	return artifact, nil
}
