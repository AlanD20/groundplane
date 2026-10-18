package serviceremovalwork

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

// ValidateCandidate restricts a direct Remove to its Service and its dependent
// mount/dependency declarations. Artifact generation is validated by the desired
// revision publisher; no unrelated desired changes may hitchhike on removal.
func ValidateCandidate(input Input, current, candidate environmentprojection.EnvironmentComposeProjection) error {
	if input.Validate() != nil || environmentprojection.ValidateEnvironmentComposeProjection(current) != nil ||
		environmentprojection.ValidateEnvironmentComposeProjection(candidate) != nil ||
		current.EnvironmentID != input.Scope.EnvironmentID || candidate.EnvironmentID != input.Scope.EnvironmentID ||
		current.RevisionID != input.BaseRevisionID || candidate.RevisionID != input.DesiredRevisionID ||
		candidate.RenderGeneration != input.Generation || current.RenderGeneration == ^uint64(0) ||
		current.RenderGeneration+1 != input.Generation {
		return conflict("Service removal desired candidate differs from its captured baseline")
	}
	expected := environmentprojection.CloneEnvironmentComposeProjection(current)
	expected.RevisionID, expected.RenderGeneration = candidate.RevisionID, candidate.RenderGeneration
	expected.ComposeArtifact, expected.NormalizedCompose = candidate.ComposeArtifact, candidate.NormalizedCompose
	expected.DesiredServices = nil
	removed := 0
	for _, service := range current.DesiredServices {
		if service.Desired.ID == input.ServiceID && service.Desired.Name == input.ServiceName {
			removed++
			continue
		}
		expected.DesiredServices = append(expected.DesiredServices, service)
	}
	expected.VolumeMounts = nil
	for _, mount := range current.VolumeMounts {
		if mount.ServiceID != input.ServiceID {
			expected.VolumeMounts = append(expected.VolumeMounts, mount)
		}
	}
	expected.ServiceDependencyPlans = expected.ServiceDependencyPlans.WithoutService(input.ServiceName)
	delete(expected.ServiceExtensions, input.ServiceName)
	if len(expected.ServiceExtensions) == 0 {
		expected.ServiceExtensions = nil
	}
	left, err := environmentprojection.EncodeEnvironmentComposeProjectionStorage(expected)
	if err != nil {
		return err
	}
	defer clear(left)
	right, err := environmentprojection.EncodeEnvironmentComposeProjectionStorage(candidate)
	if err != nil {
		return err
	}
	defer clear(right)
	if removed != 1 || !bytes.Equal(left, right) {
		return conflict("Service removal candidate changes unrelated desired resources")
	}
	return nil
}
