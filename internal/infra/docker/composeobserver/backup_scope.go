package composeobserver

import "github.com/AlanD20/groundplane/proto/agentpb"

// Backup artifacts describe the captured Service runtime. Other resources in
// its Environment are outside that observation; impostors for a selected
// Compose name or Service identity still enter the ordinary collision checks.
func artifactOwnsService(artifact *agentpb.ComposeArtifact, serviceID string) bool {
	if serviceID == "" {
		return false
	}
	for _, service := range artifact.Services {
		if service.ServiceId == serviceID {
			return true
		}
	}
	return false
}
