package composehelper

import (
	"sort"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	WorkDirectory    = "/run/groundplane/compose"
	DockerExecutable = "docker"
)

var fixedEnvironment = []string{
	"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	"HOME=" + WorkDirectory,
	"DOCKER_HOST=unix:///var/run/docker.sock",
	"COMPOSE_DISABLE_ENV_FILE=1",
	"COMPOSE_PARALLEL_LIMIT=1",
}

func composeProjectDirectory(artifact *agentpb.ComposeArtifact) string {
	if artifact.AuthorizedVolumeDir != "" {
		return artifact.AuthorizedVolumeDir
	}
	return WorkDirectory
}

func serviceNames(artifact *agentpb.ComposeArtifact, selected []string) []string {
	selectedIDs := make(map[string]struct{}, len(selected))
	for _, serviceID := range selected {
		selectedIDs[serviceID] = struct{}{}
	}
	names := make([]string, 0, len(selected)*3)
	for _, service := range artifact.Services {
		if _, ok := selectedIDs[service.ServiceId]; ok {
			names = append(names, service.ComposeName)
		}
	}
	sort.Strings(names)
	return names
}
