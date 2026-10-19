package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A scaled Compose name selects many containers. Retire only the immutable id
// captured before publication, after checking it against the entire old/new set.
func verifyCapturedContainer(ctx context.Context, taskRunner runner.Runner,
	replica rollingrelease.Replica, workload *agentpb.ComposeService,
) error {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{.Name}}\n{{json .Config.Labels}}", replica.ContainerID},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
	})
	if err != nil || result.ExitCode != 0 {
		return errs.New(errs.KindStateConflict, "captured predecessor container is no longer observable")
	}
	name, raw, found := strings.Cut(strings.TrimSpace(string(result.Stdout)), "\n")
	labels := map[string]string{}
	if !found || strings.TrimPrefix(name, "/") != replica.ContainerName ||
		json.Unmarshal([]byte(raw), &labels) != nil ||
		!hasAllExpectedLabels(labels, workload.GetExpectedLabels()) ||
		labels["com.docker.compose.service"] != replica.ComposeName ||
		labels["com.docker.compose.container-number"] != strconv.FormatUint(uint64(replica.ComposeReplica), 10) ||
		labels["com.docker.compose.oneoff"] != "False" {
		return errs.New(errs.KindStateConflict, "captured predecessor container name or Compose identity changed")
	}
	return inspectRestorationImage(ctx, taskRunner, replica.ContainerID, workload)
}
