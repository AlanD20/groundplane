package composehelper

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ReleaseStartupCommands renders only candidate startup. The executor must prove
// existing container ownership before running it; this function neither launches
// Docker nor permits replay of an interrupted mutation. Stable proxies are left
// to the separately acknowledged traffic-switch boundary.
func ReleaseStartupCommands(
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
) ([]runner.RunCmdOpts, error) {
	if report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout {
		return nil, errs.New(errs.KindStateConflict, "candidate startup requires its acknowledged Release boundary")
	}
	stageID, err := report.Progress.StageID(input)
	if err != nil || stageID != report.StageID {
		return nil, errs.New(errs.KindStateConflict, "candidate startup receipt names another Release action")
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		return nil, err
	}
	workloads := artifacts.Workloads
	if report.Progress.Rolling != nil {
		if artifacts.Rolling == nil || report.Progress.Rolling.Phase != rollingrelease.StartCandidate {
			return nil, errs.New(errs.KindStateConflict, "rolling Release is not starting a candidate")
		}
		changes, err := artifacts.Rolling.Changes()
		if err != nil {
			return nil, err
		}
		candidate := changes[report.Progress.Rolling.Change-1].Candidate
		if candidate == nil {
			return nil, errs.New(errs.KindStateConflict, "rolling change has no startup candidate")
		}
		workloads = nil
		for _, service := range artifacts.Workloads {
			if service.ComposeName == candidate.ComposeName {
				workloads = append(workloads, service)
			}
		}
		if len(workloads) != 1 {
			return nil, errs.New(errs.KindStateConflict, "rolling startup candidate is absent or ambiguous")
		}
	} else if report.Progress.Action != releaseexecution.StartCandidate {
		return nil, errs.New(errs.KindStateConflict, "Release is not starting its candidate")
	}
	return candidateStartupCommands(artifacts.Candidate, workloads)
}

func candidateStartupCommands(
	artifact *agentpb.ComposeArtifact,
	workloads []*agentpb.ComposeService,
) ([]runner.RunCmdOpts, error) {
	if len(workloads) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "candidate startup requires explicit workload names")
	}
	names := make([]string, len(workloads))
	for index, workload := range workloads {
		names[index] = workload.ComposeName
	}
	slices.Sort(names)
	stdin, err := composeCLIInput(artifact.CanonicalYaml)
	if err != nil {
		return nil, err
	}
	prefix := []string{"compose", "--project-name", artifact.ProjectName,
		"--project-directory", composeProjectDirectory(artifact), "--file", "-"}
	base := runner.RunCmdOpts{Name: DockerExecutable, Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment),
		ReplaceEnv: true, Stdin: stdin, CaptureLimitBytes: 32 * 1024}
	validation := base
	validation.Args = append(slices.Clone(prefix), "config", "--quiet", "--no-interpolate")
	mutation := base
	mutation.Args = append(
		slices.Clone(prefix),
		"up",
		"--detach",
		"--no-deps",
		"--no-build",
		"--pull",
		"never",
		"--force-recreate",
	)
	mutation.Args = append(mutation.Args, names...)
	return []runner.RunCmdOpts{validation, mutation}, nil
}
