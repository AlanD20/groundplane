package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"golang.org/x/sys/unix"
)

func (control *HostControl) ObserveScopedCandidate(
	ctx context.Context,
	plan corerunner.Plan,
) (runnerallocation.RunnerRuntimeEvidence, error) {
	if !plan.Egress.ScopedEndpoint.IsValid() {
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(
			errs.KindValidationFailed,
			"Runner candidate is not scoped",
		)
	}
	if err := requireScopedCredential(plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	if err := requireScopedRegistryTrust(plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	if _, err := control.ObserveEgress(ctx, plan); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	step, err := control.ObserveRunner(ctx, plan)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	return *step.Ownership, nil
}

// ReplaceRegisteredRunner preserves the existing provisioned registration and
// installed software while replacing only its container and local egress.
// The caller must hold durable, exclusive authority for the exact cutover.
func (control *HostControl) ReplaceRegisteredRunner(ctx context.Context, legacy, next corerunner.Plan,
	expected runnerallocation.RunnerRuntimeEvidence,
) (runnerallocation.RunnerRuntimeEvidence, error) {
	if ctx == nil || corerunner.ValidatePlan(legacy) != nil || corerunner.ValidatePlan(next) != nil ||
		legacy.Egress.ScopedEndpoint.IsValid() || !next.Egress.ScopedEndpoint.IsValid() ||
		legacy.RuntimeEpoch == ^uint64(0) || next.RuntimeEpoch != legacy.RuntimeEpoch+1 ||
		next.IdentityDigest() != legacy.IdentityDigest() || !expected.Valid() {
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(
			errs.KindValidationFailed,
			"Runner cutover does not match its captured predecessor",
		)
	}
	if control.local == nil {
		return runnerallocation.RunnerRuntimeEvidence{}, errs.New(
			errs.KindStateConflict,
			"Runner cutover requires the owning Docker host",
		)
	}
	if err := requireScopedCredential(next); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	if err := requireScopedRegistryTrust(next); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	if err := registrationPresent(legacy); err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	defer engine.Close()
	containerID, err := control.local.replaceRegisteredContainer(ctx, engine, legacy, next, expected)
	if err != nil {
		return runnerallocation.RunnerRuntimeEvidence{}, err
	}
	return control.Resume(ctx, next, containerID)
}

func (operations *localOperations) replaceRegisteredContainer(ctx context.Context, engine *client.Client,
	legacy, next corerunner.Plan, expected runnerallocation.RunnerRuntimeEvidence,
) (string, error) {
	result, err := engine.ContainerInspect(ctx, legacy.Container.Name, client.ContainerInspectOptions{})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return "", dockerError(ctx, "inspect Runner before scoped cutover", err)
	}
	if err == nil && result.Container.ID != expected.ContainerID {
		if err := validateRunnerContainer(result.Container, next); err != nil {
			return "", err
		}
		// The candidate may already be present after an unknown Docker response
		// or Controller exit. Never recreate an already-owned candidate.
		return result.Container.ID, nil
	}
	if err == nil {
		if err := operations.drainLegacyRunner(ctx, engine, result.Container, legacy, expected); err != nil {
			return "", err
		}
	}
	// Prove there are no remaining rootless Docker processes before touching
	// Runner-owned configuration or replacing legacy egress.
	if _, err := operations.StopDaemon(ctx, legacy); err != nil {
		return "", err
	}
	if _, err := operations.ObserveDaemonAbsent(ctx, legacy); err != nil {
		return "", err
	}
	if err := operations.waitLegacyUnitRetired(ctx, legacy); err != nil {
		return "", err
	}
	if err := scrubLegacyRegistry(legacy); err != nil {
		return "", err
	}
	if err := operations.EnsureEgress(ctx, next); err != nil {
		return "", err
	}
	created, err := engine.ContainerCreate(ctx, runnerContainerOptions(next))
	if err != nil {
		return "", dockerError(ctx, "create scoped Runner container", err)
	}
	if created.ID == "" {
		return "", errs.New(errs.KindInternal, "Docker did not return the scoped Runner identity")
	}
	return created.ID, nil
}

func (operations *localOperations) waitLegacyUnitRetired(ctx context.Context, legacy corerunner.Plan) error {
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		state, err := operations.unitState(bounded, legacy)
		if err != nil || state == "absent" {
			return err
		}
		if state != "inactive" {
			return errs.New(errs.KindStateConflict, "Runner predecessor daemon resumed during scoped cutover")
		}
		select {
		case <-bounded.Done():
			return errs.New(errs.KindStateConflict, "Runner predecessor daemon ownership has not retired")
		case <-poll.C:
		}
	}
}

func (operations *localOperations) drainLegacyRunner(ctx context.Context, engine *client.Client,
	prior container.InspectResponse, legacy corerunner.Plan, expected runnerallocation.RunnerRuntimeEvidence,
) error {
	if err := validateRunnerContainer(prior, legacy); err != nil {
		return err
	}
	if prior.State == nil || prior.State.Paused || prior.State.Restarting {
		return errs.New(errs.KindStateConflict, "Runner predecessor is in an unexpected lifecycle state")
	}
	if prior.State.Running {
		updater := SoftwareUpdater{operations: operations}
		busy, pid, err := updater.processes(ctx, legacy, expected)
		if err != nil {
			return err
		}
		if busy {
			return errs.New(errs.KindResourceInUse, "Runner has an active GitHub job")
		}
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			return errs.Wrap(errs.KindStateConflict, err)
		}
		defer unix.Close(fd)
		busy, again, err := updater.processes(ctx, legacy, expected)
		if err != nil {
			return err
		}
		if busy || again != pid {
			return errs.New(errs.KindResourceInUse, "Runner accepted a job before its scoped cutover")
		}
		if err := unix.PidfdSendSignal(fd, unix.SIGSTOP, nil, 0); err != nil {
			return errs.Wrap(errs.KindStateConflict, err)
		}
		defer unix.PidfdSendSignal(fd, unix.SIGCONT, nil, 0)
		if err := waitListenerStopped(ctx, pid); err != nil {
			return err
		}
		busy, again, err = updater.processes(ctx, legacy, expected)
		if err != nil {
			return err
		}
		if busy || again != pid {
			return errs.New(errs.KindResourceInUse, "Runner accepted a job before its scoped cutover")
		}
		// Only the exact stopped listener can have admitted a worker. It must
		// remain stopped until Docker has terminated its container.
		timeout := 0
		if _, err := engine.ContainerStop(ctx, expected.ContainerID, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
			return dockerError(ctx, "stop idle Runner predecessor", err)
		}
	}
	result, err := engine.ContainerInspect(ctx, expected.ContainerID, client.ContainerInspectOptions{})
	if err != nil {
		return dockerError(ctx, "observe stopped Runner predecessor", err)
	}
	actual := result.Container
	if err := validateRunnerContainer(actual, legacy); err != nil {
		return err
	}
	if actual.ID != expected.ContainerID || actual.State == nil || actual.State.Running || actual.State.Restarting {
		return errs.New(errs.KindStateConflict, "Runner predecessor did not stop")
	}
	_, err = engine.ContainerRemove(ctx, expected.ContainerID, client.ContainerRemoveOptions{})
	if err != nil {
		return dockerError(ctx, "remove stopped Runner predecessor container", err)
	}
	return nil
}

func waitListenerStopped(ctx context.Context, pid int) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	directory := filepath.Join("/proc", strconv.Itoa(pid), "task")
	for {
		threads, err := os.ReadDir(directory)
		if err != nil {
			return errs.Wrap(errs.KindStateConflict, err)
		}
		stopped := len(threads) > 0
		for _, thread := range threads {
			value, err := os.ReadFile(filepath.Join(directory, thread.Name(), "status"))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return errs.Wrap(errs.KindStateConflict, err)
			}
			if err == nil && !strings.Contains(string(value), "\nState:\tT ") {
				stopped = false
				break
			}
		}
		if stopped {
			return nil
		}
		select {
		case <-ctx.Done():
			return errs.New(errs.KindStateConflict, "Runner listener quiescence could not be confirmed")
		case <-ticker.C:
		}
	}
}
