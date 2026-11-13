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
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
	"golang.org/x/sys/unix"
)

func listenerStart(pid int) (string, error) {
	value, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", errs.Wrap(errs.KindStateConflict, err)
	}
	_, tail, found := strings.Cut(string(value), ") ")
	fields := strings.Fields(tail)
	if !found || len(fields) < 20 {
		return "", errs.New(errs.KindStateConflict, "Runner listener process identity is unavailable")
	}
	return fields[19], nil
}
func updateResult(request runnersoftware.Request) runnersoftware.Result {
	return runnersoftware.Result{
		RequestID:    request.RequestID,
		RunnerID:     request.RunnerID,
		RuntimeEpoch: request.RuntimeEpoch,
		PlanSHA256:   request.PlanSHA256,
		ObservedAt:   time.Now().UTC(),
		Version:      request.Release.Version,
		Stages: []runnersoftware.Stage{
			{Name: "download", Outcome: "passed"},
			{Name: "idle", Outcome: "passed"},
			{Name: "activate", Outcome: "passed"},
			{Name: "reconnect", Outcome: "passed"},
		},
	}
}

func (updater *SoftwareUpdater) ownedContainer(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	id string,
) (bool, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return false, err
	}
	defer engine.Close()
	response, err := engine.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return false, dockerError(ctx, "inspect Runner recovery ownership", err)
	}
	if err := validateRunnerContainer(response.Container, plan); err != nil {
		return false, err
	}
	if response.Container.ID != id || response.Container.State == nil {
		return false, errs.New(errs.KindStateConflict, "Runner recovery ownership changed")
	}
	return response.Container.State.Running, nil
}

// Recover observes the pinned operation only. Completion is proved from its
// receipt/process; otherwise the exact captured predecessor is restored.
func (updater *SoftwareUpdater) Recover(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	expected runnerallocation.RunnerRuntimeEvidence,
	request runnersoftware.Request,
) (*runnersoftware.Result, error) {
	if err := validateCall(ctx, plan); err != nil {
		return nil, err
	}
	root := softwareDirectory(plan, request)
	state, err := loadSoftwareJournal(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if state.Request != request {
		return nil, errs.New(errs.KindStateConflict, "Runner recovery differs from the captured update")
	}
	if state.Phase == "prepared" || state.Phase == "restored" {
		return nil, cleanupSoftwareStaging(root)
	}
	if state.ContainerID != expected.ContainerID {
		return nil, errs.New(errs.KindStateConflict, "Runner recovery container changed")
	}
	running, err := updater.ownedContainer(ctx, plan, expected.ContainerID)
	if err != nil {
		return nil, err
	}
	if state.Phase == "draining" {
		// A Controller exit after SIGSTOP must not leave the listener frozen.
		// A reused PID is never permission to signal a different process.
		start, err := listenerStart(state.ListenerPID)
		if err == nil && start == state.ListenerStart {
			fd, err := unix.PidfdOpen(state.ListenerPID, 0)
			if err != nil {
				return nil, errs.Wrap(errs.KindStateConflict, err)
			}
			defer unix.Close(fd)
			again, err := listenerStart(state.ListenerPID)
			if err != nil || again != state.ListenerStart {
				return nil, errs.New(errs.KindStateConflict, "Runner listener changed during recovery")
			}
			if err := unix.PidfdSendSignal(fd, unix.SIGCONT, nil, 0); err != nil {
				return nil, errs.Wrap(errs.KindStateConflict, err)
			}
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		state.Phase = "restored"
		return nil, saveSoftwareJournal(root, state)
	}
	if state.Phase != "committed" && state.Phase != "activating" {
		return nil, errs.New(errs.KindStateConflict, "Runner software update phase is invalid")
	}
	if state.Phase == "activating" && predecessorRestored(plan.Paths.RunnerHome, state) {
		start, identityErr := listenerStart(state.ListenerPID)
		if identityErr == nil && start == state.ListenerStart {
			fd, err := unix.PidfdOpen(state.ListenerPID, 0)
			if err != nil {
				return nil, errs.Wrap(errs.KindStateConflict, err)
			}
			defer unix.Close(fd)
			if err := unix.PidfdSendSignal(fd, unix.SIGCONT, nil, 0); err != nil {
				return nil, errs.Wrap(errs.KindStateConflict, err)
			}
			state.Phase = "restored"
			return nil, saveSoftwareJournal(root, state)
		}
		if running {
			if err := updater.connected(ctx, plan, expected.ContainerID, state.PreviousVersion, state.ActivatedAt); err != nil {
				return nil, err
			}
			state.Phase = "restored"
			return nil, saveSoftwareJournal(root, state)
		}
	}
	if running {
		version, versionErr := updater.InstalledVersion(ctx, plan, expected.ContainerID)
		if versionErr == nil && version == request.Release.Version {
			busy, _, processErr := updater.processes(ctx, plan, expected)
			// A worker proves the new listener has already accepted a job; never
			// terminate it merely because the Controller lost the success reply.
			if processErr == nil && busy {
				state.Phase = "committed"
				if err := saveSoftwareJournal(root, state); err != nil {
					return nil, err
				}
				if err := cleanupSoftwareStaging(root); err != nil {
					return nil, err
				}
				result := updateResult(request)
				return &result, nil
			}
			if err := updater.connected(ctx, plan, expected.ContainerID, request.Release.Version, state.ActivatedAt); err != nil {
				return nil, err
			}
			state.Phase = "committed"
			if err := saveSoftwareJournal(root, state); err != nil {
				return nil, err
			}
			if err := cleanupSoftwareStaging(root); err != nil {
				return nil, err
			}
			result := updateResult(request)
			return &result, nil
		}
		busy, _, err := updater.processes(ctx, plan, expected)
		if err != nil {
			return nil, err
		}
		if busy {
			return nil, errs.New(
				errs.KindResourceInUse,
				"Runner has an active job; captured software recovery is waiting",
			)
		}
	}
	if state.Phase == "committed" {
		return nil, errs.New(
			errs.KindStateConflict,
			"Completed Runner update no longer matches its verified installed version",
		)
	}
	// A stopped container has no active job. Only this operation's captured
	// package entries may be restored; credentials and workspace are excluded.
	if running {
		return nil, errs.New(
			errs.KindStateConflict,
			"Runner restarted with an unexpected version; inspect it before recovery",
		)
	}
	for _, entry := range state.Entries {
		if !validPackageName(entry.Name) {
			return nil, errs.New(errs.KindStateConflict, "Runner recovery package path is invalid")
		}
		old := filepath.Join(root, "previous", entry.Name)
		live := filepath.Join(plan.Paths.RunnerHome, entry.Name)
		if _, err := os.Lstat(old); err == nil {
			digest, err := packageDigest(old)
			if err != nil || digest != entry.PriorDigest {
				return nil, errs.New(
					errs.KindStateConflict,
					"Runner predecessor package no longer matches its captured digest",
				)
			}
			if err := os.RemoveAll(live); err != nil {
				return nil, errs.Wrap(errs.KindInternal, err)
			}
			if err := os.Rename(old, live); err != nil {
				return nil, errs.Wrap(errs.KindInternal, err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, errs.Wrap(errs.KindInternal, err)
		} else if entry.Prior {
			digest, err := packageDigest(live)
			if err != nil || digest != entry.PriorDigest {
				return nil, errs.New(errs.KindStateConflict, "Runner predecessor backup is missing and restoration is unproved")
			}
		} else if entry.Next {
			if err := os.RemoveAll(live); err != nil {
				return nil, errs.Wrap(errs.KindInternal, err)
			}
		}
	}
	if _, err := updater.operations.ObserveDaemon(ctx, plan); err != nil {
		return nil, err
	}
	if _, err := updater.operations.run(ctx, "docker", "--host=unix://"+hostDockerSocket, "start", expected.ContainerID); err != nil {
		return nil, err
	}
	if !predecessorRestored(plan.Paths.RunnerHome, state) {
		return nil, errs.New(errs.KindStateConflict, "Runner predecessor package restoration is incomplete")
	}
	if !runnersoftware.ValidVersion(state.PreviousVersion) {
		return nil, errs.New(errs.KindStateConflict, "Runner recovery lost its predecessor version")
	}
	if err := updater.connected(ctx, plan, expected.ContainerID, state.PreviousVersion, time.Now().UTC().Add(-time.Second)); err != nil {
		return nil, err
	}
	state.Phase = "restored"
	return nil, saveSoftwareJournal(root, state)
}

func cleanupSoftwareStaging(root string) error {
	for _, name := range []string{"package", "previous", "download.tar.gz"} {
		if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
	}
	return nil
}

func (updater *SoftwareUpdater) Prune(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	request runnersoftware.Request,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.Validate() != nil || request.RunnerID != plan.RunnerID || request.PlanSHA256 != plan.Digest() {
		return errs.New(errs.KindValidationFailed, "Runner receipt retirement input is invalid")
	}
	root := softwareDirectory(plan, request)
	state, err := loadSoftwareJournal(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if _, statErr := os.Lstat(root); errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			// Package preparation may be interrupted before its first journal.
			// Activation always writes that journal before changing the Runner.
			if err := ensureOwnedDirectory(root, 0, 0, 0o700); err != nil {
				return err
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil {
				return errs.Wrap(errs.KindInternal, readErr)
			}
			for _, entry := range entries {
				if entry.Name() != "package" && entry.Name() != "download.tar.gz" && entry.Name() != "state.new" {
					return errs.New(
						errs.KindStateConflict,
						"Unjournaled Runner staging contains unexpected recovery data",
					)
				}
			}
			if err := os.RemoveAll(root); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
			return nil
		}
		return err
	}
	if state.Request != request ||
		(state.Phase != "prepared" && state.Phase != "restored" && state.Phase != "committed") {
		return errs.New(errs.KindStateConflict, "Runner receipt still holds unresolved software effects")
	}
	if err := os.RemoveAll(root); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
