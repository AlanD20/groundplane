package runner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// SoftwareUpdater changes only the upstream package in the registered home.
// Registration, GP tooling, workspace and rootless Docker storage stay in place.
type SoftwareUpdater struct{ operations *localOperations }

func NewSoftwareUpdater(commands commandrunner.Runner) (*SoftwareUpdater, error) {
	if commands == nil {
		return nil, errs.New(errs.KindInternal, "Runner software command transport is required")
	}
	return &SoftwareUpdater{operations: &localOperations{commands: commands}}, nil
}

type packageEntry struct {
	Name        string
	Prior       bool
	Next        bool
	PriorDigest string
}
type softwareJournal struct {
	Request         runnersoftware.Request
	ContainerID     string
	Phase           string
	ListenerPID     int
	ListenerStart   string
	PreviousVersion string
	Entries         []packageEntry
	ActivatedAt     time.Time
}

func softwareDirectory(plan runnerallocation.RuntimePlan, request runnersoftware.Request) string {
	return filepath.Join("/var/lib/groundplane/runner-software", plan.RunnerID, request.RequestID)
}
func saveSoftwareJournal(root string, state softwareJournal) error {
	value, err := json.Marshal(state)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	file, err := os.OpenFile(
		filepath.Join(root, "state.new"),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	_, a := file.Write(value)
	b := file.Sync()
	c := file.Close()
	if err := errors.Join(a, b, c); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Rename(filepath.Join(root, "state.new"), filepath.Join(root, "state.json")); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	dir, err := os.Open(root)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
func loadSoftwareJournal(root string) (softwareJournal, error) {
	var state softwareJournal
	file, err := os.OpenFile(filepath.Join(root, "state.json"), os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return state, errs.Wrap(errs.KindStateConflict, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > 32<<10 {
		return state, errs.New(errs.KindStateConflict, "Runner update journal is invalid")
	}
	if err := json.NewDecoder(file).Decode(&state); err != nil {
		return state, errs.Wrap(errs.KindStateConflict, err)
	}
	return state, nil
}

func (updater *SoftwareUpdater) WaitIdle(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	expected runnerallocation.RunnerRuntimeEvidence,
) error {
	for {
		busy, _, err := updater.processes(ctx, plan, expected)
		if err != nil {
			return err
		}
		if !busy {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (updater *SoftwareUpdater) processes(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	expected runnerallocation.RunnerRuntimeEvidence,
) (bool, int, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return false, 0, err
	}
	defer engine.Close()
	actual, found, err := updater.operations.inspectRunner(ctx, engine, plan)
	if err != nil {
		return false, 0, err
	}
	if !found || actual != expected {
		return false, 0, errs.New(errs.KindStateConflict, "Runner runtime changed before its software update")
	}
	result, err := updater.operations.run(
		ctx,
		"docker",
		"--host=unix://"+hostDockerSocket,
		"top",
		expected.ContainerID,
		"-eo",
		"pid,comm",
	)
	if err != nil {
		return false, 0, err
	}
	busy, pid := false, 0
	for _, line := range strings.Split(string(result.Stdout), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if fields[1] == "Runner.Worker" {
			busy = true
		}
		if fields[1] == "Runner.Listener" {
			pid, _ = strconv.Atoi(fields[0])
		}
	}
	if pid <= 0 {
		return false, 0, errs.New(errs.KindStateConflict, "Runner listener is not running; restore it before updating")
	}
	return busy, pid, nil
}

func (updater *SoftwareUpdater) Update(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	expected runnerallocation.RunnerRuntimeEvidence,
	request runnersoftware.Request,
) (runnersoftware.Result, error) {
	if err := validateCall(ctx, plan); err != nil {
		return runnersoftware.Result{}, err
	}
	root := softwareDirectory(plan, request)
	state, err := loadSoftwareJournal(root)
	if err != nil {
		return runnersoftware.Result{}, err
	}
	if state.Request != request || state.Phase != "prepared" {
		return runnersoftware.Result{}, errs.New(
			errs.KindStateConflict,
			"Runner update staging differs from its captured release",
		)
	}
	busy, pid, err := updater.processes(ctx, plan, expected)
	if err != nil {
		return runnersoftware.Result{}, err
	}
	if busy {
		return runnersoftware.Result{}, errs.New(
			errs.KindResourceInUse,
			"Runner accepted another job; wait for it to finish and submit the update again",
		)
	}
	// Hold the exact listener process, then check again: an active worker must
	// never be killed merely because an earlier idle observation became stale.
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return runnersoftware.Result{}, errs.Wrap(errs.KindStateConflict, err)
	}
	defer unix.Close(fd)
	state.ContainerID = expected.ContainerID
	state.ListenerPID = pid
	state.ListenerStart, err = listenerStart(pid)
	if err != nil {
		return runnersoftware.Result{}, err
	}
	state.PreviousVersion, err = updater.InstalledVersion(ctx, plan, expected.ContainerID)
	if err != nil {
		return runnersoftware.Result{}, err
	}
	state.Phase = "draining"
	if err := saveSoftwareJournal(root, state); err != nil {
		return runnersoftware.Result{}, err
	}
	if err := unix.PidfdSendSignal(fd, unix.SIGSTOP, nil, 0); err != nil {
		return runnersoftware.Result{}, errs.Wrap(errs.KindStateConflict, err)
	}
	defer unix.PidfdSendSignal(fd, unix.SIGCONT, nil, 0)
	busy, again, err := updater.processes(ctx, plan, expected)
	if err != nil {
		return runnersoftware.Result{}, err
	}
	if busy || again != pid {
		return runnersoftware.Result{}, errs.New(
			errs.KindResourceInUse,
			"Runner became busy before activation; its job was left running",
		)
	}
	state.ContainerID = expected.ContainerID
	state.Phase = "activating"
	state.ActivatedAt = time.Now().UTC()
	for _, name := range packageNames {
		_, oldErr := os.Lstat(filepath.Join(plan.Paths.RunnerHome, name))
		_, newErr := os.Lstat(filepath.Join(root, "package", name))
		if oldErr != nil && !errors.Is(oldErr, os.ErrNotExist) {
			return runnersoftware.Result{}, errs.Wrap(errs.KindInternal, oldErr)
		}
		if newErr != nil && !errors.Is(newErr, os.ErrNotExist) {
			return runnersoftware.Result{}, errs.Wrap(errs.KindInternal, newErr)
		}
		if oldErr == nil || newErr == nil {
			digest := ""
			if oldErr == nil {
				digest, err = packageDigest(filepath.Join(plan.Paths.RunnerHome, name))
				if err != nil {
					return runnersoftware.Result{}, err
				}
			}
			state.Entries = append(
				state.Entries,
				packageEntry{Name: name, Prior: oldErr == nil, Next: newErr == nil, PriorDigest: digest},
			)
		}
	}
	if err := saveSoftwareJournal(root, state); err != nil {
		return runnersoftware.Result{}, err
	}
	if _, err := updater.operations.run(ctx, "docker", "--host=unix://"+hostDockerSocket, "stop", "--time=0", expected.ContainerID); err != nil {
		return runnersoftware.Result{}, err
	}
	if err := os.Mkdir(filepath.Join(root, "previous"), 0o700); err != nil {
		return runnersoftware.Result{}, errs.Wrap(errs.KindInternal, err)
	}
	for _, entry := range state.Entries {
		live := filepath.Join(plan.Paths.RunnerHome, entry.Name)
		if entry.Prior {
			if err := os.Rename(live, filepath.Join(root, "previous", entry.Name)); err != nil {
				return runnersoftware.Result{}, errs.Wrap(errs.KindInternal, err)
			}
		}
		if entry.Next {
			if err := os.Rename(filepath.Join(root, "package", entry.Name), live); err != nil {
				return runnersoftware.Result{}, errs.Wrap(errs.KindInternal, err)
			}
		}
	}
	if _, err := updater.operations.run(ctx, "docker", "--host=unix://"+hostDockerSocket, "start", expected.ContainerID); err != nil {
		return runnersoftware.Result{}, err
	}
	if err := updater.connected(ctx, plan, expected.ContainerID, request.Release.Version, state.ActivatedAt); err != nil {
		return runnersoftware.Result{}, err
	}
	state.Phase = "committed"
	if err := saveSoftwareJournal(root, state); err != nil {
		return runnersoftware.Result{}, err
	}
	if err := os.RemoveAll(filepath.Join(root, "previous")); err != nil {
		return runnersoftware.Result{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := os.RemoveAll(filepath.Join(root, "package")); err != nil {
		return runnersoftware.Result{}, errs.Wrap(errs.KindInternal, err)
	}
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
	}, nil
}

func (updater *SoftwareUpdater) connected(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	containerID, version string,
	since time.Time,
) error {
	for {
		actual, err := updater.InstalledVersion(ctx, plan, containerID)
		if err == nil && actual == version {
			logs, err := updater.operations.run(
				ctx,
				"docker",
				"--host=unix://"+hostDockerSocket,
				"logs",
				"--since",
				since.Format(time.RFC3339Nano),
				"--tail=200",
				containerID,
			)
			connected := err == nil &&
				(strings.Contains(string(logs.Stdout), "Listening for Jobs") || strings.Contains(string(logs.Stderr), "Listening for Jobs"))
			clear(logs.Stdout)
			clear(logs.Stderr)
			if connected {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errs.New(
				errs.KindStateConflict,
				"Runner did not reconnect with the selected version before the update deadline",
			)
		case <-time.After(time.Second):
		}
	}
}

func (updater *SoftwareUpdater) InstalledVersion(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	containerID string,
) (string, error) {
	result, err := updater.operations.run(
		ctx,
		"docker",
		"--host=unix://"+hostDockerSocket,
		"exec",
		"--user",
		plan.Container.User,
		containerID,
		filepath.Join(plan.Paths.RunnerHome, "bin/Runner.Listener"),
		"--version",
	)
	if err != nil {
		return "", err
	}
	version := strings.TrimSpace(string(result.Stdout))
	clear(result.Stdout)
	clear(result.Stderr)
	if !runnersoftware.ValidVersion(version) {
		return "", errs.New(errs.KindStateConflict, "Registered Runner returned an invalid installed version")
	}
	return version, nil
}

func (control *HostControl) InstalledVersion(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	id string,
) (string, error) {
	if err := validateCall(ctx, plan); err != nil {
		return "", err
	}
	reader, ok := control.operations.(interface {
		InstalledVersion(context.Context, runnerallocation.RuntimePlan, string) (string, error)
	})
	if !ok {
		return "", errs.New(errs.KindInternal, "Runner software observation is not configured")
	}
	return reader.InstalledVersion(ctx, plan, id)
}

func (operations *localOperations) InstalledVersion(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	id string,
) (string, error) {
	return (&SoftwareUpdater{operations: operations}).InstalledVersion(ctx, plan, id)
}
