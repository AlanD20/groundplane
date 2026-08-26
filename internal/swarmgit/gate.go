package swarmgit

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/swarmcheck"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const (
	repositoryCITimeout       = 2 * time.Hour
	operatorVerifierTimeout   = 15 * time.Minute
	gateCaptureLimitBytes     = 128 << 10
	gateDiagnosticPrefixBytes = 4 << 10
	repositoryCIExecutable    = "/usr/bin/make"
	operatorVerifierPath      = ".agents/skills/verify-groundplane/scripts/host-health-ssh.sh"
)

type gateCommand struct {
	executable  string
	args        []string
	timeout     time.Duration
	environment []string
}

// ProveGate runs one closed gate against the clean signed delivery candidate
// and atomically emits the typed artifact named by its returned receipt.
func (inspector *Inspector) ProveGate(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	gate swarmcheck.GateID,
) (swarmcheck.GateReceipt, error) {
	var receipt swarmcheck.GateReceipt
	if manifest.Phase != swarmcheck.PhaseIntegrate || manifest.Delivery == nil {
		return receipt, invalid(
			"prove-gate requires an integrate manifest with a signed delivery candidate",
		)
	}
	if err := inspector.ValidateIntegration(ctx, manifest); err != nil {
		return receipt, err
	}
	for _, existing := range manifest.Gates {
		if existing.Gate == gate {
			return receipt, invalid(fmt.Sprintf("gate %s already has a manifest receipt", gate))
		}
	}
	command, err := inspector.gateCommand(ctx, gate)
	if err != nil {
		return receipt, err
	}
	before, err := inspector.gateState(ctx, manifest)
	if err != nil {
		return receipt, err
	}
	if err := requireDeliveryState(manifest, before); err != nil {
		return receipt, err
	}

	result, runErr := inspector.runGateCommand(ctx, command)
	if ctx.Err() != nil {
		return receipt, errs.Wrap(errs.KindInternal, ctx.Err())
	}
	after, stateErr := inspector.gateState(ctx, manifest)
	if stateErr != nil {
		return receipt, stateErr
	}
	artifactExitCode := result.ExitCode
	if runErr != nil && artifactExitCode == 0 {
		artifactExitCode = -1
	}
	artifact := swarmcheck.NewGateArtifact(
		manifest.Wave,
		gate,
		command.executable,
		command.args,
		command.environment,
		manifest.Delivery.Commit,
		manifest.Delivery.Tree,
		before,
		after,
		artifactExitCode,
		result.Stdout,
		result.Stderr,
	)
	data, digest, err := swarmcheck.EncodeGateArtifact(ctx, artifact)
	if err != nil {
		return receipt, err
	}
	relativePath := swarmcheck.GateArtifactPath(manifest.Wave, gate)
	if err := inspector.writeArtifactAtomically(
		ctx,
		manifest.Wave,
		relativePath,
		data,
	); err != nil {
		return receipt, err
	}
	receipt = swarmcheck.GateReceipt{
		Wave:      manifest.Wave,
		Gate:      gate,
		Candidate: manifest.Delivery.Commit,
		Tree:      manifest.Delivery.Tree,
		Artifact:  relativePath,
		Digest:    digest,
	}
	if runErr != nil || result.ExitCode != 0 {
		return receipt, invalid(gateFailureMessage(gate, result, runErr))
	}
	if err := requireDeliveryState(manifest, after); err != nil {
		return receipt, err
	}
	if before != after {
		return receipt, invalid(fmt.Sprintf("gate %s changed the delivery candidate state", gate))
	}
	return receipt, nil
}

func (inspector *Inspector) runGateCommand(
	ctx context.Context,
	command gateCommand,
) (runner.Result, error) {
	return inspector.Runner.Run(ctx, runner.RunCmdOpts{
		Name:              command.executable,
		Args:              append([]string(nil), command.args...),
		Dir:               inspector.Root,
		Env:               append([]string(nil), command.environment...),
		ReplaceEnv:        true,
		Timeout:           command.timeout,
		CaptureLimitBytes: gateCaptureLimitBytes,
	})
}

func (inspector *Inspector) gateCommand(
	ctx context.Context,
	gate swarmcheck.GateID,
) (gateCommand, error) {
	args, exists := swarmcheck.GateArgs(gate)
	if !exists {
		return gateCommand{}, invalid(fmt.Sprintf("gate %s is outside the closed policy", gate))
	}
	command := gateCommand{args: args, environment: inspector.gateEnvironment(gate)}
	switch gate {
	case swarmcheck.GateRepositoryCI:
		command.executable = repositoryCIExecutable
		command.timeout = repositoryCITimeout
		if err := validateAbsoluteExecutable(command.executable); err != nil {
			return gateCommand{}, err
		}
	case swarmcheck.GateOperatorVerifier:
		command.executable = filepath.Join(inspector.Root, filepath.FromSlash(operatorVerifierPath))
		command.timeout = operatorVerifierTimeout
		if err := inspector.validateRepositoryExecutable(ctx, operatorVerifierPath); err != nil {
			return gateCommand{}, err
		}
	}
	return command, nil
}

func (inspector *Inspector) gateEnvironment(gate swarmcheck.GateID) []string {
	keys := []string{
		"DOCKER_HOST",
		"GOCACHE",
		"GOMODCACHE",
		"GOPATH",
		"HOME",
		"NPM_CONFIG_CACHE",
		"SSH_AUTH_SOCK",
		"XDG_CACHE_HOME",
		"XDG_CONFIG_HOME",
	}
	if gate == swarmcheck.GateOperatorVerifier {
		keys = append(keys,
			"GROUNDPLANE_EVIDENCE_DIR",
			"GROUNDPLANE_LOCAL_HTTP_PORT",
			"GROUNDPLANE_REMOTE_HTTP_ADDR",
			"GROUNDPLANE_SSH_KEY",
			"GROUNDPLANE_SSH_KNOWN_HOSTS",
			"GROUNDPLANE_SSH_PORT",
			"GROUNDPLANE_SSH_TARGET",
		)
	}
	environment := controlledEnvironment(keys)
	environment[0] = "PATH=" + trustedGateToolchainPath(inspector.Root)
	return environment
}

func validateAbsoluteExecutable(executable string) error {
	fd, err := unix.Openat2(unix.AT_FDCWD, executable, &unix.OpenHow{
		Flags:   uint64(unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: unix.RESOLVE_NO_SYMLINKS,
	})
	if err != nil {
		return invalid(fmt.Sprintf("open gate executable %s without symlinks: %v", executable, err))
	}
	defer func() {
		// Descriptor cleanup cannot affect the already completed identity check.
		_ = unix.Close(fd)
	}()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return invalid(fmt.Sprintf("stat gate executable %s: %v", executable, err))
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o111 == 0 {
		return invalid(
			fmt.Sprintf("gate executable %s is not an executable regular file", executable),
		)
	}
	return nil
}

func (inspector *Inspector) validateRepositoryExecutable(ctx context.Context, value string) error {
	if err := ctx.Err(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	directoryFD, err := openDirectoryBeneath(
		ctx,
		inspector.Root,
		filepath.ToSlash(filepath.Dir(value)),
		false,
	)
	if err != nil {
		return err
	}
	fd, err := unix.Openat2(directoryFD, filepath.Base(value), &unix.OpenHow{
		Flags:   uint64(unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS,
	})
	closeDirectoryErr := unix.Close(directoryFD)
	if err != nil {
		return invalid(fmt.Sprintf("open repository gate executable %s: %v", value, err))
	}
	if closeDirectoryErr != nil {
		// The parent close failure precedes transfer to the executable descriptor.
		_ = unix.Close(fd)
		return errs.Wrap(errs.KindInternal, closeDirectoryErr)
	}
	defer func() {
		// Descriptor cleanup cannot affect the already completed identity check.
		_ = unix.Close(fd)
	}()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return invalid(fmt.Sprintf("stat repository gate executable %s: %v", value, err))
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o111 == 0 {
		return invalid(fmt.Sprintf("repository gate executable %s is not executable", value))
	}
	return nil
}

func (inspector *Inspector) gateState(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) (swarmcheck.GateState, error) {
	if err := inspector.requireTargetRef(ctx, manifest.TargetRef); err != nil {
		return swarmcheck.GateState{}, err
	}
	head, err := inspector.resolve(ctx, "HEAD")
	if err != nil {
		return swarmcheck.GateState{}, err
	}
	tree, err := inspector.writeTree(ctx)
	if err != nil {
		return swarmcheck.GateState{}, err
	}
	status, err := inspector.status(ctx, true)
	if err != nil {
		return swarmcheck.GateState{}, err
	}
	if err := validatePrimaryStatus(status, manifest.Wave, false); err != nil {
		return swarmcheck.GateState{}, err
	}
	return swarmcheck.GateState{Head: head, Tree: tree, Clean: true}, nil
}

func requireDeliveryState(manifest swarmcheck.Manifest, state swarmcheck.GateState) error {
	if manifest.Delivery == nil || state.Head != manifest.Delivery.Commit ||
		state.Tree != manifest.Delivery.Tree || !state.Clean {
		return invalid("repository is not clean at the exact signed delivery candidate")
	}
	return nil
}

func (inspector *Inspector) requireGateArtifact(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	receipt swarmcheck.GateReceipt,
) error {
	data, digest, err := inspector.readArtifact(ctx, manifest.Wave, receipt.Artifact)
	if err != nil {
		return err
	}
	if digest != receipt.Digest {
		return invalid(
			fmt.Sprintf("gate artifact %s digest does not match its receipt", receipt.Artifact),
		)
	}
	artifact, err := swarmcheck.ParseGateArtifact(ctx, data)
	if err != nil {
		return err
	}
	command, err := inspector.gateCommand(ctx, receipt.Gate)
	if err != nil {
		return err
	}
	if artifact.Wave != manifest.Wave || artifact.Gate != receipt.Gate ||
		artifact.Executable != command.executable || !sameValues(artifact.Args, command.args) ||
		!sameValues(artifact.Environment, command.environment) ||
		artifact.Candidate != receipt.Candidate || artifact.Candidate != manifest.Delivery.Commit ||
		artifact.Tree != receipt.Tree || artifact.Tree != manifest.Delivery.Tree || artifact.ExitCode != 0 ||
		artifact.Before.Head != artifact.Candidate || artifact.Before.Tree != artifact.Tree ||
		!artifact.Before.Clean || artifact.After != artifact.Before {
		return invalid(
			fmt.Sprintf(
				"gate artifact %s does not prove the closed command and unchanged tree",
				receipt.Artifact,
			),
		)
	}
	return nil
}

func gateFailureMessage(gate swarmcheck.GateID, result runner.Result, runErr error) string {
	diagnostic := bytes.TrimSpace(result.Stderr)
	stream := "stderr"
	if len(diagnostic) == 0 {
		diagnostic = bytes.TrimSpace(result.Stdout)
		stream = "stdout"
	}
	if len(diagnostic) > gateDiagnosticPrefixBytes {
		diagnostic = diagnostic[:gateDiagnosticPrefixBytes]
	}
	if len(diagnostic) != 0 {
		return fmt.Sprintf(
			"gate %s failed with exit code %d; %s prefix: %s",
			gate,
			result.ExitCode,
			stream,
			diagnostic,
		)
	}
	if runErr != nil {
		return fmt.Sprintf("gate %s failed with exit code %d: %v", gate, result.ExitCode, runErr)
	}
	return fmt.Sprintf("gate %s failed with exit code %d", gate, result.ExitCode)
}

func sameValues[T comparable](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
