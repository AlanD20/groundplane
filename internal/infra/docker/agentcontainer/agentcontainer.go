// Package agentcontainer reconciles the single Controller-owned local Agent
// container through the Docker Engine Go client. It is intentionally separate
// from the Agent-side Compose workload applier in internal/infra/docker.
//
// Docker Engine SDK: https://docs.docker.com/reference/api/engine/sdk/
// Container API: https://docs.docker.com/reference/api/engine/#tag/Container
package agentcontainer

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/distribution/reference"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/oklog/ulid/v2"
	digest "github.com/opencontainers/go-digest"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ContainerName = "groundplane-agent"

	dockerHost       = "unix:///var/run/docker.sock"
	dockerSocketPath = "/var/run/docker.sock"
	agentStatePath   = "/var/lib/groundplane/agent"
	channelPath      = "/run/groundplane/controller"
	agentRuntimeRoot = "/run/groundplane/agents"
	agentConfigPath  = "/run/groundplane/agent.yaml"
	agentTokenPath   = "/run/groundplane/agent.token"

	labelManaged    = "groundplane.managed"
	labelKind       = "groundplane.kind"
	labelAgentID    = "groundplane.agent_id"
	labelGeneration = "groundplane.generation"
)

// Desired is the complete caller-controlled identity and runtime material for
// the local Agent. Image is used exactly as supplied and must include a sha256
// digest; this package never selects or pulls an image source.
type Desired struct {
	Image      string
	AgentID    string
	Generation string
}

// RuntimePaths are the Controller-owned host paths a materializer must create
// before reconciliation. Container destinations remain fixed private policy.
type RuntimePaths struct {
	Directory string
	Config    string
	Token     string
}

type State struct {
	Exists     bool
	Owned      bool
	Running    bool
	ID         string
	Image      string
	AgentID    string
	Generation string
}

type Action string

const (
	ActionNone     Action = "none"
	ActionCreated  Action = "created"
	ActionStarted  Action = "started"
	ActionReplaced Action = "replaced"
)

type Result struct {
	State  State
	Action Action
}

// Lifecycle is the neutral convergent seam used by Controller orchestration.
// It deliberately exposes no Docker SDK types.
type Lifecycle interface {
	Reconcile(ctx context.Context, desired Desired) (Result, error)
	Inspect(ctx context.Context) (State, error)
	Remove(ctx context.Context) error
}

type engineClient interface {
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	Close() error
}

type Manager struct {
	client engineClient
}

// New connects to the local Docker Engine socket. The current Moby client
// negotiates the Engine API version by default.
func New(ctx context.Context) (*Manager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	engine, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		return nil, errs.Wrap(errs.CodeInternal, fmt.Errorf("agent container: create Docker client: %w", err))
	}
	return &Manager{client: engine}, nil
}

func (m *Manager) Close() error {
	if err := m.client.Close(); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent container: close Docker client: %w", err))
	}
	return nil
}

func RuntimePathsForAgent(agentID string) (RuntimePaths, error) {
	if !isCanonicalAgentID(agentID) {
		return RuntimePaths{}, errs.New(errs.CodeValidationFailed, "agent container: agent id must be agt_<26 uppercase Crockford ULID>")
	}
	return runtimePaths(agentID), nil
}

func runtimePaths(agentID string) RuntimePaths {
	directory := path.Join(agentRuntimeRoot, agentID)
	return RuntimePaths{
		Directory: directory,
		Config:    path.Join(directory, "config.yaml"),
		Token:     path.Join(directory, "token"),
	}
}

func (m *Manager) Reconcile(ctx context.Context, desired Desired) (Result, error) {
	if err := validateDesired(desired); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	inspect, exists, err := m.inspect(ctx)
	if err != nil {
		return Result{}, err
	}
	if !exists {
		state, err := m.createAndStart(ctx, desired)
		return Result{State: state, Action: ActionCreated}, err
	}

	state := stateFromInspect(inspect)
	if !state.Owned {
		return Result{}, unownedCollision()
	}
	if matchesDesired(inspect, desired) {
		if state.Running {
			return Result{State: state, Action: ActionNone}, nil
		}
		if _, err := m.client.ContainerStart(ctx, state.ID, client.ContainerStartOptions{}); err != nil {
			return Result{}, operationError(ctx, "start stopped container", err)
		}
		state.Running = true
		return Result{State: state, Action: ActionStarted}, nil
	}

	if _, err := m.client.ContainerRemove(ctx, state.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
		return Result{}, operationError(ctx, "remove drifted container", err)
	}
	state, err = m.createAndStart(ctx, desired)
	return Result{State: state, Action: ActionReplaced}, err
}

func (m *Manager) Inspect(ctx context.Context) (State, error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	inspect, exists, err := m.inspect(ctx)
	if err != nil {
		return State{}, err
	}
	if !exists {
		return State{}, nil
	}
	return stateFromInspect(inspect), nil
}

func (m *Manager) Remove(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	inspect, exists, err := m.inspect(ctx)
	if err != nil || !exists {
		return err
	}
	state := stateFromInspect(inspect)
	if !state.Owned {
		return unownedCollision()
	}
	if _, err := m.client.ContainerRemove(ctx, state.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
		if containerderrdefs.IsNotFound(err) {
			return nil
		}
		return operationError(ctx, "remove container", err)
	}
	return nil
}

func (m *Manager) inspect(ctx context.Context) (container.InspectResponse, bool, error) {
	result, err := m.client.ContainerInspect(ctx, ContainerName, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return container.InspectResponse{}, false, nil
	}
	if err != nil {
		return container.InspectResponse{}, false, operationError(ctx, "inspect container", err)
	}
	return result.Container, true, nil
}

func (m *Manager) createAndStart(ctx context.Context, desired Desired) (State, error) {
	options := createOptions(desired)
	created, err := m.client.ContainerCreate(ctx, options)
	if err != nil {
		return State{}, operationError(ctx, "create container", err)
	}
	if created.ID == "" {
		return State{}, errs.New(errs.CodeInternal, "agent container: Docker returned an empty container id")
	}
	if _, err := m.client.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		return State{}, operationError(ctx, "start created container", err)
	}
	return State{
		Exists:     true,
		Owned:      true,
		Running:    true,
		ID:         created.ID,
		Image:      desired.Image,
		AgentID:    desired.AgentID,
		Generation: desired.Generation,
	}, nil
}

func createOptions(desired Desired) client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Name: ContainerName,
		Config: &container.Config{
			Image: desired.Image,
			User:  "0",
			Labels: map[string]string{
				labelManaged:    "true",
				labelKind:       "agent",
				labelAgentID:    desired.AgentID,
				labelGeneration: desired.Generation,
			},
		},
		HostConfig: &container.HostConfig{
			NetworkMode:   container.NetworkMode("host"),
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			Mounts:        desiredMounts(desired),
		},
	}
}

func desiredMounts(desired Desired) []mount.Mount {
	runtimePaths := runtimePaths(desired.AgentID)
	return []mount.Mount{
		{Type: mount.TypeBind, Source: dockerSocketPath, Target: dockerSocketPath},
		{Type: mount.TypeBind, Source: agentStatePath, Target: agentStatePath},
		{Type: mount.TypeBind, Source: channelPath, Target: channelPath, ReadOnly: true},
		{Type: mount.TypeBind, Source: runtimePaths.Config, Target: agentConfigPath, ReadOnly: true},
		{Type: mount.TypeBind, Source: runtimePaths.Token, Target: agentTokenPath, ReadOnly: true},
	}
}

func stateFromInspect(inspect container.InspectResponse) State {
	state := State{Exists: true, ID: inspect.ID}
	if inspect.Config != nil {
		state.Image = inspect.Config.Image
		state.AgentID = inspect.Config.Labels[labelAgentID]
		state.Generation = inspect.Config.Labels[labelGeneration]
		state.Owned = hasOwnershipLabels(inspect.Config.Labels)
	}
	if inspect.State != nil {
		state.Running = inspect.State.Running
	}
	return state
}

func matchesDesired(inspect container.InspectResponse, desired Desired) bool {
	if inspect.Config == nil || inspect.HostConfig == nil {
		return false
	}
	labels := inspect.Config.Labels
	return inspect.Config.Image == desired.Image &&
		inspect.Config.User == "0" &&
		hasOwnershipLabels(labels) &&
		labels[labelAgentID] == desired.AgentID &&
		labels[labelGeneration] == desired.Generation &&
		inspect.HostConfig.NetworkMode == container.NetworkMode("host") &&
		inspect.HostConfig.RestartPolicy.Name == container.RestartPolicyDisabled &&
		inspect.HostConfig.RestartPolicy.MaximumRetryCount == 0 &&
		equalMounts(inspect.HostConfig.Mounts, desiredMounts(desired))
}

func hasOwnershipLabels(labels map[string]string) bool {
	return labels[labelManaged] == "true" && labels[labelKind] == "agent"
}

func equalMounts(actual, desired []mount.Mount) bool {
	if len(actual) != len(desired) {
		return false
	}
	actualKeys := mountKeys(actual)
	desiredKeys := mountKeys(desired)
	for i := range actualKeys {
		if actualKeys[i] != desiredKeys[i] {
			return false
		}
	}
	return true
}

func mountKeys(mounts []mount.Mount) []string {
	keys := make([]string, len(mounts))
	for i, item := range mounts {
		keys[i] = fmt.Sprintf("%s\x00%s\x00%s\x00%t", item.Type, item.Source, item.Target, item.ReadOnly)
	}
	sort.Strings(keys)
	return keys
}

func validateDesired(desired Desired) error {
	if !isDigestPinned(desired.Image) {
		return errs.New(errs.CodeValidationFailed, "agent container: image must be a caller-supplied sha256 digest reference")
	}
	if _, err := RuntimePathsForAgent(desired.AgentID); err != nil {
		return err
	}
	if strings.TrimSpace(desired.Generation) == "" {
		return errs.New(errs.CodeValidationFailed, "agent container: generation is required")
	}
	return nil
}

func isCanonicalAgentID(agentID string) bool {
	const prefix = "agt_"
	if !strings.HasPrefix(agentID, prefix) {
		return false
	}
	encoded := strings.TrimPrefix(agentID, prefix)
	id, err := ulid.ParseStrict(encoded)
	return err == nil && id.String() == encoded
}

func isDigestPinned(image string) bool {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return false
	}
	digested, ok := named.(reference.Digested)
	if !ok {
		return false
	}
	parsed, err := digest.Parse(digested.Digest().String())
	return err == nil && parsed.Algorithm() == digest.SHA256
}

func operationError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errs.Wrap(errs.CodeInternal, fmt.Errorf("agent container: %s: %w", operation, err))
}

func unownedCollision() error {
	return errs.Newf(errs.CodeInternal, "agent container: %q exists without Groundplane Agent ownership labels", ContainerName)
}
