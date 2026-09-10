// Package scriptrunner executes one sealed Script runner projection through
// the local Docker Engine without using exec, copy, or container discovery.
package scriptrunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/blkiodev"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	dockerHost     = "unix:///var/run/docker.sock"
	bodyTarget     = "/groundplane-script-body"
	cleanupTimeout = 60 * time.Second
	stopSeconds    = 10
)

type networkConnector interface {
	NetworkConnect(context.Context, string, client.NetworkConnectOptions) (client.NetworkConnectResult, error)
}

type engineClient interface {
	networkConnector
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerAttach(context.Context, string, client.ContainerAttachOptions) (client.ContainerAttachResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerKill(context.Context, string, client.ContainerKillOptions) (client.ContainerKillResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	Close() error
}

type Runner struct {
	client engineClient
	bodies *bodyStore
}

func New(ctx context.Context) (*Runner, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "Script runner: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bodies, err := newBodyStore(agentprotocol.StatePath, 0, 0)
	if err != nil {
		return nil, err
	}
	engine, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		_ = bodies.Close()
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("Script runner: create Docker client: %w", err))
	}
	return &Runner{client: engine, bodies: bodies}, nil
}

func (runner *Runner) Close() error {
	if runner == nil {
		return nil
	}
	var engineErr, bodyErr error
	if runner.client != nil {
		engineErr = runner.client.Close()
	}
	if runner.bodies != nil {
		bodyErr = runner.bodies.Close()
	}
	if joined := errors.Join(engineErr, bodyErr); joined != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("Script runner: close runtime: %w", joined))
	}
	return nil
}

func (runner *Runner) PrepareBody(
	ctx context.Context,
	request scriptexecution.Request,
) (scriptexecution.BodyEvidence, error) {
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return scriptexecution.BodyEvidence{}, errs.New(errs.KindInternal, "Script runner: runtime is not configured")
	}
	if err := validateRequest(request); err != nil {
		return scriptexecution.BodyEvidence{}, err
	}
	prepared, err := runner.bodies.Prepare(
		request.AssignmentID,
		request.ExecutionID,
		request.Body,
		request.BodyMetadata.Uid,
		request.BodyMetadata.Gid,
	)
	if err != nil {
		return scriptexecution.BodyEvidence{}, err
	}
	evidence := bodyEvidence(prepared)
	if err := runner.bodies.PrepareEntries(request.AssignmentID, request.ExecutionID, request.Entries); err != nil {
		return evidence, err
	}
	return evidence, nil
}

func (runner *Runner) RecoverContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
) (scriptexecution.ContainerRecovery, error) {
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return scriptexecution.ContainerRecovery{}, errs.New(
			errs.KindInternal,
			"Script runner: runtime is not configured",
		)
	}
	if err := validateRequest(request); err != nil {
		return scriptexecution.ContainerRecovery{}, err
	}
	prepared, err := preparedBodyForEvidence(runner.bodies, request, body)
	if err != nil {
		return scriptexecution.ContainerRecovery{}, err
	}
	inspected, err := runner.client.ContainerInspect(ctx, request.Projection.Name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return scriptexecution.ContainerRecovery{Found: false}, nil
	}
	if err != nil {
		return scriptexecution.ContainerRecovery{}, operationError(ctx, "recover container by name", err)
	}
	if !validDockerContainerID(inspected.Container.ID) {
		return scriptexecution.ContainerRecovery{}, errs.New(
			errs.KindStateConflict,
			"Script runner: recovered container id is invalid",
		)
	}
	if err := validateOwnedContainer(inspected, inspected.Container.ID, request, prepared); err != nil {
		return scriptexecution.ContainerRecovery{}, err
	}
	if inspected.Container.State.Status != container.StateCreated || inspected.Container.State.Running {
		return scriptexecution.ContainerRecovery{}, errs.New(
			errs.KindStateConflict,
			"Script runner: recovered container is not stopped in created state",
		)
	}
	return scriptexecution.ContainerRecovery{
		Found:    true,
		Evidence: containerEvidence(request, inspected.Container.ID),
	}, nil
}

func (runner *Runner) CreateContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
) (scriptexecution.ContainerEvidence, error) {
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return scriptexecution.ContainerEvidence{}, errs.New(
			errs.KindInternal,
			"Script runner: runtime is not configured",
		)
	}
	if err := validateRequest(request); err != nil {
		return scriptexecution.ContainerEvidence{}, err
	}
	prepared, err := preparedBodyForEvidence(runner.bodies, request, body)
	if err != nil {
		return scriptexecution.ContainerEvidence{}, err
	}

	options, err := createOptions(request, prepared.hostPath)
	if err != nil {
		return scriptexecution.ContainerEvidence{}, err
	}
	created, err := runner.client.ContainerCreate(ctx, options)
	if created.ID == "" && err != nil {
		return scriptexecution.ContainerEvidence{}, operationError(ctx, "create container", err)
	}
	if created.ID == "" {
		return scriptexecution.ContainerEvidence{}, errs.New(
			errs.KindInternal,
			"Script runner: Docker returned an empty container id",
		)
	}
	evidence := containerEvidence(request, created.ID)
	if !validDockerContainerID(created.ID) {
		return evidence, errs.New(errs.KindStateConflict, "Script runner: Docker returned an invalid container id")
	}
	inspected, inspectErr := runner.inspectOwnedContainer(ctx, created.ID, request, prepared)
	if inspectErr != nil {
		if kind, ok := errs.KindOf(inspectErr); ok && kind == errs.KindStateConflict {
			return evidence, inspectErr
		}
		return evidence, errs.Wrap(
			errs.KindStateConflict,
			fmt.Errorf("Script runner: inspect ambiguous created container: %w", errors.Join(err, inspectErr)),
		)
	}
	if inspected.Container.State.Status != container.StateCreated || inspected.Container.State.Running {
		return evidence, errs.New(
			errs.KindStateConflict,
			"Script runner: created container is not stopped in created state",
		)
	}
	return evidence, nil
}

func (runner *Runner) RunContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
	evidence scriptexecution.ContainerEvidence,
) (scriptexecution.RunResult, error) {
	var result scriptexecution.RunResult
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return result, errs.New(errs.KindInternal, "Script runner: runtime is not configured")
	}
	if err := validateRequest(request); err != nil {
		return result, err
	}
	prepared, err := preparedBodyForEvidence(runner.bodies, request, body)
	if err != nil {
		return result, err
	}
	expected := containerEvidence(request, evidence.ID)
	if !bytes.Equal(evidence.OwnershipLabelsSHA256, expected.OwnershipLabelsSHA256) {
		return result, errs.New(errs.KindStateConflict, "Script runner: container ownership digest differs")
	}
	inspected, err := runner.inspectOwnedContainer(ctx, evidence.ID, request, prepared)
	if err != nil {
		return result, operationError(ctx, "inspect captured container", err)
	}
	if inspected.Container.State.Status == container.StateExited && !inspected.Container.State.Running {
		return scriptExitResult(int64(inspected.Container.State.ExitCode))
	}
	if inspected.Container.State.Status != container.StateCreated &&
		inspected.Container.State.Status != container.StateRunning {
		return result, errs.New(
			errs.KindStateConflict,
			"Script runner: captured container has an invalid runtime state",
		)
	}
	if inspected.Container.State.Status == container.StateCreated {
		var connected map[string]*network.EndpointSettings
		if inspected.Container.NetworkSettings != nil {
			connected = inspected.Container.NetworkSettings.Networks
		}
		if err := connectSecondaryNetworks(ctx, runner.client, evidence.ID, request.Projection.Networks, connected); err != nil {
			return result, err
		}
	}

	attached, err := runner.client.ContainerAttach(ctx, evidence.ID, client.ContainerAttachOptions{
		Stream: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		return result, operationError(ctx, "attach output", err)
	}
	defer attached.Close()
	drained := make(chan error, 1)
	go func() {
		_, drainErr := stdcopy.StdCopy(io.Discard, io.Discard, attached.Reader)
		drained <- drainErr
	}()

	if inspected.Container.State.Status == container.StateCreated {
		if _, err := runner.client.ContainerStart(ctx, evidence.ID, client.ContainerStartOptions{}); err != nil {
			return result, operationError(ctx, "start container", err)
		}
	}
	response, err := waitForContainer(ctx, runner.client, evidence.ID)
	if err != nil {
		return result, err
	}
	if response.Error != nil {
		return result, errs.New(errs.KindInternal, "Script runner: Docker reported a wait failure")
	}
	attached.Close()
	select {
	case drainErr := <-drained:
		if drainErr != nil && !errors.Is(drainErr, io.EOF) {
			return result, errs.Wrap(errs.KindInternal, fmt.Errorf("Script runner: drain output: %w", drainErr))
		}
	case <-ctx.Done():
		return result, ctx.Err()
	}
	return scriptExitResult(response.StatusCode)
}

func (runner *Runner) Cleanup(
	request scriptexecution.Request,
	body *scriptexecution.BodyEvidence,
	containerEvidence *scriptexecution.ContainerEvidence,
) (scriptexecution.CleanupProof, error) {
	var proof scriptexecution.CleanupProof
	if runner == nil || runner.client == nil || runner.bodies == nil {
		return proof, errs.New(errs.KindInternal, "Script runner: runtime is not configured")
	}
	if err := validateRequest(request); err != nil {
		return proof, err
	}
	var prepared *preparedBody
	if body != nil {
		value, err := preparedBodyForEvidence(runner.bodies, request, *body)
		if err != nil {
			return proof, err
		}
		prepared = &value
		proof.BodyDevice, proof.BodyInode, proof.BodyLeaf = body.Device, body.Inode, body.Leaf
	}
	containerID := ""
	if containerEvidence != nil {
		expected := containerEvidenceForRequest(request, containerEvidence.ID)
		if !bytes.Equal(containerEvidence.OwnershipLabelsSHA256, expected.OwnershipLabelsSHA256) {
			return proof, errs.New(errs.KindStateConflict, "Script runner: cleanup ownership digest differs")
		}
		containerID = containerEvidence.ID
		proof.ContainerID = containerID
	}
	if err := runner.cleanup(request, containerID, prepared); err != nil {
		return proof, errs.Wrap(errs.KindInternal, fmt.Errorf("Script runner: cleanup failed: %w", err))
	}
	proof.ContainerAbsent = true
	proof.BodyAbsent = true
	proof.ExecutionDirectoryAbsent = true
	return proof, nil
}

func bodyEvidence(prepared preparedBody) scriptexecution.BodyEvidence {
	return scriptexecution.BodyEvidence{
		SHA256: append([]byte(nil), prepared.identity.SHA256[:]...), UID: prepared.identity.UID,
		GID: prepared.identity.GID, Device: prepared.identity.Device, Inode: prepared.identity.Inode,
		Leaf: prepared.identity.Leaf,
	}
}

func preparedBodyForEvidence(
	store *bodyStore,
	request scriptexecution.Request,
	evidence scriptexecution.BodyEvidence,
) (preparedBody, error) {
	if store == nil || evidence.Leaf != bodyLeaf || evidence.Device == 0 || evidence.Inode == 0 ||
		len(evidence.SHA256) != sha256.Size || !bytes.Equal(evidence.SHA256, request.BodyMetadata.Sha256) ||
		evidence.UID != request.BodyMetadata.Uid || evidence.GID != request.BodyMetadata.Gid {
		return preparedBody{}, errs.New(errs.KindStateConflict, "Script runner: durable body evidence is invalid")
	}
	var digest [sha256.Size]byte
	copy(digest[:], evidence.SHA256)
	return preparedBody{
		hostPath:     filepath.Join(store.rootPath, request.AssignmentID, request.ExecutionID, bodyLeaf),
		assignmentID: request.AssignmentID, executionID: request.ExecutionID,
		identity: bodyIdentity{Device: evidence.Device, Inode: evidence.Inode, UID: evidence.UID,
			GID: evidence.GID, Size: uint32(len(request.Body)), SHA256: digest, Leaf: evidence.Leaf},
	}, nil
}

func containerEvidenceForRequest(
	request scriptexecution.Request,
	containerID string,
) scriptexecution.ContainerEvidence {
	return containerEvidence(request, containerID)
}

func containerEvidence(request scriptexecution.Request, containerID string) scriptexecution.ContainerEvidence {
	labels := environmentList(request.Projection.Labels)
	sort.Strings(labels)
	digest := sha256.Sum256([]byte(strings.Join(labels, "\x00")))
	return scriptexecution.ContainerEvidence{ID: containerID, OwnershipLabelsSHA256: append([]byte(nil), digest[:]...)}
}

func scriptExitResult(exitCode int64) (scriptexecution.RunResult, error) {
	if exitCode < 0 || exitCode > int64(^uint32(0)>>1) {
		return scriptexecution.RunResult{}, errs.New(
			errs.KindInternal,
			"Script runner: container exit status is out of range",
		)
	}
	return scriptexecution.RunResult{ExitCode: int32(exitCode)}, nil
}

func validateRequest(request scriptexecution.Request) error {
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, request.AssignmentID) != nil ||
		ids.Validate(ids.KindOperation, request.OperationID) != nil ||
		ids.Validate(ids.KindStep, request.StepID) != nil ||
		len(request.PlanHash) != sha256.Size ||
		request.Projection == nil ||
		request.BodyMetadata == nil ||
		request.ExecutionID == "" ||
		request.BodyMetadata.ScriptExecutionId != request.ExecutionID ||
		len(request.Body) == 0 ||
		len(request.Body) != int(request.BodyMetadata.Size) ||
		len(request.Body) > executionplan.MaximumScriptBodyBytes ||
		request.Projection.Name != "gp-script-"+strings.ToLower(request.ExecutionID) ||
		!workloadimage.LocalIDValid(request.Projection.Image) ||
		len(request.Projection.Entrypoint) != 1 ||
		request.Projection.Entrypoint[0] != "/bin/sh" ||
		len(request.Projection.Command) != 1 ||
		request.Projection.Command[0] != bodyTarget ||
		request.Projection.StopGraceSeconds != stopSeconds {
		return errs.New(errs.KindInternal, "Script runner: request is invalid")
	}
	digest := sha256.Sum256(request.Body)
	if !bytes.Equal(digest[:], request.BodyMetadata.Sha256) {
		return errs.New(errs.KindInternal, "Script runner: body digest does not match")
	}
	for _, entry := range request.Entries {
		if entry == nil || entry.Binding == nil || len(entry.Binding.Sha256) != sha256.Size ||
			len(entry.Value) > 256<<10 {
			return errs.New(errs.KindInternal, "Script runner: Entry artifact is invalid")
		}
		entryDigest := sha256.Sum256(entry.Value)
		if !bytes.Equal(entryDigest[:], entry.Binding.Sha256) {
			return errs.New(errs.KindInternal, "Script runner: Entry artifact digest does not match")
		}
		switch entry.Binding.Kind {
		case agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV:
			if entry.Binding.EnvironmentKey == "" || entry.Binding.FileTarget != "" {
				return errs.New(errs.KindInternal, "Script runner: Environment Entry binding is invalid")
			}
		case agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE:
			if entry.Binding.EnvironmentKey != "" || !filepath.IsAbs(entry.Binding.FileTarget) ||
				filepath.Clean(
					entry.Binding.FileTarget,
				) != entry.Binding.FileTarget || entry.Binding.FileTarget == bodyTarget ||
				(entry.Binding.Mode != 0o444 && entry.Binding.Mode != 0o600) {
				return errs.New(errs.KindInternal, "Script runner: file Entry binding is invalid")
			}
		default:
			return errs.New(errs.KindInternal, "Script runner: Entry binding kind is invalid")
		}
	}
	return nil
}

func createOptions(request scriptexecution.Request, bodyPath string) (client.ContainerCreateOptions, error) {
	projection := request.Projection
	platform, err := parsePlatform(projection.Platform)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	dns, err := parseDNS(projection.Dns)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	mounts, err := dockerMounts(projection.Mounts, bodyPath, request.Entries)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	resources, err := dockerResources(projection)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	networks := make(map[string]*network.EndpointSettings, 1)
	networkMode := container.NetworkMode("none")
	if len(projection.Networks) != 0 {
		primary := projection.Networks[0]
		name := primary.RenderedAttachment.DockerNetworkName
		networkMode = container.NetworkMode(name)
		networks[name] = scriptEndpointSettings(primary)
	}
	return client.ContainerCreateOptions{
		Name: request.Projection.Name, Platform: platform,
		Config: &container.Config{
			Image: projection.Image, User: strconv.FormatUint(uint64(projection.Uid), 10) + ":" + strconv.FormatUint(uint64(projection.Gid), 10),
			WorkingDir: projection.WorkingDir, Env: scriptEnvironment(projection.Environment, request.Entries), Labels: pairMap(projection.Labels),
			Entrypoint: append(
				[]string(nil),
				projection.Entrypoint...), Cmd: append([]string(nil), projection.Command...),
			AttachStdout: true, AttachStderr: true, StopSignal: "SIGTERM", StopTimeout: intPointer(stopSeconds),
		},
		HostConfig: &container.HostConfig{
			LogConfig: container.LogConfig{Type: "none"}, NetworkMode: networkMode,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			DNS:           dns, DNSOptions: append([]string(nil), projection.DnsOpt...), DNSSearch: append([]string(nil), projection.DnsSearch...),
			ExtraHosts: append(
				[]string(nil),
				projection.ExtraHosts...), GroupAdd: append([]string(nil), projection.GroupAdd...),
			CapDrop: append([]string(nil), projection.CapDrop...), ReadonlyRootfs: projection.ReadOnly,
			SecurityOpt: append([]string(nil), projection.SecurityOpt...), StorageOpt: pairMap(projection.StorageOpt),
			Tmpfs: tmpfsMap(projection.Tmpfs), ShmSize: projection.ShmSize, Sysctls: pairMap(projection.Sysctls),
			Runtime: projection.Runtime, Isolation: container.Isolation(projection.Isolation), Resources: resources,
			Mounts: mounts, Init: projection.Init,
		},
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: networks},
	}, nil
}

func connectSecondaryNetworks(
	ctx context.Context,
	connector networkConnector,
	containerID string,
	networks []*agentpb.ScriptRunnerNetwork,
	connected map[string]*network.EndpointSettings,
) error {
	for _, item := range networks[min(1, len(networks)):] {
		name := item.RenderedAttachment.DockerNetworkName
		if _, exists := connected[name]; exists {
			continue
		}
		if _, err := connector.NetworkConnect(ctx, name, client.NetworkConnectOptions{
			Container: containerID, EndpointConfig: scriptEndpointSettings(item),
		}); err != nil {
			return operationError(ctx, "connect secondary network "+name, err)
		}
	}
	return nil
}

func scriptEndpointSettings(item *agentpb.ScriptRunnerNetwork) *network.EndpointSettings {
	attachment := item.RenderedAttachment
	options := pairMap(attachment.DriverOptions)
	if attachment.InterfaceName != "" {
		if options == nil {
			options = make(map[string]string, 1)
		}
		options["com.docker.network.endpoint.ifname"] = attachment.InterfaceName
	}
	return &network.EndpointSettings{DriverOpts: options, GwPriority: int(attachment.Priority)}
}

func dockerMounts(
	values []*agentpb.ScriptRunnerMount,
	bodyPath string,
	entries []*agentpb.ScriptEntryArtifact,
) ([]mount.Mount, error) {
	result := make([]mount.Mount, 0, len(values)+len(entries)+1)
	result = append(result, mount.Mount{
		Type: mount.TypeBind, Source: bodyPath, Target: bodyTarget, ReadOnly: true,
		BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate},
	})
	for _, value := range values {
		item := value.RenderedMount
		if item == nil || item.Type != "volume" {
			return nil, errs.New(errs.KindInternal, "Script runner: unsupported sealed mount")
		}
		result = append(result, mount.Mount{
			Type: mount.TypeVolume, Source: item.Source, Target: item.Target, ReadOnly: item.ReadOnly,
			Consistency:   mount.Consistency(item.Consistency),
			VolumeOptions: &mount.VolumeOptions{NoCopy: item.VolumeNoCopy, Subpath: item.VolumeSubpath},
		})
	}
	for _, entry := range entries {
		if entry.Binding.Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE {
			continue
		}
		result = append(result, mount.Mount{
			Type:        mount.TypeBind,
			Source:      filepath.Join(filepath.Dir(bodyPath), entryArtifactLeaf(entry.Binding)),
			Target:      entry.Binding.FileTarget,
			ReadOnly:    true,
			BindOptions: &mount.BindOptions{Propagation: mount.PropagationRPrivate},
		})
	}
	return result, nil
}

func scriptEnvironment(
	values []*agentpb.ScriptStringPair,
	entries []*agentpb.ScriptEntryArtifact,
) []string {
	environment := make(map[string]string, len(values)+len(entries))
	for _, entry := range entries {
		if entry.Binding.Kind == agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV {
			environment[entry.Binding.EnvironmentKey] = string(entry.Value)
		}
	}
	for _, value := range values {
		environment[value.Key] = value.Value
	}
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, len(keys))
	for index, key := range keys {
		result[index] = key + "=" + environment[key]
	}
	return result
}

func dockerResources(projection *agentpb.ScriptRunnerProjection) (container.Resources, error) {
	pidsLimit := projection.PidsLimit
	resources := container.Resources{
		CPUShares: projection.CpuShares, Memory: projection.MemLimit,
		CPUPeriod: projection.CpuPeriod, CPUQuota: projection.CpuQuota,
		CPURealtimePeriod: projection.CpuRtPeriod, CPURealtimeRuntime: projection.CpuRtRuntime,
		CpusetCpus: projection.Cpuset, MemoryReservation: projection.MemReservation,
		MemorySwap: projection.MemswapLimit, CPUCount: projection.CpuCount, CPUPercent: int64(projection.CpuPercent),
	}
	if projection.Cpus > 0 {
		resources.NanoCPUs = int64(float64(projection.Cpus) * 1_000_000_000)
	}
	if projection.Limits != nil {
		if resources.NanoCPUs == 0 && projection.Limits.Cpus > 0 {
			resources.NanoCPUs = int64(float64(projection.Limits.Cpus) * 1_000_000_000)
		}
		if resources.Memory == 0 {
			resources.Memory = projection.Limits.MemoryBytes
		}
		if pidsLimit == 0 && projection.Limits.Pids != 0 {
			pidsLimit = projection.Limits.Pids
		}
	}
	if projection.Reservations != nil && resources.MemoryReservation == 0 {
		resources.MemoryReservation = projection.Reservations.MemoryBytes
	}
	if projection.MemSwappiness != 0 {
		resources.MemorySwappiness = int64Pointer(projection.MemSwappiness)
	}
	resources.OomKillDisable = boolPointer(projection.OomKillDisable)
	if pidsLimit != 0 {
		resources.PidsLimit = int64Pointer(pidsLimit)
	}
	for _, limit := range projection.Ulimits {
		if limit == nil {
			return container.Resources{}, errs.New(errs.KindInternal, "Script runner: invalid ulimit")
		}
		soft, hard := limit.Soft, limit.Hard
		if limit.Single != 0 {
			soft, hard = limit.Single, limit.Single
		}
		resources.Ulimits = append(resources.Ulimits, &container.Ulimit{Name: limit.Name, Soft: soft, Hard: hard})
	}
	if projection.Blkio != nil {
		resources.BlkioWeight = uint16(projection.Blkio.Weight)
		for _, value := range projection.Blkio.WeightDevices {
			resources.BlkioWeightDevice = append(
				resources.BlkioWeightDevice,
				&blkiodev.WeightDevice{Path: value.Path, Weight: uint16(value.Weight)},
			)
		}
		var err error
		if resources.BlkioDeviceReadBps, err = throttleDevices(projection.Blkio.DeviceReadBps); err != nil {
			return container.Resources{}, err
		}
		if resources.BlkioDeviceReadIOps, err = throttleDevices(projection.Blkio.DeviceReadIops); err != nil {
			return container.Resources{}, err
		}
		if resources.BlkioDeviceWriteBps, err = throttleDevices(projection.Blkio.DeviceWriteBps); err != nil {
			return container.Resources{}, err
		}
		if resources.BlkioDeviceWriteIOps, err = throttleDevices(projection.Blkio.DeviceWriteIops); err != nil {
			return container.Resources{}, err
		}
	}
	return resources, nil
}

func throttleDevices(values []*agentpb.ScriptThrottleDevice) ([]*blkiodev.ThrottleDevice, error) {
	result := make([]*blkiodev.ThrottleDevice, len(values))
	for index, value := range values {
		if value == nil || value.Rate <= 0 {
			return nil, errs.New(errs.KindInternal, "Script runner: invalid blkio throttle")
		}
		result[index] = &blkiodev.ThrottleDevice{Path: value.Path, Rate: uint64(value.Rate)}
	}
	return result, nil
}

func waitForContainer(ctx context.Context, engine engineClient, containerID string) (container.WaitResponse, error) {
	wait := engine.ContainerWait(
		ctx,
		containerID,
		client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning},
	)
	select {
	case response, ok := <-wait.Result:
		if !ok {
			return container.WaitResponse{}, errs.New(errs.KindInternal, "Script runner: Docker closed wait result")
		}
		return response, nil
	case err, ok := <-wait.Error:
		if !ok || err == nil {
			return container.WaitResponse{}, errs.New(errs.KindInternal, "Script runner: Docker closed wait error")
		}
		return container.WaitResponse{}, operationError(ctx, "wait for container", err)
	case <-ctx.Done():
		return container.WaitResponse{}, ctx.Err()
	}
}

func (runner *Runner) cleanup(
	request scriptexecution.Request,
	containerID string,
	prepared *preparedBody,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	var cleanupErrors []error
	containerAbsent := containerID == ""
	containerOwned := containerID != ""
	if containerID != "" {
		inspected, err := runner.inspectCleanupContainer(ctx, containerID, request)
		if err != nil {
			if containerderrdefs.IsNotFound(err) {
				containerAbsent = true
			} else {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("inspect container before cleanup: %w", err))
				containerOwned = false
			}
		} else if inspected.Container.State != nil && inspected.Container.State.Running {
			timeout := stopSeconds
			if _, stopErr := runner.client.ContainerStop(ctx, containerID, client.ContainerStopOptions{Signal: "SIGTERM", Timeout: &timeout}); stopErr != nil && !containerderrdefs.IsNotFound(stopErr) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("stop container: %w", stopErr))
			}
		}

		if !containerAbsent && containerOwned {
			inspected, err = runner.inspectCleanupContainer(ctx, containerID, request)
			if err != nil {
				if containerderrdefs.IsNotFound(err) {
					containerAbsent = true
				} else {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("inspect container after stop: %w", err))
					containerOwned = false
				}
			} else if inspected.Container.State != nil && inspected.Container.State.Running {
				if _, killErr := runner.client.ContainerKill(ctx, containerID, client.ContainerKillOptions{Signal: "SIGKILL"}); killErr != nil && !containerderrdefs.IsNotFound(killErr) {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("kill container: %w", killErr))
				}
			}
		}

		if !containerAbsent && containerOwned {
			if _, err = runner.inspectCleanupContainer(ctx, containerID, request); err != nil {
				if containerderrdefs.IsNotFound(err) {
					containerAbsent = true
				} else {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("inspect container before removal: %w", err))
					containerOwned = false
				}
			} else if _, removeErr := runner.client.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{Force: true}); removeErr != nil && !containerderrdefs.IsNotFound(removeErr) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove container: %w", removeErr))
			}
		}

		if !containerAbsent && containerOwned {
			if _, err = runner.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{}); err == nil {
				cleanupErrors = append(cleanupErrors, errors.New("container remains after removal"))
			} else if containerderrdefs.IsNotFound(err) {
				containerAbsent = true
			} else {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("prove container removal: %w", err))
			}
		}
	}
	if prepared != nil && containerAbsent {
		if err := runner.bodies.RemoveEntries(request.AssignmentID, request.ExecutionID, request.Entries); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove Entry evidence: %w", err))
		}
		if err := runner.bodies.Remove(*prepared); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove body evidence: %w", err))
		}
	} else if prepared != nil {
		cleanupErrors = append(cleanupErrors, errors.New("container absence is not proven; Script artifacts retained"))
	}
	if containerAbsent {
		if err := runner.bodies.ProveExecutionAbsent(request.AssignmentID, request.ExecutionID); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("prove execution directory absence: %w", err))
		}
	}
	return errors.Join(cleanupErrors...)
}

func validDockerContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func parsePlatform(value string) (*ocispec.Platform, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, "/")
	if (len(parts) != 2 && len(parts) != 3) || !validPlatformPart(parts[0]) || !validPlatformPart(parts[1]) {
		return nil, errs.New(errs.KindInternal, "Script runner: invalid sealed platform")
	}
	parsed := ocispec.Platform{OS: parts[0], Architecture: parts[1]}
	if len(parts) == 3 {
		if !validPlatformPart(parts[2]) {
			return nil, errs.New(errs.KindInternal, "Script runner: invalid sealed platform")
		}
		parsed.Variant = parts[2]
	}
	return &parsed, nil
}

func validPlatformPart(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func parseDNS(values []string) ([]netip.Addr, error) {
	result := make([]netip.Addr, len(values))
	for index, value := range values {
		parsed, err := netip.ParseAddr(value)
		if err != nil {
			return nil, errs.New(errs.KindInternal, "Script runner: invalid sealed DNS address")
		}
		result[index] = parsed
	}
	return result, nil
}

func environmentList(values []*agentpb.ScriptStringPair) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = value.Key + "=" + value.Value
	}
	return result
}

func pairMap(values []*agentpb.ScriptStringPair) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for _, value := range values {
		result[value.Key] = value.Value
	}
	return result
}

func tmpfsMap(values []string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for _, value := range values {
		target, options, _ := strings.Cut(value, ":")
		result[target] = options
	}
	return result
}

func intPointer(value int) *int       { return &value }
func int64Pointer(value int64) *int64 { return &value }
func boolPointer(value bool) *bool    { return &value }

func operationError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("Script runner: %s: %w", operation, err))
}
