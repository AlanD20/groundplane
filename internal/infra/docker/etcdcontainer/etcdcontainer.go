// Package etcdcontainer owns the private single-node etcd container required
// before the Controller can open its durable store.
package etcdcontainer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"slices"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ContainerName = "groundplane-etcd"
	DataDirectory = "/var/lib/groundplane/etcd"
	Image         = "gcr.io/etcd-development/etcd@sha256:a491baeaa0cb0c9cd89c0062ac44ece53886e3e5bddad18d2daf36678ce665b6"

	dockerHost        = "unix:///var/run/docker.sock"
	endpoint          = "127.0.0.1:2379"
	reconcileInterval = 2 * time.Second
	startupTimeout    = 30 * time.Second

	labelManaged = "com.groundplane.managed"
	labelKind    = "com.groundplane.kind"
)

var command = []string{
	"--name=groundplane",
	"--data-dir=" + DataDirectory,
	"--listen-client-urls=http://127.0.0.1:2379",
	"--advertise-client-urls=http://127.0.0.1:2379",
	"--listen-peer-urls=http://127.0.0.1:2380",
	"--initial-advertise-peer-urls=http://127.0.0.1:2380",
	"--initial-cluster=groundplane=http://127.0.0.1:2380",
	"--initial-cluster-token=groundplane-etcd",
	"--initial-cluster-state=new",
	"--max-txn-ops=256",
	"--logger=zap",
	"--log-outputs=stderr",
}

type engineClient interface {
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerStop(context.Context, string, client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	ImagePull(context.Context, string, client.ImagePullOptions) (client.ImagePullResponse, error)
	Close() error
}

type Manager struct {
	client engineClient
}

func Endpoints() []string {
	return []string{endpoint}
}

func New(ctx context.Context) (*Manager, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "etcd container: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ensureDataDirectory(); err != nil {
		return nil, err
	}
	engine, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		return nil, operationError(ctx, "create Docker client", err)
	}
	return &Manager{client: engine}, nil
}

func (m *Manager) Reconcile(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "etcd container: reconcile context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inspected, exists, err := m.inspect(ctx)
	if err != nil {
		return err
	}
	if exists && !owned(inspected) {
		return errs.Newf(errs.KindInternal, "etcd container: %q exists without Groundplane ownership labels", ContainerName)
	}
	if exists && !matchesDesired(inspected) {
		if _, err := m.client.ContainerRemove(ctx, inspected.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
			return operationError(ctx, "remove drifted container", err)
		}
		exists = false
	}
	if !exists {
		if err := m.pullImage(ctx); err != nil {
			return err
		}
		created, err := m.client.ContainerCreate(ctx, createOptions())
		if err != nil {
			return operationError(ctx, "create container", err)
		}
		if created.ID == "" {
			return errs.New(errs.KindInternal, "etcd container: Docker returned an empty container id")
		}
		if _, err := m.client.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
			return operationError(ctx, "start created container", err)
		}
	} else if inspected.State == nil || !inspected.State.Running {
		if _, err := m.client.ContainerStart(ctx, inspected.ID, client.ContainerStartOptions{}); err != nil {
			return operationError(ctx, "start stopped container", err)
		}
	}
	return m.waitReady(ctx)
}

func (m *Manager) Run(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "etcd container: run context is required")
	}
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := m.Reconcile(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func (m *Manager) Close() error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var stopErr error
	inspected, exists, err := m.inspect(shutdownCtx)
	if err != nil {
		stopErr = err
	} else if exists && !owned(inspected) {
		stopErr = errs.Newf(errs.KindInternal, "etcd container: refusing to stop unowned container %q", ContainerName)
	} else if exists && inspected.State != nil && inspected.State.Running {
		timeout := 10
		if _, err := m.client.ContainerStop(shutdownCtx, inspected.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil && !containerderrdefs.IsNotFound(err) {
			stopErr = operationError(shutdownCtx, "stop container", err)
		}
	}
	closeErr := m.client.Close()
	if closeErr != nil {
		closeErr = errs.Wrap(errs.KindInternal, fmt.Errorf("etcd container: close Docker client: %w", closeErr))
	}
	return errors.Join(stopErr, closeErr)
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

func (m *Manager) pullImage(ctx context.Context) error {
	pull, err := m.client.ImagePull(ctx, Image, client.ImagePullOptions{})
	if err != nil {
		return operationError(ctx, "pull image", err)
	}
	defer pull.Close()
	if err := pull.Wait(ctx); err != nil {
		return operationError(ctx, "wait for image pull", err)
	}
	return nil
}

func createOptions() client.ContainerCreateOptions {
	return client.ContainerCreateOptions{
		Name: ContainerName,
		Config: &container.Config{
			Image:      Image,
			User:       "0",
			Entrypoint: []string{"/usr/local/bin/etcd"},
			Cmd:        append([]string(nil), command...),
			Labels: map[string]string{
				labelManaged: "true",
				labelKind:    "etcd",
			},
		},
		HostConfig: &container.HostConfig{
			NetworkMode:   container.NetworkMode("host"),
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			Mounts: []mount.Mount{{
				Type:   mount.TypeBind,
				Source: DataDirectory,
				Target: DataDirectory,
			}},
		},
	}
}

func owned(inspected container.InspectResponse) bool {
	return inspected.Config != nil && inspected.Config.Labels[labelManaged] == "true" && inspected.Config.Labels[labelKind] == "etcd"
}

func matchesDesired(inspected container.InspectResponse) bool {
	if !owned(inspected) || inspected.HostConfig == nil {
		return false
	}
	desired := createOptions()
	return inspected.Config.Image == desired.Config.Image &&
		inspected.Config.User == desired.Config.User &&
		equalEtcdStrings(inspected.Config.Entrypoint, desired.Config.Entrypoint) &&
		equalEtcdStrings(inspected.Config.Cmd, desired.Config.Cmd) &&
		inspected.HostConfig.NetworkMode == desired.HostConfig.NetworkMode &&
		inspected.HostConfig.RestartPolicy.Name == container.RestartPolicyDisabled &&
		inspected.HostConfig.RestartPolicy.MaximumRetryCount == 0 &&
		equalEtcdMounts(inspected.HostConfig.Mounts, desired.HostConfig.Mounts)
}

func equalEtcdMounts(left, right []mount.Mount) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !equalEtcdMount(left[index], right[index]) {
			return false
		}
	}
	return true
}

func equalEtcdStrings(left, right []string) bool {
	return (left == nil) == (right == nil) && slices.Equal(left, right)
}

func equalEtcdStringMap(left, right map[string]string) bool {
	return (left == nil) == (right == nil) && maps.Equal(left, right)
}

func equalEtcdStringMatrix(left, right [][]string) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for index := range left {
		if !equalEtcdStrings(left[index], right[index]) {
			return false
		}
	}
	return true
}

func equalEtcdMount(left, right mount.Mount) bool {
	return left.Type == right.Type && left.Source == right.Source && left.Target == right.Target &&
		left.ReadOnly == right.ReadOnly && left.Consistency == right.Consistency &&
		equalEtcdBindOptions(left.BindOptions, right.BindOptions) &&
		equalEtcdVolumeOptions(left.VolumeOptions, right.VolumeOptions) &&
		equalEtcdImageOptions(left.ImageOptions, right.ImageOptions) &&
		equalEtcdTmpfsOptions(left.TmpfsOptions, right.TmpfsOptions) &&
		(left.ClusterOptions == nil) == (right.ClusterOptions == nil)
}

func equalEtcdBindOptions(left, right *mount.BindOptions) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalEtcdVolumeOptions(left, right *mount.VolumeOptions) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.NoCopy == right.NoCopy && equalEtcdStringMap(left.Labels, right.Labels) && left.Subpath == right.Subpath &&
		equalEtcdDrivers(left.DriverConfig, right.DriverConfig)
}

func equalEtcdDrivers(left, right *mount.Driver) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Name == right.Name && equalEtcdStringMap(left.Options, right.Options)
}

func equalEtcdImageOptions(left, right *mount.ImageOptions) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Subpath == right.Subpath
}

func equalEtcdTmpfsOptions(left, right *mount.TmpfsOptions) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.SizeBytes == right.SizeBytes && left.Mode == right.Mode && equalEtcdStringMatrix(left.Options, right.Options)
}

func ensureDataDirectory() error {
	if err := os.MkdirAll(DataDirectory, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("etcd container: create data directory: %w", err))
	}
	info, err := os.Lstat(DataDirectory)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("etcd container: inspect data directory: %w", err))
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errs.New(errs.KindInternal, "etcd container: data path must be a directory, not a symlink")
	}
	if err := os.Chmod(DataDirectory, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("etcd container: secure data directory: %w", err))
	}
	return nil
}

func (m *Manager) waitReady(ctx context.Context) error {
	deadline := time.NewTimer(startupTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		connection, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", endpoint)
		if err == nil {
			_ = connection.Close()
			inspected, exists, inspectErr := m.inspect(ctx)
			if inspectErr != nil {
				return inspectErr
			}
			if exists && owned(inspected) && inspected.State != nil && inspected.State.Running {
				return nil
			}
			return errs.New(errs.KindStorageUnavailable, "etcd container: managed process stopped before readiness")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errs.New(errs.KindStorageUnavailable, "etcd container: endpoint did not become ready")
		case <-ticker.C:
		}
	}
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
	return errs.Wrap(errs.KindInternal, fmt.Errorf("etcd container: %s: %w", operation, err))
}
