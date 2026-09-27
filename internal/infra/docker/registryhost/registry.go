// Package registryhost maintains the bootstrap registry's private host binding.
package registryhost

import (
	"context"
	"net/netip"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/registryconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

const name = "groundplane-registry"
const ownerLabel = "groundplane.registry.schema"

// Reconcile leaves a matching running registry untouched. Binding changes may
// recreate only its proven-owned container, never its persistent storage.
func Reconcile(ctx context.Context, address netip.Addr) error {
	if !address.Is4() || !(address.IsPrivate() || address == netip.MustParseAddr("127.0.0.1")) {
		return errs.New(errs.KindValidationFailed, "registry listener must be loopback or private IPv4")
	}
	settings, err := registryconfiguration.Read(ctx)
	if err != nil {
		return err
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	wanted := options(settings.Image, address)
	inspected, err := engine.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil && !errdefs.IsNotFound(err) {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err == nil {
		current := inspected.Container
		if err := validate(current, settings.Image); err != nil {
			return err
		}
		if sameBindings(current.HostConfig.PortBindings, wanted.HostConfig.PortBindings) {
			if current.State != nil && current.State.Running {
				return nil
			}
			_, err = engine.ContainerStart(ctx, current.ID, client.ContainerStartOptions{})
			return wrap(err)
		}
		if _, err := engine.ContainerRemove(ctx, current.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
			return wrap(err)
		}
	}
	created, err := engine.ContainerCreate(ctx, wanted)
	if err != nil {
		return wrap(err)
	}
	if created.ID == "" {
		return errs.New(errs.KindInternal, "registry creation returned no container identity")
	}
	_, err = engine.ContainerStart(ctx, created.ID, client.ContainerStartOptions{})
	return wrap(err)
}

func options(image string, address netip.Addr) client.ContainerCreateOptions {
	port, _ := network.PortFrom(5000, network.TCP)
	bindings := []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: "5000"}}
	if address.IsPrivate() {
		bindings = append(bindings, network.PortBinding{HostIP: address, HostPort: "5000"})
	}
	mounts := []mount.Mount{{Type: mount.TypeBind, Source: registryconfiguration.DataRoot, Target: "/var/lib/registry"}}
	for _, file := range []string{"tls.crt", "tls.key", "htpasswd"} {
		mounts = append(mounts, mount.Mount{Type: mount.TypeBind,
			Source: registryconfiguration.Root + "/" + file, Target: "/run/groundplane-registry/" + file, ReadOnly: true})
	}
	return client.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{Image: image, Labels: map[string]string{ownerLabel: "1"},
			ExposedPorts: network.PortSet{port: {}}, Env: registryEnvironment()},
		HostConfig: &container.HostConfig{
			PortBindings: network.PortMap{port: bindings}, Mounts: mounts, ReadonlyRootfs: true,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			CapDrop:       []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"},
		},
	}
}

func registryEnvironment() []string {
	return []string{
		"REGISTRY_HTTP_TLS_CERTIFICATE=/run/groundplane-registry/tls.crt",
		"REGISTRY_HTTP_TLS_KEY=/run/groundplane-registry/tls.key",
		"REGISTRY_AUTH=htpasswd", "REGISTRY_AUTH_HTPASSWD_REALM=groundplane",
		"REGISTRY_AUTH_HTPASSWD_PATH=/run/groundplane-registry/htpasswd",
		"REGISTRY_LOG_LEVEL=warn", "OTEL_TRACES_EXPORTER=none",
	}
}

func validate(current container.InspectResponse, image string) error {
	if current.ID == "" || current.Config == nil || current.HostConfig == nil ||
		current.Config.Image != image || current.Config.Labels[ownerLabel] != "1" ||
		current.HostConfig.Privileged || !current.HostConfig.ReadonlyRootfs || len(current.Mounts) != 4 {
		return errs.New(errs.KindStateConflict, "registry container ownership changed")
	}
	for _, environment := range registryEnvironment() {
		if !slices.Contains(current.Config.Env, environment) {
			return errs.New(errs.KindStateConflict, "registry security configuration changed")
		}
	}
	for _, wanted := range options(image, netip.MustParseAddr("127.0.0.1")).HostConfig.Mounts {
		found := false
		for _, actual := range current.Mounts {
			if actual.Type == mount.TypeBind && actual.Source == wanted.Source && actual.Destination == wanted.Target &&
				actual.RW == !wanted.ReadOnly {
				found = true
			}
		}
		if !found {
			return errs.New(errs.KindStateConflict, "registry storage ownership changed")
		}
	}
	return nil
}

func sameBindings(left, right network.PortMap) bool {
	if len(left) != len(right) {
		return false
	}
	for port, wanted := range right {
		actual := left[port]
		if len(actual) != len(wanted) {
			return false
		}
		for _, binding := range wanted {
			if !slices.Contains(actual, binding) {
				return false
			}
		}
	}
	return true
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindInternal, err)
}
