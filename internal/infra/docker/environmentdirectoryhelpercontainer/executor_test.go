package environmentdirectoryhelpercontainer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const helperImage = "registry.example/groundplane-agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCreateOptionsGrantOnlyConfiguredVolumeRoot(t *testing.T) {
	root := "/srv/groundplane/vol"
	options := createOptions(helperImage, root)
	if options.Name != "" || options.Config == nil || options.HostConfig == nil {
		t.Fatalf("create options = %#v", options)
	}
	config := options.Config
	if config.Image != helperImage || config.User != "0" ||
		!reflect.DeepEqual(config.Cmd, []string{helperArgument}) ||
		!reflect.DeepEqual(config.Env, []string{environmentdirectoryhelper.VolumeRootEnv + "=" + root}) ||
		!config.AttachStdin || !config.AttachStdout || !config.AttachStderr ||
		!config.OpenStdin || !config.StdinOnce || config.Tty || config.WorkingDir != "/" ||
		!config.NetworkDisabled {
		t.Fatalf("container config = %#v", config)
	}
	host := options.HostConfig
	if host.NetworkMode != container.NetworkMode("none") || !host.ReadonlyRootfs || host.AutoRemove ||
		host.RestartPolicy.Name != container.RestartPolicyDisabled ||
		!reflect.DeepEqual([]string(host.CapDrop), []string{"ALL"}) ||
		!reflect.DeepEqual(host.SecurityOpt, []string{"no-new-privileges"}) {
		t.Fatalf("host config = %#v", host)
	}
	want := []mount.Mount{{Type: mount.TypeBind, Source: root, Target: root}}
	if !reflect.DeepEqual(host.Mounts, want) {
		t.Fatalf("mounts = %#v, want %#v", host.Mounts, want)
	}
}

func TestNewWithEngineRejectsMutableImageAndInvalidRoot(t *testing.T) {
	engine := stubEngine{}
	if _, err := NewWithEngine(
		engine,
		"registry.example/agent:latest",
		"/srv/groundplane/vol",
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("NewWithEngine(tag) error = %v", err)
	}
	if _, err := NewWithEngine(
		engine,
		helperImage,
		"relative",
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("NewWithEngine(root) error = %v", err)
	}
}

type stubEngine struct{}

func (stubEngine) ContainerCreate(
	context.Context,
	client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	return client.ContainerCreateResult{}, nil
}

func (stubEngine) ContainerAttach(
	context.Context,
	string,
	client.ContainerAttachOptions,
) (client.ContainerAttachResult, error) {
	return client.ContainerAttachResult{}, nil
}
func (stubEngine) ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult {
	return client.ContainerWaitResult{}
}

func (stubEngine) ContainerStart(
	context.Context,
	string,
	client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	return client.ContainerStartResult{}, nil
}

func (stubEngine) ContainerStop(
	context.Context,
	string,
	client.ContainerStopOptions,
) (client.ContainerStopResult, error) {
	return client.ContainerStopResult{}, nil
}

func (stubEngine) ContainerRemove(
	context.Context,
	string,
	client.ContainerRemoveOptions,
) (client.ContainerRemoveResult, error) {
	return client.ContainerRemoveResult{}, nil
}
func (stubEngine) Close() error { return nil }
