// Package containerterminal owns Docker's private, framed helper transport for
// one exact Controller-selected workload container. It cannot select a Service.
package containerterminal

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/client"
)

type Engine interface {
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	CopyToContainer(context.Context, string, client.CopyToContainerOptions) (client.CopyToContainerResult, error)
	ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error)
	ExecAttach(context.Context, string, client.ExecAttachOptions) (client.ExecAttachResult, error)
}

type Executor struct {
	engine Engine
	helper []byte
}

func New(engine Engine) (*Executor, error) {
	helper, err := os.ReadFile(terminalsession.HelperPath)
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	if engine == nil || len(helper) == 0 || len(helper) > 8<<20 {
		return nil, errs.New(errs.KindValidationFailed, "container terminal helper is unavailable")
	}
	return &Executor{engine: engine, helper: helper}, nil
}

func (executor *Executor) Open(
	ctx context.Context,
	request *agentpb.ContainerTerminalOpen,
) (terminalsession.Process, error) {
	options, err := agentprotocol.ContainerOptionsFromWire(request)
	if err != nil {
		return nil, err
	}
	inspected, err := executor.engine.ContainerInspect(ctx, request.ContainerId, client.ContainerInspectOptions{})
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	container := inspected.Container
	running := container.State != nil && container.State.Running && !container.State.Paused &&
		!container.State.Restarting
	privatePID := container.HostConfig != nil && string(container.HostConfig.PidMode) != "host" &&
		!strings.HasPrefix(string(container.HostConfig.PidMode), "container:")
	if container.ID != request.ContainerId || container.Config == nil || !running || !privatePID {
		return nil, errs.New(
			errs.KindStateConflict,
			"selected container is unavailable or shares another PID namespace",
		)
	}
	selected := false
	for _, source := range request.Sources {
		if serviceobservation.SelectsWorkload(container.Config.Labels, source) {
			selected = true
			break
		}
	}
	if !selected {
		return nil, errs.New(
			errs.KindStateConflict,
			"selected container no longer belongs to the captured Service runtime",
		)
	}
	directory := ".groundplane-terminal-" + options.SessionID
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, header := range []*tar.Header{
		{Name: directory, Mode: 0700, Typeflag: tar.TypeDir},
		{Name: directory + "/helper", Mode: 0500, Typeflag: tar.TypeReg, Size: int64(len(executor.helper))},
	} {
		if err := writer.WriteHeader(header); err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
	}
	if _, err := writer.Write(executor.helper); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := writer.Close(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if _, err := executor.engine.CopyToContainer(ctx, container.ID, client.CopyToContainerOptions{
		DestinationPath: "/tmp", Content: &archive,
	}); err != nil {
		return nil, errs.New(errs.KindRequestUnavailable, "container shell requires a writable /tmp directory")
	}
	created, err := executor.engine.ExecCreate(ctx, container.ID, client.ExecCreateOptions{
		User: "0:0", Cmd: []string{filepath.Join("/tmp", directory, "helper")},
		AttachStdin: true, AttachStdout: true, AttachStderr: true, TTY: false, Privileged: false,
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	if created.ID == "" {
		return nil, errs.New(errs.KindRequestUnavailable, "container terminal helper was not created")
	}
	attached, err := executor.engine.ExecAttach(ctx, created.ID, client.ExecAttachOptions{TTY: false})
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	if attached.Conn == nil || attached.Reader == nil {
		attached.Close()
		return nil, errs.New(errs.KindRequestUnavailable, "container terminal helper stream is unavailable")
	}
	return openProcess(ctx, attached, options)
}
