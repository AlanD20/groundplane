package managedconfighelpercontainer

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateToExit provisions the complete native candidate in a disposable,
// networkless container. It never mounts the serving file or any host path.
func (executor *Executor) ValidateToExit(
	ctx context.Context, image ValidatorImage, arguments []string, content []byte,
) (resultErr error) {
	if executor == nil || executor.engine == nil || ctx == nil || !imageref.IsDigestPinned(image.Reference) ||
		!validValidatorPlatform(
			image.Platform,
		) || !strings.HasSuffix(image.Reference, "@sha256:"+image.Platform.ChildDigest) ||
		len(
			arguments,
		) == 0 || arguments[0] == "" || len(content) == 0 || uint64(len(content)) > entrymaterialization.MaximumContentBytes {
		return errs.New(errs.KindValidationFailed, "native file validator configuration is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	localID, err := executor.ensureImage(ctx, image)
	if err != nil {
		return err
	}
	created, err := executor.engine.ContainerCreate(ctx, nativeValidationOptions(localID, arguments))
	if err != nil {
		return operationError(ctx, "create native validator", err)
	}
	if created.ID == "" {
		return errs.New(errs.KindInternal, "native validator create returned an empty container id")
	}
	defer func() {
		if err := executor.remove(created.ID); err != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, err))
		}
	}()
	attached, err := executor.engine.ContainerAttach(ctx, created.ID, client.ContainerAttachOptions{
		Stream: true, Stdin: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		return operationError(ctx, "attach native validator", err)
	}
	if attached.Conn == nil || attached.Reader == nil {
		if attached.Conn != nil {
			attached.Close()
		}
		return errs.New(errs.KindInternal, "native validator attachment is incomplete")
	}
	defer attached.Close()
	stopClose := context.AfterFunc(ctx, attached.Close)
	defer stopClose()
	outputDone := make(chan outputResult, 1)
	go readOutput(attached.Reader, outputDone)
	defer func() {
		if resultErr != nil {
			attached.Close()
		}
		output := <-outputDone
		attached.Close()
		clear(output.stdout)
		clear(output.stderr)
		if output.err != nil {
			resultErr = errs.Wrap(errs.KindRequestFailed, errors.Join(resultErr, output.err))
		}
		if ctx.Err() != nil {
			resultErr = errs.Wrap(errs.KindRequestFailed, errors.Join(resultErr, ctx.Err()))
		}
	}()
	wait := executor.engine.ContainerWait(
		ctx,
		created.ID,
		client.ContainerWaitOptions{Condition: container.WaitConditionNextExit},
	)
	if wait.Result == nil || wait.Error == nil {
		return errs.New(errs.KindInternal, "native validator wait channels are not configured")
	}
	if _, err := executor.engine.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		return operationError(ctx, "start native validator", err)
	}
	if err := writeAll(attached.Conn, content); err != nil {
		return operationError(ctx, "write native validator candidate", err)
	}
	if err := attached.CloseWrite(); err != nil {
		return operationError(ctx, "close native validator candidate", err)
	}
	select {
	case response, open := <-wait.Result:
		if !open || response.Error != nil || response.StatusCode != 0 {
			return errs.New(errs.KindRequestFailed, "native Component configuration was rejected")
		}
		return ctx.Err()
	case err, open := <-wait.Error:
		if !open || err == nil {
			return errs.New(errs.KindInternal, "native validator wait failed without an error")
		}
		return operationError(ctx, "wait for native validator", err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nativeValidationOptions(image string, arguments []string) client.ContainerCreateOptions {
	options := validationCreateOptions(image, arguments[1:])
	options.Config.Entrypoint = []string{arguments[0]}
	options.Config.Env = []string{
		"XDG_DATA_HOME=/tmp/groundplane-validator/data", "XDG_CONFIG_HOME=/tmp/groundplane-validator/config",
	}
	options.HostConfig.Tmpfs = map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=16777216,mode=1777"}
	options.HostConfig.LogConfig = container.LogConfig{Type: "none"}
	pids := int64(64)
	options.HostConfig.Resources = container.Resources{Memory: 256 << 20, NanoCPUs: 500000000, PidsLimit: &pids}
	return options
}
