package managedconfighelpercontainer

import (
	"context"
	"errors"
	"io"
	"iter"
	"reflect"
	"strings"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	containerderrdefs "github.com/containerd/errdefs"
	imagetypes "github.com/moby/moby/api/types/image"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const pinnedValidatorImage = "coredns/coredns@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
const pinnedValidatorID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func validatorTestImage() ValidatorImage {
	return ValidatorImage{
		Reference: pinnedValidatorImage,
		Platform: componentsdk.OCIPlatform{
			OS:           "linux",
			Architecture: "amd64",
			ChildDigest:  strings.Split(pinnedValidatorImage, "@sha256:")[1],
			ConfigDigest: strings.TrimPrefix(pinnedValidatorID, "sha256:"),
		},
	}
}

// Rationale: low-port validation needs one capability without weakening any existing validator isolation control.
func TestValidationCreateOptionsGrantOnlyNetBindService(t *testing.T) {
	options := validationCreateOptions(pinnedValidatorImage, []string{"-conf", "/dev/stdin"})

	if options.Config == nil {
		t.Fatal("Config = nil")
	}
	if options.Config.User != "65534:65534" {
		t.Fatalf("User = %q, want %q", options.Config.User, "65534:65534")
	}
	if !options.Config.NetworkDisabled {
		t.Fatal("NetworkDisabled = false, want true")
	}
	if options.HostConfig == nil {
		t.Fatal("HostConfig = nil")
	}
	if options.HostConfig.NetworkMode != "none" {
		t.Fatalf("NetworkMode = %q, want %q", options.HostConfig.NetworkMode, "none")
	}
	if options.HostConfig.RestartPolicy.Name != "no" {
		t.Fatalf("RestartPolicy.Name = %q, want %q", options.HostConfig.RestartPolicy.Name, "no")
	}
	if !options.HostConfig.ReadonlyRootfs {
		t.Fatal("ReadonlyRootfs = false, want true")
	}
	if want := []string{"ALL"}; !reflect.DeepEqual(options.HostConfig.CapDrop, want) {
		t.Fatalf("CapDrop = %#v, want %#v", options.HostConfig.CapDrop, want)
	}
	if want := []string{"NET_BIND_SERVICE"}; !reflect.DeepEqual(options.HostConfig.CapAdd, want) {
		t.Fatalf("CapAdd = %#v, want %#v", options.HostConfig.CapAdd, want)
	}
	if want := []string{"no-new-privileges"}; !reflect.DeepEqual(options.HostConfig.SecurityOpt, want) {
		t.Fatalf("SecurityOpt = %#v, want %#v", options.HostConfig.SecurityOpt, want)
	}
}

// Rationale: a clean host must acquire the already-authorized immutable validator image before container creation.
func TestExecutorValidatePullsMissingPinnedImageBeforeCreate(t *testing.T) {
	engine := &fakeEngine{
		imageID:    pinnedValidatorID,
		inspectErr: containerderrdefs.ErrNotFound,
		createErr:  errors.New("stop after create"),
	}
	executor := mustExecutor(t, engine)

	err := executor.Validate(
		t.Context(),
		validatorTestImage(),
		[]string{"-conf", "/dev/stdin"},
		[]byte(".:53 {}"),
	)

	if err == nil {
		t.Fatal("Validate() error = nil, want container create error")
	}
	wantEvents := []string{
		"inspect:" + pinnedValidatorImage,
		"pull:" + pinnedValidatorImage,
		"pull-wait",
		"pull-close",
		"inspect:" + pinnedValidatorImage,
		"create:" + pinnedValidatorID,
	}
	if !reflect.DeepEqual(engine.events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", engine.events, wantEvents)
	}
}

// Rationale: a cached immutable validator image must not incur a registry pull before validation.
func TestExecutorValidateCreatesFromCachedPinnedImageWithoutPull(t *testing.T) {
	engine := &fakeEngine{imageID: pinnedValidatorID, createErr: errors.New("stop after create")}
	executor := mustExecutor(t, engine)

	err := executor.Validate(
		t.Context(),
		validatorTestImage(),
		[]string{"-conf", "/dev/stdin"},
		[]byte(".:53 {}"),
	)

	if err == nil {
		t.Fatal("Validate() error = nil, want container create error")
	}
	wantEvents := []string{"inspect:" + pinnedValidatorImage, "create:" + pinnedValidatorID}
	if !reflect.DeepEqual(engine.events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", engine.events, wantEvents)
	}
}

// Rationale: registry acquisition failure must fail validation without attempting to create an unavailable image.
func TestExecutorValidateDoesNotCreateWhenPinnedImagePullFails(t *testing.T) {
	pullErr := errors.New("registry unavailable")
	engine := &fakeEngine{inspectErr: containerderrdefs.ErrNotFound, pullErr: pullErr}
	executor := mustExecutor(t, engine)

	err := executor.Validate(
		t.Context(),
		validatorTestImage(),
		[]string{"-conf", "/dev/stdin"},
		[]byte(".:53 {}"),
	)

	if !errors.Is(err, pullErr) {
		t.Fatalf("Validate() error = %v, want cause %v", err, pullErr)
	}
	wantEvents := []string{"inspect:" + pinnedValidatorImage, "pull:" + pinnedValidatorImage}
	if !reflect.DeepEqual(engine.events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", engine.events, wantEvents)
	}
}

// Rationale: a failed pull stream must still close its response and must prevent validator creation.
func TestExecutorValidateDoesNotCreateWhenPinnedImagePullWaitFails(t *testing.T) {
	waitErr := errors.New("pull stream failed")
	closeErr := errors.New("pull response close failed")
	engine := &fakeEngine{
		inspectErr: containerderrdefs.ErrNotFound, pullWaitErr: waitErr, pullCloseErr: closeErr,
	}
	executor := mustExecutor(t, engine)

	err := executor.Validate(
		t.Context(),
		validatorTestImage(),
		[]string{"-conf", "/dev/stdin"},
		[]byte(".:53 {}"),
	)

	if !errors.Is(err, waitErr) {
		t.Fatalf("Validate() error = %v, want cause %v", err, waitErr)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("Validate() error = %v, want cleanup cause %v", err, closeErr)
	}
	wantEvents := []string{
		"inspect:" + pinnedValidatorImage,
		"pull:" + pinnedValidatorImage,
		"pull-wait",
		"pull-close",
	}
	if !reflect.DeepEqual(engine.events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", engine.events, wantEvents)
	}
}

// Rationale: successfully receiving an image is incomplete until the pull response closes cleanly.
func TestExecutorValidateDoesNotCreateWhenPinnedImagePullCloseFails(t *testing.T) {
	closeErr := errors.New("pull response close failed")
	engine := &fakeEngine{inspectErr: containerderrdefs.ErrNotFound, pullCloseErr: closeErr}
	executor := mustExecutor(t, engine)

	err := executor.Validate(
		t.Context(),
		validatorTestImage(),
		[]string{"-conf", "/dev/stdin"},
		[]byte(".:53 {}"),
	)

	if !errors.Is(err, closeErr) {
		t.Fatalf("Validate() error = %v, want cause %v", err, closeErr)
	}
	wantEvents := []string{
		"inspect:" + pinnedValidatorImage,
		"pull:" + pinnedValidatorImage,
		"pull-wait",
		"pull-close",
	}
	if !reflect.DeepEqual(engine.events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", engine.events, wantEvents)
	}
}

// Rationale: only typed image absence authorizes a pull; other inspect failures must fail closed.
func TestExecutorValidateDoesNotPullOrCreateAfterImageInspectFailure(t *testing.T) {
	inspectErr := errors.New("daemon unavailable")
	engine := &fakeEngine{inspectErr: inspectErr}
	executor := mustExecutor(t, engine)

	err := executor.Validate(
		t.Context(),
		validatorTestImage(),
		[]string{"-conf", "/dev/stdin"},
		[]byte(".:53 {}"),
	)

	if !errors.Is(err, inspectErr) {
		t.Fatalf("Validate() error = %v, want cause %v", err, inspectErr)
	}
	wantEvents := []string{"inspect:" + pinnedValidatorImage}
	if !reflect.DeepEqual(engine.events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", engine.events, wantEvents)
	}
}

func mustExecutor(t *testing.T, engine Engine) *Executor {
	t.Helper()
	executor, err := NewWithEngine(engine, pinnedValidatorImage)
	if err != nil {
		t.Fatalf("NewWithEngine() error = %v", err)
	}
	return executor
}

type fakeEngine struct {
	imageID      string
	descriptor   *ocispec.Descriptor
	platform     *ocispec.Platform
	events       []string
	inspectErr   error
	pullErr      error
	pullWaitErr  error
	pullCloseErr error
	createErr    error
	createdID    string
	attachErr    error
}

func (engine *fakeEngine) ImageInspect(
	_ context.Context,
	image string,
	_ ...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	engine.events = append(engine.events, "inspect:"+image)
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	if engine.platform != nil {
		platform = *engine.platform
	}
	return client.ImageInspectResult{
		InspectResponse: imagetypes.InspectResponse{
			ID:           engine.imageID,
			Descriptor:   engine.descriptor,
			Os:           platform.OS,
			Architecture: platform.Architecture,
			Variant:      platform.Variant,
		},
	}, engine.inspectErr
}

func (engine *fakeEngine) ImagePull(
	_ context.Context,
	image string,
	_ client.ImagePullOptions,
) (client.ImagePullResponse, error) {
	engine.events = append(engine.events, "pull:"+image)
	if engine.pullErr != nil {
		return nil, engine.pullErr
	}
	return &fakeImagePullResponse{engine: engine}, nil
}

func (engine *fakeEngine) ContainerCreate(
	_ context.Context,
	options client.ContainerCreateOptions,
) (client.ContainerCreateResult, error) {
	engine.events = append(engine.events, "create:"+options.Config.Image)
	return client.ContainerCreateResult{ID: engine.createdID}, engine.createErr
}

func (engine *fakeEngine) ContainerAttach(
	context.Context,
	string,
	client.ContainerAttachOptions,
) (client.ContainerAttachResult, error) {
	engine.events = append(engine.events, "attach")
	return client.ContainerAttachResult{}, engine.attachErr
}

func (*fakeEngine) ContainerWait(
	context.Context,
	string,
	client.ContainerWaitOptions,
) client.ContainerWaitResult {
	panic("unexpected ContainerWait call")
}

func (*fakeEngine) ContainerStart(
	context.Context,
	string,
	client.ContainerStartOptions,
) (client.ContainerStartResult, error) {
	panic("unexpected ContainerStart call")
}

func (*fakeEngine) ContainerStop(
	context.Context,
	string,
	client.ContainerStopOptions,
) (client.ContainerStopResult, error) {
	panic("unexpected ContainerStop call")
}

func (engine *fakeEngine) ContainerRemove(
	context.Context,
	string,
	client.ContainerRemoveOptions,
) (client.ContainerRemoveResult, error) {
	engine.events = append(engine.events, "remove")
	return client.ContainerRemoveResult{}, nil
}

func (*fakeEngine) Close() error { return nil }

type fakeImagePullResponse struct {
	engine *fakeEngine
}

func (*fakeImagePullResponse) Read([]byte) (int, error) { return 0, io.EOF }

func (*fakeImagePullResponse) JSONMessages(context.Context) iter.Seq2[jsonstream.Message, error] {
	return func(func(jsonstream.Message, error) bool) {}
}

func (response *fakeImagePullResponse) Wait(context.Context) error {
	response.engine.events = append(response.engine.events, "pull-wait")
	if response.engine.pullWaitErr == nil {
		response.engine.inspectErr = nil
	}
	return response.engine.pullWaitErr
}

func (response *fakeImagePullResponse) Close() error {
	response.engine.events = append(response.engine.events, "pull-close")
	return response.engine.pullCloseErr
}
