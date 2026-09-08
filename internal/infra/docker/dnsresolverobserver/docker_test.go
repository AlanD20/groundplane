package dnsresolverobserver

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	containertypes "github.com/moby/moby/api/types/container"
	imagetypes "github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: observation evidence must derive the config identity from the
// running container and reject either an expected/runtime or inspect/runtime mismatch.
func TestVerifiedImageConfigDigestRequiresActualRunningImage(t *testing.T) {
	expected := sha256.Sum256([]byte("expected image config"))
	actualID := "sha256:" + hex.EncodeToString(expected[:])
	if got, err := observedClassicImageConfigDigest(actualID, actualID, expected); err != nil || got != expected {
		t.Fatalf("observedClassicImageConfigDigest() = %x, %v", got, err)
	}
	other := sha256.Sum256([]byte("different image config"))
	if _, err := observedClassicImageConfigDigest("sha256:"+hex.EncodeToString(other[:]), actualID, expected); err == nil {
		t.Fatal("observedClassicImageConfigDigest() accepted unexpected running image")
	}
	if _, err := observedClassicImageConfigDigest(actualID, "sha256:"+hex.EncodeToString(other[:]), expected); err == nil {
		t.Fatal("observedClassicImageConfigDigest() accepted mismatched image inspection")
	}
}

type imageIdentityEngine struct {
	containerImageID    string
	inspectedImageID    string
	imageReference      string
	artifact            []byte
	descriptor          *ocispec.Descriptor
	containerDescriptor *ocispec.Descriptor
	platform            ocispec.Platform
}

func (engine imageIdentityEngine) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	return client.ContainerListResult{Items: []containertypes.Summary{{ID: "container"}}}, nil
}

func (engine imageIdentityEngine) ContainerInspect(
	context.Context,
	string,
	client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	return client.ContainerInspectResult{Container: containertypes.InspectResponse{
		ID: "container", Image: engine.containerImageID, State: &containertypes.State{Running: true},
		ImageManifestDescriptor: engine.containerDescriptor,
		Config: &containertypes.Config{
			Image: engine.imageReference, Tty: true, Labels: map[string]string{"owned": "true"},
		},
		Mounts: []containertypes.MountPoint{{Destination: "/etc/coredns", RW: false}},
	}}, nil
}

func (engine imageIdentityEngine) ContainerLogs(
	context.Context,
	string,
	client.ContainerLogsOptions,
) (client.ContainerLogsResult, error) {
	return io.NopCloser(bytes.NewReader([]byte("runtime log\n"))), nil
}

func (engine imageIdentityEngine) CopyFromContainer(
	context.Context,
	string,
	client.CopyFromContainerOptions,
) (client.CopyFromContainerResult, error) {
	buffer := &bytes.Buffer{}
	writer := tar.NewWriter(buffer)
	if err := writer.WriteHeader(&tar.Header{Name: "Corefile", Mode: 0o600, Size: int64(len(engine.artifact))}); err != nil {
		return client.CopyFromContainerResult{}, err
	}
	if _, err := writer.Write(engine.artifact); err != nil {
		return client.CopyFromContainerResult{}, err
	}
	if err := writer.Close(); err != nil {
		return client.CopyFromContainerResult{}, err
	}
	return client.CopyFromContainerResult{Content: io.NopCloser(bytes.NewReader(buffer.Bytes()))}, nil
}

func (engine imageIdentityEngine) ImageInspect(
	context.Context,
	string,
	...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	return client.ImageInspectResult{InspectResponse: imagetypes.InspectResponse{
		ID: engine.inspectedImageID, RepoDigests: []string{engine.imageReference},
		Descriptor: engine.descriptor, Os: engine.platform.OS, Architecture: engine.platform.Architecture, Variant: engine.platform.Variant,
	}}, nil
}

func TestDockerRuntimeInspectorContainerdSelectedChildIdentity(t *testing.T) {
	config := sha256.Sum256([]byte("expected image config"))
	child := digest.FromString("selected child")
	reference := "example/resolver@" + child.String()
	request := Request{
		ProjectName: "groundplane-infra", ServiceName: "coredns", ArtifactTarget: "/etc/coredns/Corefile",
		ImageReference: reference, ImageConfigDigest: config, ExpectedLabels: map[string]string{"owned": "true"},
		ImageOS: "linux", ImageArchitecture: "amd64",
	}
	for _, name := range []string{"selected-child", "container-id", "image-id", "container-descriptor", "image-descriptor", "index", "wrong-child", "descriptor-platform", "os", "architecture", "variant"} {
		t.Run(name, func(t *testing.T) {
			engine := imageIdentityEngine{
				containerImageID: child.String(), inspectedImageID: child.String(), imageReference: reference,
				artifact: []byte("config\n"), platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
				descriptor: &ocispec.Descriptor{
					Digest:    child,
					MediaType: ocispec.MediaTypeImageManifest,
					Size:      1917,
				},
				containerDescriptor: &ocispec.Descriptor{
					Digest:    child,
					MediaType: ocispec.MediaTypeImageManifest,
					Size:      1917,
				},
			}
			switch name {
			case "container-id":
				engine.containerImageID = "sha256:" + hex.EncodeToString(config[:])
			case "image-id":
				engine.inspectedImageID = "sha256:" + hex.EncodeToString(config[:])
			case "container-descriptor":
				engine.containerDescriptor = nil
			case "image-descriptor":
				engine.descriptor = nil
			case "index":
				engine.descriptor.MediaType = ocispec.MediaTypeImageIndex
			case "wrong-child":
				engine.containerDescriptor.Digest = digest.FromString("other child")
			case "descriptor-platform":
				engine.containerDescriptor.Platform = &ocispec.Platform{OS: "linux", Architecture: "arm64"}
			case "os":
				engine.platform.OS = "windows"
			case "architecture":
				engine.platform.Architecture = "arm64"
			case "variant":
				engine.platform.Variant = "v8"
			}
			evidence, err := (&dockerRuntimeInspector{engine: engine}).Inspect(context.Background(), request)
			if name != "selected-child" {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("unproven runtime identity accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("selected containerd child rejected: %v", err)
			}
			if evidence.imageConfigAuthority != config ||
				"sha256:"+hex.EncodeToString(evidence.verifiedImageDigest[:]) != child.String() {
				t.Fatalf("child and bound config authority were confused: %#v", evidence)
			}
		})
	}
}

func (imageIdentityEngine) Close() error { return nil }

// Rationale: classic Docker supplies config identity from the actual
// running container and cross-check the image inspection before returning evidence.
func TestDockerRuntimeInspectorBindsActualImageConfigIdentity(t *testing.T) {
	expected := sha256.Sum256([]byte("expected image config"))
	other := sha256.Sum256([]byte("different image config"))
	expectedID := "sha256:" + hex.EncodeToString(expected[:])
	otherID := "sha256:" + hex.EncodeToString(other[:])
	child := sha256.Sum256([]byte("selected child"))
	reference := "example/resolver@sha256:" + hex.EncodeToString(child[:])
	request := Request{
		ProjectName: "groundplane-infra", ServiceName: "coredns", ArtifactTarget: "/etc/coredns/Corefile",
		ImageReference: reference, ImageConfigDigest: expected, ExpectedLabels: map[string]string{"owned": "true"},
		ImageOS: "linux", ImageArchitecture: "amd64",
	}
	for name, engine := range map[string]imageIdentityEngine{
		"running container mismatch": {
			containerImageID: otherID, inspectedImageID: otherID, imageReference: reference, artifact: []byte("config\n"),
			platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		},
		"image inspection mismatch": {
			containerImageID: expectedID, inspectedImageID: otherID, imageReference: reference, artifact: []byte("config\n"),
			platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (&dockerRuntimeInspector{engine: engine}).Inspect(context.Background(), request); err == nil {
				t.Fatal("Inspect() accepted mismatched actual image config identity")
			}
		})
	}
	engine := imageIdentityEngine{
		containerImageID: expectedID, inspectedImageID: expectedID, imageReference: reference, artifact: []byte("config\n"),
		platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
	evidence, err := (&dockerRuntimeInspector{engine: engine}).Inspect(context.Background(), request)
	if err != nil || evidence.imageConfigAuthority != expected {
		t.Fatalf("Inspect() evidence/config = %#v / %v", evidence, err)
	}
}
