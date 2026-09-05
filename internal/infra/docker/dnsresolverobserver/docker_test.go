package dnsresolverobserver

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	containertypes "github.com/moby/moby/api/types/container"
	imagetypes "github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
)

// Rationale: observation evidence must derive the config identity from the
// running container and reject either an expected/runtime or inspect/runtime mismatch.
func TestVerifiedImageConfigDigestRequiresActualRunningImage(t *testing.T) {
	expected := sha256.Sum256([]byte("expected image config"))
	actualID := "sha256:" + hex.EncodeToString(expected[:])
	if got, err := verifiedImageConfigDigest(actualID, actualID, expected); err != nil || got != expected {
		t.Fatalf("verifiedImageConfigDigest() = %x, %v", got, err)
	}
	other := sha256.Sum256([]byte("different image config"))
	if _, err := verifiedImageConfigDigest("sha256:"+hex.EncodeToString(other[:]), actualID, expected); err == nil {
		t.Fatal("verifiedImageConfigDigest() accepted unexpected running image")
	}
	if _, err := verifiedImageConfigDigest(actualID, "sha256:"+hex.EncodeToString(other[:]), expected); err == nil {
		t.Fatal("verifiedImageConfigDigest() accepted mismatched image inspection")
	}
}

type imageIdentityEngine struct {
	containerImageID string
	inspectedImageID string
	imageReference   string
	artifact         []byte
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
	}}, nil
}

func (imageIdentityEngine) Close() error { return nil }

// Rationale: the Docker consumer must take config identity from the actual
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
	}
	for name, engine := range map[string]imageIdentityEngine{
		"running container mismatch": {
			containerImageID: otherID, inspectedImageID: otherID, imageReference: reference, artifact: []byte("config\n"),
		},
		"image inspection mismatch": {
			containerImageID: expectedID, inspectedImageID: otherID, imageReference: reference, artifact: []byte("config\n"),
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
	}
	evidence, err := (&dockerRuntimeInspector{engine: engine}).Inspect(context.Background(), request)
	if err != nil || evidence.verifiedImageConfigDigest != expected {
		t.Fatalf("Inspect() evidence/config = %#v / %v", evidence, err)
	}
}
