package dnsresolverobserver

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type dockerEngine interface {
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	ContainerLogs(context.Context, string, client.ContainerLogsOptions) (client.ContainerLogsResult, error)
	CopyFromContainer(context.Context, string, client.CopyFromContainerOptions) (client.CopyFromContainerResult, error)
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
	Close() error
}

type dockerRuntimeInspector struct{ engine dockerEngine }

func (inspector *dockerRuntimeInspector) Close() error { return inspector.engine.Close() }

func (inspector *dockerRuntimeInspector) Inspect(ctx context.Context, request Request) (runtimeEvidence, error) {
	filters := client.Filters{}.
		Add("label", "com.docker.compose.project="+request.ProjectName).
		Add("label", "com.docker.compose.service="+request.ServiceName)
	listed, err := inspector.engine.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return runtimeEvidence{}, errs.Wrap(errs.KindInternal, err)
	}
	if len(listed.Items) != 1 {
		return runtimeEvidence{}, errs.New(errs.KindStateConflict, "DNS resolver runtime container is not unique")
	}
	inspected, err := inspector.engine.ContainerInspect(ctx, listed.Items[0].ID, client.ContainerInspectOptions{})
	if err != nil {
		return runtimeEvidence{}, errs.Wrap(errs.KindInternal, err)
	}
	container := inspected.Container
	expectedImageID := "sha256:" + hex.EncodeToString(request.ImageConfigDigest[:])
	if container.Config == nil || container.State == nil || !container.State.Running ||
		container.Config.Image != request.ImageReference || container.Image != expectedImageID {
		return runtimeEvidence{}, errs.New(errs.KindStateConflict, "DNS resolver runtime identity is invalid")
	}
	for key, value := range request.ExpectedLabels {
		if container.Config.Labels[key] != value {
			return runtimeEvidence{}, errs.New(errs.KindStateConflict, "DNS resolver runtime ownership changed")
		}
	}
	mounted := false
	for _, candidate := range container.Mounts {
		if artifactDirectoryMountOwnsTarget(candidate.Destination, candidate.RW, request.ArtifactTarget) {
			mounted = true
			break
		}
	}
	if !mounted {
		return runtimeEvidence{}, errs.New(errs.KindStateConflict, "DNS resolver mounted artifact ownership changed")
	}
	copyResult, err := inspector.engine.CopyFromContainer(
		ctx,
		container.ID,
		client.CopyFromContainerOptions{SourcePath: request.ArtifactTarget},
	)
	if err != nil {
		return runtimeEvidence{}, errs.Wrap(errs.KindInternal, err)
	}
	artifact, err := readSingleTarFile(copyResult.Content)
	if err != nil {
		return runtimeEvidence{}, err
	}
	logStream, err := inspector.engine.ContainerLogs(ctx, container.ID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       strconv.Itoa(maximumLogLines),
	})
	if err != nil {
		clear(artifact)
		return runtimeEvidence{}, errs.Wrap(errs.KindInternal, err)
	}
	logs, err := readBoundedContainerLogs(logStream, container.Config.Tty)
	if err != nil {
		clear(artifact)
		return runtimeEvidence{}, err
	}
	image, err := inspector.engine.ImageInspect(ctx, container.Image)
	if err != nil {
		clear(artifact)
		clear(logs)
		return runtimeEvidence{}, errs.Wrap(errs.KindInternal, err)
	}
	digest, err := verifiedImageDigest(image, request.ImageReference)
	if err != nil {
		clear(artifact)
		clear(logs)
		return runtimeEvidence{}, err
	}
	actualConfigDigest, err := verifiedImageConfigDigest(container.Image, image.ID, request.ImageConfigDigest)
	if err != nil {
		clear(artifact)
		clear(logs)
		return runtimeEvidence{}, err
	}
	return runtimeEvidence{
		artifact: artifact, logs: logs, verifiedImageDigest: digest,
		verifiedImageConfigDigest: actualConfigDigest,
	}, nil
}

func verifiedImageConfigDigest(
	containerImageID string,
	inspectedImageID string,
	expected [sha256.Size]byte,
) ([sha256.Size]byte, error) {
	if containerImageID != "sha256:"+hex.EncodeToString(expected[:]) || inspectedImageID != containerImageID {
		return [sha256.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver image config digest changed")
	}
	const prefix = "sha256:"
	decoded, err := hex.DecodeString(strings.TrimPrefix(containerImageID, prefix))
	if err != nil || !strings.HasPrefix(containerImageID, prefix) || len(decoded) != sha256.Size ||
		containerImageID != prefix+hex.EncodeToString(decoded) {
		return [sha256.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver image config digest is invalid")
	}
	var result [sha256.Size]byte
	copy(result[:], decoded)
	return result, nil
}

func readBoundedContainerLogs(content io.ReadCloser, tty bool) ([]byte, error) {
	if content == nil {
		return nil, errs.New(errs.KindInternal, "DNS resolver log stream is missing")
	}
	defer content.Close()
	limited := &io.LimitedReader{R: content, N: maximumLogBytes + 1}
	buffer := &bytes.Buffer{}
	var err error
	if tty {
		_, err = io.Copy(buffer, limited)
	} else {
		_, err = stdcopy.StdCopy(buffer, buffer, limited)
	}
	if err != nil || limited.N <= 0 || buffer.Len() == 0 || buffer.Len() > maximumLogBytes {
		clear(buffer.Bytes())
		return nil, errs.New(errs.KindStateConflict, "DNS resolver runtime logs are invalid")
	}
	return buffer.Bytes(), nil
}

func artifactDirectoryMountOwnsTarget(destination string, readWrite bool, target string) bool {
	base := path.Base(target)
	return !readWrite && base != "." && base != "/" && base != "" &&
		destination == path.Dir(target)
}

func readSingleTarFile(content io.ReadCloser) ([]byte, error) {
	if content == nil {
		return nil, errs.New(errs.KindInternal, "DNS resolver artifact stream is missing")
	}
	defer content.Close()
	reader := tar.NewReader(content)
	header, err := reader.Next()
	if err != nil || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > maximumArtifactBytes {
		return nil, errs.New(errs.KindStateConflict, "DNS resolver artifact archive is invalid")
	}
	value, err := io.ReadAll(io.LimitReader(reader, maximumArtifactBytes+1))
	if err != nil || int64(len(value)) != header.Size {
		clear(value)
		return nil, errs.New(errs.KindStateConflict, "DNS resolver artifact archive changed")
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		clear(value)
		return nil, errs.New(errs.KindStateConflict, "DNS resolver artifact archive contains extra entries")
	}
	return value, nil
}

func verifiedImageDigest(image client.ImageInspectResult, reference string) ([sha256.Size]byte, error) {
	separator := strings.LastIndex(reference, "@sha256:")
	expected, err := hex.DecodeString(reference[separator+len("@sha256:"):])
	if err != nil || len(expected) != sha256.Size {
		return [sha256.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver image reference is invalid")
	}
	found := false
	for _, digest := range image.RepoDigests {
		found = found || strings.HasSuffix(digest, "@sha256:"+hex.EncodeToString(expected))
	}
	if !found && image.Descriptor != nil {
		found = image.Descriptor.Digest.String() == "sha256:"+hex.EncodeToString(expected)
	}
	if !found {
		return [sha256.Size]byte{}, errs.New(errs.KindStateConflict, "DNS resolver image digest changed")
	}
	var result [sha256.Size]byte
	copy(result[:], expected)
	return result, nil
}
