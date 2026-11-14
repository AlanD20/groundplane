// Package agentimagemetadata verifies immutable Agent image metadata without
// starting its executable, attaching host mounts or granting network access.
package agentimagemetadata

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

func Read(ctx context.Context, image, owner string) (_ agentprotocol.ImageMetadata, resultErr error) {
	if ctx == nil || !imageref.IsDigestPinned(image) || validOwner(owner) != nil {
		return agentprotocol.ImageMetadata{}, invalid()
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return agentprotocol.ImageMetadata{}, errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	imageInfo, err := engine.ImageInspect(bounded, image)
	if err != nil {
		return agentprotocol.ImageMetadata{}, errs.Wrap(errs.KindInternal, err)
	}
	if imageInfo.Config == nil || len(imageInfo.Config.Volumes) != 0 {
		return agentprotocol.ImageMetadata{}, invalid()
	}
	if err := cleanup(bounded, engine, owner); err != nil {
		return agentprotocol.ImageMetadata{}, err
	}
	created, err := engine.ContainerCreate(bounded, client.ContainerCreateOptions{Name: name(owner),
		Config: &container.Config{Image: image, Entrypoint: []string{neverExecute},
			Labels: map[string]string{
				"com.groundplane.managed": "true",
				"com.groundplane.kind":    "software-inspection",
				ownerLabel:                owner,
			}},
		HostConfig: &container.HostConfig{NetworkMode: "none", ReadonlyRootfs: true, CapDrop: []string{"ALL"}},
	})
	if err != nil || created.ID == "" {
		if err != nil {
			return agentprotocol.ImageMetadata{}, errs.Wrap(errs.KindInternal, err)
		}
		return agentprotocol.ImageMetadata{}, invalid()
	}
	defer func() {
		cleanupCtx, done := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer done()
		resultErr = errors.Join(resultErr, cleanup(cleanupCtx, engine, owner))
	}()
	var raw []byte
	err = readPayload(bounded, engine, created.ID, agentprotocol.ImageMetadataPath, 4096,
		func(reader io.Reader) error {
			var readErr error
			raw, readErr = io.ReadAll(reader)
			return readErr
		})
	if err != nil {
		return agentprotocol.ImageMetadata{}, err
	}
	metadata, err := agentprotocol.ParseImageMetadata(raw)
	if err != nil {
		return agentprotocol.ImageMetadata{}, err
	}
	digest := sha256.New()
	err = readPayload(bounded, engine, created.ID, "/usr/local/bin/groundplane-agent", 256<<20,
		func(reader io.Reader) error { _, copyErr := io.Copy(digest, reader); return copyErr })
	if err != nil {
		return agentprotocol.ImageMetadata{}, err
	}
	if metadata.BinarySHA256 != fmt.Sprintf("sha256:%x", digest.Sum(nil)) {
		return agentprotocol.ImageMetadata{}, invalid()
	}
	inspected, err := engine.ContainerInspect(bounded, created.ID, client.ContainerInspectOptions{})
	if err != nil || inspected.Container.Config == nil {
		return agentprotocol.ImageMetadata{}, invalid()
	}
	labels := inspected.Container.Config.Labels
	if metadata.Source.Kind != labels["com.groundplane.source-kind"] ||
		metadata.Source.Commit != labels["org.opencontainers.image.revision"] ||
		metadata.Version != labels["org.opencontainers.image.version"] {
		return agentprotocol.ImageMetadata{}, invalid()
	}
	return metadata, nil
}

func readPayload(ctx context.Context, engine *client.Client, id, filename string, maximum int64,
	consume func(io.Reader) error,
) error {
	file, err := engine.CopyFromContainer(ctx, id, client.CopyFromContainerOptions{SourcePath: filename})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return invalid()
		}
		return errs.Wrap(errs.KindInternal, err)
	}
	defer file.Content.Close()
	archive := tar.NewReader(io.LimitReader(file.Content, maximum+(1<<20)))
	header, err := archive.Next()
	if err != nil || header.Typeflag != tar.TypeReg || header.Name != path.Base(filename) ||
		header.Size < 1 || header.Size > maximum {
		return invalid()
	}
	if err := consume(io.LimitReader(archive, maximum+1)); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if _, err := archive.Next(); err != io.EOF {
		return invalid()
	}
	return ctx.Err()
}

func invalid() error {
	return errs.New(
		errs.KindSoftwareAgentMetadataInvalid,
		"Agent image metadata or executable is missing or invalid; rebuild the Agent image before applying it",
	)
}
