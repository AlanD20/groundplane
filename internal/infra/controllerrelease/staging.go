package controllerrelease

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/jcs"
)

// Stage verifies a prepared native payload and publishes its immutable release
// under the same private store boundary used by activation and recovery. It
// selects a candidate only; neither installed executable is changed here.
func (store *Store) Stage(
	ctx context.Context,
	metadata []byte,
	binary io.Reader,
	agentImage string,
) (upgrade.Release, error) {
	if len(metadata) == 0 || len(metadata) > upgrade.MaxManifestBytes || binary == nil {
		return upgrade.Release{}, unsafeFile()
	}
	canonical, err := jcs.Canonicalize(metadata)
	if err != nil {
		return upgrade.Release{}, err
	}
	build, err := jcs.Decode[struct {
		Schema            int            `json:"schema"`
		ControllerSHA256  upgrade.Digest `json:"controller_sha256"`
		ControllerVersion string         `json:"controller_version"`
		StorageEpoch      int            `json:"storage_epoch"`
		ChannelSchema     int            `json:"channel_schema"`
	}](canonical)
	if err != nil {
		return upgrade.Release{}, err
	}
	manifest := upgrade.Manifest{Schema: build.Schema, ControllerSHA256: build.ControllerSHA256,
		ControllerVersion: build.ControllerVersion, StorageEpoch: build.StorageEpoch,
		ChannelSchema: build.ChannelSchema, AgentImage: agentImage}
	if err := manifest.Validate(); err != nil {
		return upgrade.Release{}, err
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return upgrade.Release{}, fileError(err)
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return upgrade.Release{}, err
	}
	release := upgrade.Release{Release: upgrade.Hash(raw), Manifest: manifest}
	unlock, err := store.lock(ctx)
	if err != nil {
		return upgrade.Release{}, err
	}
	defer unlock()
	if err := store.publishStage(ctx, release, raw, binary); err != nil {
		return upgrade.Release{}, err
	}
	pointer, err := json.Marshal(struct {
		Release upgrade.Digest `json:"release"`
	}{release.Release})
	if err != nil {
		return upgrade.Release{}, fileError(err)
	}
	pointer, err = jcs.Canonicalize(pointer)
	if err != nil {
		return upgrade.Release{}, err
	}
	err = store.atomicWrite(ctx, store.root, "candidate.json", 0o400, func(output io.Writer) error {
		_, err := output.Write(pointer)
		return fileError(err)
	})
	return release, err
}

func (store *Store) publishStage(ctx context.Context, release upgrade.Release, raw []byte, binary io.Reader) error {
	releases, err := openDirectory(ctx, store.root, "releases")
	if err != nil {
		return err
	}
	defer releases.Close()
	if err := privateDirectory(ctx, releases, store.uid); err != nil {
		return err
	}
	leaf := string(release.Release)[7:]
	if _, err := releases.Lstat(leaf); err == nil {
		stored, err := store.Inspect(ctx, release.Release)
		if err != nil || stored != release.Manifest {
			return unsafeFile()
		}
		actual, err := copyDigest(ctx, io.Discard, binary)
		if err != nil || actual != release.Manifest.ControllerSHA256 {
			return unsafeFile()
		}
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fileError(err)
	}
	temporary := ".staging-" + leaf
	if _, err := releases.Lstat(temporary); err == nil {
		stale, err := openDirectory(ctx, releases, temporary)
		if err != nil {
			return err
		}
		validation := privateDirectory(ctx, stale, store.uid)
		closeErr := stale.Close()
		if validation != nil || closeErr != nil {
			return unsafeFile()
		}
		// The release digest binds this private staging directory to this exact
		// operation; an interrupted unpublished transfer can be rebuilt safely.
		if err := releases.RemoveAll(temporary); err != nil {
			return fileError(err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fileError(err)
	}
	if err := releases.Mkdir(temporary, 0o700); err != nil {
		return fileError(err)
	}
	staging, err := openDirectory(ctx, releases, temporary)
	if err != nil {
		return err
	}
	defer staging.Close()
	published := false
	defer func() {
		if !published {
			// This generated private directory contains only our two closed files.
			_ = staging.Remove("controller")
			_ = staging.Remove("manifest.json")
			_ = releases.Remove(temporary)
		}
	}()
	if err := store.atomicWrite(ctx, staging, "manifest.json", 0o400, func(output io.Writer) error {
		_, err := output.Write(raw)
		return fileError(err)
	}); err != nil {
		return err
	}
	if err := store.atomicWrite(ctx, staging, "controller", 0o500, func(output io.Writer) error {
		actual, err := copyDigest(ctx, output, binary)
		if err != nil {
			return err
		}
		if actual != release.Manifest.ControllerSHA256 {
			return unsafeFile()
		}
		return nil
	}); err != nil {
		return err
	}
	if err := releases.Rename(temporary, leaf); err != nil {
		return fileError(err)
	}
	published = true
	return syncDirectory(releases)
}
