package softwarepreparation

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerDigests struct {
	binary   string
	metadata string
	cli      string
}

func (preparer *Preparer) downloadSource(ctx context.Context, source ResolvedSource, destination string) error {
	endpoint := "https://codeload.github.com/" + githubRepository + "/tar.gz/" + source.ResolvedSHA
	archive := filepath.Join(destination, "source.tar.gz")
	if err := preparer.download(ctx, endpoint, archive, maximumSourceBytes, 0, ""); err != nil {
		return err
	}
	return extractSourceArchive(archive, filepath.Join(destination, "source"), source.ResolvedSHA)
}

func (preparer *Preparer) downloadAsset(
	ctx context.Context,
	tag string,
	asset ReleaseAsset,
	destination string,
) error {
	endpoint := githubSourceURL + "/releases/download/" + url.PathEscape(tag) + "/" + url.PathEscape(asset.Name)
	return preparer.download(ctx, endpoint, destination, asset.Size, asset.Size, asset.SHA256)
}

func (preparer *Preparer) download(
	ctx context.Context,
	endpoint string,
	destination string,
	maximum int64,
	expectedSize int64,
	expectedDigest string,
) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	request.Header.Set("Accept", "application/octet-stream")
	request.Header.Set("User-Agent", "groundplane-software-preparation")
	response, err := preparer.http.Do(request)
	if err != nil {
		return errs.Wrap(errs.KindRequestFailed, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Request.URL.Scheme != "https" ||
		!trustedGitHubHost(response.Request.URL.Hostname()) {
		return errs.New(errs.KindStateConflict, "software preparation download endpoint is invalid")
	}
	if response.ContentLength > maximum || expectedSize > 0 && response.ContentLength >= 0 &&
		response.ContentLength != expectedSize {
		return errs.New(errs.KindStateConflict, "software preparation download size differs from authority")
	}
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hasher), io.LimitReader(response.Body, maximum+1))
	syncErr := file.Sync()
	closeErr := file.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		return errs.WrapJoined(errs.KindInternal, copyErr, syncErr, closeErr)
	}
	if written <= 0 || written > maximum || expectedSize > 0 && written != expectedSize {
		return errs.New(errs.KindStateConflict, "software preparation download is empty or oversized")
	}
	actual := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if expectedDigest != "" && actual != expectedDigest {
		return errs.New(errs.KindStateConflict, "software preparation download digest differs from authority")
	}
	return nil
}

func extractSourceArchive(archivePath, destination, commit string) error {
	if err := os.Mkdir(destination, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer archive.Close()
	compressed, err := gzip.NewReader(archive)
	if err != nil {
		return errs.New(errs.KindStateConflict, "selected source archive is not valid gzip")
	}
	defer compressed.Close()
	prefix := "groundplane-" + commit
	reader := tar.NewReader(compressed)
	seen := make(map[string]struct{})
	var total int64
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errs.New(errs.KindStateConflict, "selected source archive is invalid")
		}
		name := strings.TrimSuffix(header.Name, "/")
		parts := strings.Split(name, "/")
		if len(parts) == 0 || parts[0] != prefix || name != filepath.ToSlash(filepath.Clean(name)) {
			return errs.New(errs.KindStateConflict, "selected source archive path is unsafe")
		}
		relative := strings.Join(parts[1:], "/")
		if relative == "" {
			continue
		}
		if _, duplicate := seen[relative]; duplicate {
			return errs.New(errs.KindStateConflict, "selected source archive repeats a path")
		}
		seen[relative] = struct{}{}
		target := filepath.Join(destination, filepath.FromSlash(relative))
		if !strings.HasPrefix(target, destination+string(filepath.Separator)) {
			return errs.New(errs.KindStateConflict, "selected source archive escaped its workspace")
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
		case tar.TypeReg:
			if header.Size < 0 || header.Size > 64<<20 {
				return errs.New(errs.KindStateConflict, "selected source archive member exceeds its bound")
			}
			total += header.Size
			if total > maximumUnpackedBytes {
				return errs.New(errs.KindStateConflict, "selected source archive exceeds its unpacked bound")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
			mode := os.FileMode(0o600)
			if header.Mode&0o111 != 0 {
				mode = 0o700
			}
			file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
			if err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
			written, copyErr := io.Copy(file, io.LimitReader(reader, header.Size+1))
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil || written != header.Size {
				return errs.WrapJoined(errs.KindInternal, copyErr, closeErr)
			}
		default:
			return errs.New(errs.KindStateConflict, "selected source archive contains an unsupported member")
		}
	}
	for _, required := range []string{"Dockerfile.build", "Dockerfile.agent", "VERSION", "Makefile"} {
		if info, err := os.Lstat(filepath.Join(destination, required)); err != nil || !info.Mode().IsRegular() {
			return errs.New(errs.KindStateConflict, "selected source predates the required container build contract")
		}
	}
	return nil
}

func extractControllerBundle(archivePath, destination, version string, platform Platform) (controllerDigests, error) {
	archive, err := os.Open(archivePath)
	if err != nil {
		return controllerDigests{}, errs.Wrap(errs.KindInternal, err)
	}
	defer archive.Close()
	compressed, err := gzip.NewReader(archive)
	if err != nil {
		return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release bundle is not valid gzip")
	}
	defer compressed.Close()
	reader := tar.NewReader(compressed)
	seen := make(map[string]string, 24)
	var manifestBytes []byte
	var total int64
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release bundle is invalid")
		}
		if header.Name == "" || len(header.Name) > 128 || !utf8.ValidString(header.Name) ||
			filepath.Base(header.Name) != header.Name || header.Name == "." || header.Typeflag != tar.TypeReg ||
			header.Size <= 0 || header.Size > maximumBundleMemberSize || len(seen) >= 64 {
			return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release bundle member is unsafe")
		}
		if _, duplicate := seen[header.Name]; duplicate {
			return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release bundle repeats a member")
		}
		total += header.Size
		if total > maximumUnpackedBytes {
			return controllerDigests{}, errs.New(
				errs.KindStateConflict,
				"controller release bundle exceeds its unpacked bound",
			)
		}
		hasher := sha256.New()
		var writer io.Writer = hasher
		var output *os.File
		if header.Name == "controller" || header.Name == "controller-release.json" || header.Name == "groundplane" {
			mode := os.FileMode(0o600)
			if header.Name != "controller-release.json" {
				mode = 0o700
			}
			output, err = os.OpenFile(filepath.Join(destination, header.Name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
			if err != nil {
				return controllerDigests{}, errs.Wrap(errs.KindInternal, err)
			}
			writer = io.MultiWriter(hasher, output)
		}
		if header.Name == "bundle.json" {
			buffer := bytes.NewBuffer(make([]byte, 0, header.Size))
			writer = io.MultiWriter(hasher, buffer)
			written, copyErr := io.Copy(writer, io.LimitReader(reader, header.Size+1))
			if copyErr != nil || written != header.Size {
				return controllerDigests{}, errs.New(
					errs.KindStateConflict,
					"controller release bundle member is truncated",
				)
			}
			manifestBytes = buffer.Bytes()
			seen[header.Name] = hex.EncodeToString(hasher.Sum(nil))
			continue
		}
		written, copyErr := io.Copy(writer, io.LimitReader(reader, header.Size+1))
		var closeErr error
		if output != nil {
			closeErr = output.Close()
		}
		if copyErr != nil || closeErr != nil || written != header.Size {
			return controllerDigests{}, errs.WrapJoined(errs.KindInternal, copyErr, closeErr)
		}
		seen[header.Name] = hex.EncodeToString(hasher.Sum(nil))
	}
	if manifestBytes == nil || seen["controller"] == "" || seen["controller-release.json"] == "" ||
		seen["groundplane"] == "" {
		return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release bundle is incomplete")
	}
	var manifest struct {
		Schema             int               `json:"schema"`
		Version            string            `json:"version"`
		OS                 string            `json:"os"`
		Arch               string            `json:"arch"`
		AgentImage         *string           `json:"agent_image"`
		RunnerImage        string            `json:"runner_image"`
		PostgresImage      string            `json:"postgres_image"`
		PostgresToolsImage string            `json:"postgres_tools_image"`
		Files              map[string]string `json:"files"`
	}
	if err := decodeJSON(manifestBytes, &manifest); err != nil || manifest.Schema != 1 ||
		manifest.Version != version || manifest.OS != platform.OS || manifest.Arch != platform.Architecture ||
		manifest.AgentImage != nil || !imageref.IsDigestPinned(manifest.RunnerImage) ||
		!imageref.IsDigestPinned(manifest.PostgresImage) || !imageref.IsDigestPinned(manifest.PostgresToolsImage) ||
		len(manifest.Files) != len(seen)-1 {
		return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release manifest is invalid")
	}
	for name, digest := range seen {
		if name == "bundle.json" {
			continue
		}
		if manifest.Files[name] != digest {
			return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release member digest differs")
		}
	}
	digests, err := validateControllerPayload(destination, "controller/v"+version)
	if err != nil {
		return controllerDigests{}, err
	}
	return digests, nil
}

func validateControllerPayload(directory, expectedVersion string) (controllerDigests, error) {
	binary, err := boundedFile(filepath.Join(directory, "controller"), 256<<20)
	if err != nil {
		return controllerDigests{}, err
	}
	metadata, err := boundedFile(filepath.Join(directory, "controller-release.json"), 4096)
	if err != nil {
		return controllerDigests{}, err
	}
	cli, err := boundedFile(filepath.Join(directory, "groundplane"), 256<<20)
	if err != nil {
		return controllerDigests{}, err
	}
	var release struct {
		Schema            int    `json:"schema"`
		ControllerSHA256  string `json:"controller_sha256"`
		ControllerVersion string `json:"controller_version"`
		StorageEpoch      int    `json:"storage_epoch"`
		ChannelSchema     int    `json:"channel_schema"`
	}
	if err := decodeJSON(metadata, &release); err != nil || release.Schema < 1 || release.StorageEpoch < 1 ||
		release.ChannelSchema < 1 || release.ControllerVersion != expectedVersion ||
		release.ControllerSHA256 != sha256Value(binary) {
		return controllerDigests{}, errs.New(errs.KindStateConflict, "controller release metadata is invalid")
	}
	return controllerDigests{
		binary: release.ControllerSHA256, metadata: sha256Value(metadata), cli: sha256Value(cli),
	}, nil
}

func boundedFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Size() <= 0 || info.Size() > maximum {
		return nil, errs.New(errs.KindStateConflict, "prepared Controller payload file is invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(value)) != info.Size() {
		return nil, errs.New(errs.KindStateConflict, "prepared Controller payload file is unreadable")
	}
	return value, nil
}

func decodeJSON(value []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("trailing JSON input")
	}
	return nil
}
