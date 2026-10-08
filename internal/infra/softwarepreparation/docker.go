package softwarepreparation

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/registryconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	buildClientImage     = "docker:29.1.3-cli@sha256:4fa0ee1f3a7e4354c4ea34558b6d4ee32859baf4973d4c8ccc8e7fe3dd730c04"
	buildkitImage        = "moby/buildkit:buildx-stable-1"
	agentRepository      = "groundplane-agent"
	controllerRepository = "groundplane-controller-bundle"
)

type dockerOperation struct {
	preparer       *Preparer
	directory      string
	dockerConfig   string
	operationID    string
	builder        string
	builderCreated bool
	ownedTags      []string
}

type imageInspection struct {
	ID           string `json:"Id"`
	OS           string `json:"Os"`
	Architecture string `json:"Architecture"`
	Config       struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

func newDockerOperation(preparer *Preparer, directory, operationID string) (*dockerOperation, error) {
	configuration := filepath.Join(directory, "docker-config")
	if err := os.Mkdir(configuration, 0o700); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &dockerOperation{
		preparer: preparer, directory: directory, operationID: operationID,
		dockerConfig: configuration,
		builder:      preparationBuilder(operationID, directory),
	}, nil
}

func staleDockerOperation(
	preparer *Preparer,
	directory string,
	operationID string,
	source ResolvedSource,
) *dockerOperation {
	operation := &dockerOperation{
		preparer: preparer, directory: directory, dockerConfig: filepath.Join(directory, "docker-config"),
		operationID: operationID, builder: preparationBuilder(operationID, directory),
	}
	if source.Selection.includesAgent() {
		operation.ownedTags = append(operation.ownedTags,
			operation.localTag("agent", source),
			imagefetch.RegistryAuthority+"/"+agentRepository+":"+preparedTag(source, operationID, SelectionAgent),
		)
	}
	if source.Selection.includesController() {
		operation.ownedTags = append(
			operation.ownedTags,
			operation.localTag("controller", source),
			imagefetch.RegistryAuthority+"/"+controllerRepository+":"+preparedTag(
				source,
				operationID,
				SelectionController,
			),
		)
	}
	return operation
}

func preparationBuilder(operationID, directory string) string {
	identity := sha256Value([]byte(operationID + "\x00" + filepath.Base(directory)))
	return "groundplane-prepare-" + identity[len("sha256:"):len("sha256:")+16]
}

func (operation *dockerOperation) ensureBuilder(ctx context.Context) error {
	if operation.builderCreated {
		return nil
	}
	operation.builderCreated = true
	_, err := operation.run(ctx, commandTimeout,
		"buildx", "create", "--name", operation.builder, "--driver", "docker-container",
		"--driver-opt", "image="+buildkitImage,
		"--driver-opt", "restart-policy=no",
		"--driver-opt", "memory=4g",
		"--driver-opt", "memory-swap=4g",
		"--driver-opt", "cpu-quota=200000",
	)
	if err != nil {
		return err
	}
	return nil
}

func (operation *dockerOperation) buildAgent(
	ctx context.Context,
	sourceDirectory string,
	source ResolvedSource,
	version string,
) (string, error) {
	if err := operation.ensureBuilder(ctx); err != nil {
		return "", err
	}
	tag := operation.localTag("agent", source)
	operation.ownedTags = append(operation.ownedTags, tag)
	_, err := operation.runIn(ctx, buildTimeout, sourceDirectory,
		"buildx", "build", "--builder", operation.builder,
		"--platform", source.Platform.OS+"/"+source.Platform.Architecture,
		"--provenance=false", "--progress=plain", "--file", "Dockerfile.agent",
		"--build-arg", "AGENT_VERSION="+version,
		"--build-arg", "SOURCE_DATE_EPOCH="+strconv.FormatInt(source.CommitTime.Unix(), 10),
		"--label", "org.opencontainers.image.revision="+source.ResolvedSHA,
		"--tag", tag, "--output", "type=docker,rewrite-timestamp=true", ".",
	)
	if err != nil {
		return "", err
	}
	inspection, err := operation.inspect(ctx, tag, source.Platform, version)
	if err != nil {
		return "", err
	}
	if inspection.Config.Labels["org.opencontainers.image.revision"] != source.ResolvedSHA {
		return "", errs.New(errs.KindStateConflict, "prepared Agent image revision label is invalid")
	}
	return tag, nil
}

func (operation *dockerOperation) buildController(
	ctx context.Context,
	sourceDirectory string,
	source ResolvedSource,
	version string,
	catalog []byte,
) (string, controllerDigests, error) {
	if err := operation.ensureBuilder(ctx); err != nil {
		return "", controllerDigests{}, err
	}
	payload := filepath.Join(operation.directory, "controller-payload")
	if err := os.Mkdir(payload, 0o700); err != nil {
		return "", controllerDigests{}, errs.Wrap(errs.KindInternal, err)
	}
	_, err := operation.runIn(ctx, buildTimeout, sourceDirectory,
		"buildx", "build", "--builder", operation.builder,
		"--platform", source.Platform.OS+"/"+source.Platform.Architecture,
		"--provenance=false", "--progress=plain", "--file", "Dockerfile.build",
		"--build-arg", "VERSION="+version,
		"--build-arg", "POSTGRES16_RELEASE_BASE64="+base64.StdEncoding.EncodeToString(catalog),
		"--build-arg", "SOURCE_DATE_EPOCH="+strconv.FormatInt(source.CommitTime.Unix(), 10),
		"--output", "type=local,dest="+payload, ".",
	)
	if err != nil {
		return "", controllerDigests{}, err
	}
	digests, err := validateControllerPayload(payload, version)
	if err != nil {
		return "", controllerDigests{}, err
	}
	tag, err := operation.packageController(ctx, payload, source, version, digests)
	return tag, digests, err
}

func (operation *dockerOperation) packageController(
	ctx context.Context,
	payload string,
	source ResolvedSource,
	version string,
	digests controllerDigests,
) (string, error) {
	if err := operation.ensureBuilder(ctx); err != nil {
		return "", err
	}
	revision, commitTime := componentProvenance(source, SelectionController)
	dockerfile := []byte("FROM scratch\n" +
		"COPY controller /controller\n" +
		"COPY controller-release.json /controller-release.json\n" +
		"COPY groundplane /groundplane\n" +
		"LABEL org.opencontainers.image.source=\"" + githubSourceURL + "\" \\\n" +
		"      org.opencontainers.image.revision=\"" + revision + "\" \\\n" +
		"      org.opencontainers.image.version=\"" + version + "\" \\\n" +
		"      org.groundplane.artifact=\"controller-bundle\" \\\n" +
		"      org.groundplane.controller.sha256=\"" + digests.binary + "\"\n")
	if err := os.WriteFile(filepath.Join(payload, "Dockerfile.groundplane-bundle"), dockerfile, 0o600); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	tag := operation.localTag("controller", source)
	operation.ownedTags = append(operation.ownedTags, tag)
	_, err := operation.runIn(ctx, buildTimeout, payload,
		"buildx", "build", "--builder", operation.builder,
		"--platform", source.Platform.OS+"/"+source.Platform.Architecture,
		"--network=none", "--provenance=false", "--progress=plain",
		"--file", "Dockerfile.groundplane-bundle",
		"--build-arg", "SOURCE_DATE_EPOCH="+strconv.FormatInt(commitTime.Unix(), 10),
		"--tag", tag, "--output", "type=docker,rewrite-timestamp=true", ".",
	)
	if err != nil {
		return "", err
	}
	inspection, err := operation.inspect(ctx, tag, source.Platform, version)
	if err != nil {
		return "", err
	}
	if inspection.Config.Labels["org.groundplane.artifact"] != "controller-bundle" ||
		inspection.Config.Labels["org.groundplane.controller.sha256"] != digests.binary ||
		inspection.Config.Labels["org.opencontainers.image.revision"] != revision {
		return "", errs.New(errs.KindStateConflict, "controller OCI bundle labels are invalid")
	}
	return tag, nil
}

func (operation *dockerOperation) pullAgent(
	ctx context.Context,
	reference string,
	source ResolvedSource,
	version string,
) (string, error) {
	if !strings.HasPrefix(reference, "ghcr.io/aland20/groundplane-agent@sha256:") ||
		!digestPattern.MatchString(reference[strings.LastIndexByte(reference, '@')+1:]) {
		return "", errs.New(errs.KindStateConflict, "published Agent image reference is invalid")
	}
	if _, err := operation.run(ctx, buildTimeout, "image", "pull", "--platform",
		source.Platform.OS+"/"+source.Platform.Architecture, reference); err != nil {
		return "", err
	}
	if _, err := operation.inspect(ctx, reference, source.Platform, version); err != nil {
		return "", err
	}
	tag := operation.localTag("agent", source)
	operation.ownedTags = append(operation.ownedTags, tag)
	if _, err := operation.run(ctx, commandTimeout, "image", "tag", reference, tag); err != nil {
		return "", err
	}
	return tag, nil
}

func (operation *dockerOperation) publish(
	ctx context.Context,
	localTag string,
	repository string,
	source ResolvedSource,
	component Selection,
) (Artifact, error) {
	tag := preparedTag(source, operation.operationID, component)
	requested := imagefetch.RegistryAuthority + "/" + repository + ":" + tag
	operation.ownedTags = append(operation.ownedTags, requested)
	if _, err := operation.run(ctx, commandTimeout, "image", "tag", localTag, requested); err != nil {
		return Artifact{}, err
	}
	if _, err := operation.run(ctx, buildTimeout, "--config",
		filepath.Join(registryconfiguration.Root, "client"), "image", "push", requested); err != nil {
		return Artifact{}, err
	}
	plan, err := (registryimages.Local{}).Resolve(ctx, requested)
	if err != nil {
		return Artifact{}, err
	}
	inspection, err := operation.inspect(ctx, localTag, source.Platform, "")
	if err != nil {
		return Artifact{}, err
	}
	if plan.ConfigDigest != inspection.ID || plan.Architecture != source.Platform.Architecture {
		return Artifact{}, errs.New(errs.KindStateConflict, "managed registry readback differs from prepared image")
	}
	return Artifact{
		Reference: plan.Reference(), ManifestDigest: plan.ManifestDigest, ConfigDigest: plan.ConfigDigest,
		Platform: source.Platform,
	}, nil
}

func (operation *dockerOperation) inspect(
	ctx context.Context,
	reference string,
	platform Platform,
	expectedVersion string,
) (imageInspection, error) {
	result, err := operation.run(ctx, commandTimeout, "image", "inspect", "--format", "{{json .}}", reference)
	if err != nil {
		return imageInspection{}, err
	}
	var inspection imageInspection
	if json.Unmarshal(result.Stdout, &inspection) != nil || !digestPattern.MatchString(inspection.ID) ||
		inspection.OS != platform.OS || inspection.Architecture != platform.Architecture {
		return imageInspection{}, errs.New(errs.KindStateConflict, "prepared image identity or platform is invalid")
	}
	if expectedVersion != "" && inspection.Config.Labels["org.opencontainers.image.version"] != expectedVersion {
		return imageInspection{}, errs.New(errs.KindStateConflict, "prepared image version label is invalid")
	}
	return inspection, nil
}

func (operation *dockerOperation) localTag(kind string, source ResolvedSource) string {
	component := SelectionAgent
	if kind == "controller" {
		component = SelectionController
	}
	revision, _ := componentProvenance(source, component)
	return fmt.Sprintf("groundplane-prepare-%s:%s-%s", kind, revision[:16], filepath.Base(operation.directory))
}

func preparedTag(source ResolvedSource, operationID string, component Selection) string {
	revision, _ := componentProvenance(source, component)
	return operationID + "-" + revision[:16] + "-linux-" + source.Platform.Architecture
}

func componentProvenance(source ResolvedSource, component Selection) (string, time.Time) {
	if source.SourceKind == SourceRef {
		return source.ResolvedSHA, source.CommitTime
	}
	if component == SelectionController {
		return source.ControllerRelease.ResolvedSHA, source.ControllerRelease.CommitTime
	}
	return source.AgentRelease.ResolvedSHA, source.AgentRelease.CommitTime
}

func (operation *dockerOperation) cleanup(ctx context.Context) error {
	if err := operation.removeBuildClient(ctx); err != nil {
		return err
	}
	var cleanupErr error
	for _, tag := range operation.ownedTags {
		result, err := operation.run(ctx, cleanupTimeout, "image", "ls", "--quiet", "--no-trunc", tag)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		if len(bytes.TrimSpace(result.Stdout)) == 0 {
			continue
		}
		if _, err := operation.run(ctx, cleanupTimeout, "image", "rm", tag); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	builderExists, err := operation.builderExists(ctx)
	if err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	} else if builderExists {
		_, err := operation.run(ctx, cleanupTimeout, "buildx", "rm", "--force", operation.builder)
		cleanupErr = errors.Join(cleanupErr, err)
	}
	return cleanupErr
}

func (operation *dockerOperation) removeBuildClient(ctx context.Context) error {
	result, err := operation.run(ctx, cleanupTimeout, "container", "ls", "--all", "--no-trunc",
		"--filter", "name=^/"+operation.builder+"-client$",
		"--filter", "label=com.groundplane.software-operation="+operation.operationID, "--format", "{{.ID}}")
	if err != nil {
		return err
	}
	identity := strings.TrimSpace(string(result.Stdout))
	if identity == "" {
		return nil
	}
	if !digestPattern.MatchString("sha256:" + identity) {
		return errs.New(errs.KindStateConflict, "software build client ownership is invalid")
	}
	_, err = operation.run(ctx, cleanupTimeout, "container", "rm", "--force", identity)
	return err
}

func (operation *dockerOperation) builderExists(ctx context.Context) (bool, error) {
	result, err := operation.run(ctx, cleanupTimeout, "buildx", "ls", "--format", "{{.Name}}")
	if err != nil {
		return false, err
	}
	for _, name := range strings.Split(string(result.Stdout), "\n") {
		if strings.TrimSpace(name) == operation.builder {
			return true, nil
		}
	}
	return false, nil
}

func (operation *dockerOperation) run(
	ctx context.Context,
	timeout time.Duration,
	args ...string,
) (commandrunner.Result, error) {
	return operation.runIn(ctx, timeout, "", args...)
}

func (operation *dockerOperation) runIn(
	ctx context.Context,
	timeout time.Duration,
	directory string,
	args ...string,
) (commandrunner.Result, error) {
	if len(args) > 0 && args[0] == "buildx" {
		workingDirectory := directory
		if workingDirectory == "" {
			workingDirectory = operation.directory
		}
		client := []string{"run", "--rm", "--name", operation.builder + "-client", "--network", "host",
			"--label", "com.groundplane.software-operation=" + operation.operationID,
			"--mount", "type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock",
			"--mount", "type=bind,source=" + operation.directory + ",target=" + operation.directory,
			"--workdir", workingDirectory, "--env", "DOCKER_HOST=unix:///var/run/docker.sock",
			"--env", "DOCKER_CONFIG=" + operation.dockerConfig,
			"--env", "BUILDX_CONFIG=" + filepath.Join(operation.dockerConfig, "buildx"),
			"--entrypoint", "docker", buildClientImage}
		args = append(client, args...)
	}
	result, err := operation.preparer.commands.Run(ctx, commandrunner.RunCmdOpts{
		Name: "docker", Args: args, Dir: directory,
		Env: []string{
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			"HOME=/root", "LANG=C", "LC_ALL=C", "DOCKER_HOST=unix:///var/run/docker.sock",
			"DOCKER_CONFIG=" + operation.dockerConfig,
		},
		ReplaceEnv: true, Timeout: timeout, CaptureLimitBytes: maximumOutput,
	})
	if err != nil {
		return result, errs.Wrap(errs.KindInternal, err)
	}
	if result.ExitCode != 0 {
		return result, errs.Newf(
			errs.KindInternal,
			"software preparation Docker command exited with status %d",
			result.ExitCode,
		)
	}
	return result, nil
}
