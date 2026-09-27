package runnerproxy

import (
	"net/url"
	"runtime"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
	"github.com/moby/moby/api/types/build"
	"github.com/moby/moby/client"
)

const (
	runnerOwnerLabel    = "groundplane.runner.id"
	runnerEpochLabel    = "groundplane.runtime.epoch"
	buildInputLabel     = "groundplane.runner.build-context"
	buildOperationLabel = "groundplane.runner.operation"
)

// BuildPolicy fixes ownership at proxy startup. A workflow cannot choose a
// different owner or runtime epoch in build arguments, tags or labels.
type BuildPolicy struct {
	runnerID string
	epoch    uint64
}

func NewBuildPolicy(runnerID string, epoch uint64) (*BuildPolicy, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || epoch == 0 {
		return nil, buildRequestDenied()
	}
	return &BuildPolicy{runnerID: runnerID, epoch: epoch}, nil
}

// Prepare creates the daemon input for a Controller-authorized operation. The
// operation label lets post-build inspection bind the image to that operation,
// rather than treating a mutable tag or a streamed success line as authority.
func (policy *BuildPolicy) Prepare(
	operationID string, rawQuery string, prepared *BuildContext,
) (client.ImageBuildOptions, error) {
	if policy == nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return client.ImageBuildOptions{}, buildRequestDenied()
	}
	options, err := prepareBuildOptions(rawQuery, prepared, policy.runnerID, policy.epoch)
	if err != nil {
		return client.ImageBuildOptions{}, err
	}
	options.Labels[buildOperationLabel] = operationID
	return options, nil
}

// prepareBuildOptions constructs a fresh daemon request from a closed set of
// workflow inputs. Unknown query keys are rejected rather than forwarded.
func prepareBuildOptions(
	rawQuery string,
	prepared *BuildContext,
	runnerID string,
	epoch uint64,
) (client.ImageBuildOptions, error) {
	if prepared == nil || ids.Validate(ids.KindRunner, runnerID) != nil || epoch == 0 || len(rawQuery) > 64<<10 {
		return client.ImageBuildOptions{}, buildRequestDenied()
	}
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return client.ImageBuildOptions{}, buildRequestDenied()
	}
	options := client.ImageBuildOptions{
		Version: build.BuilderV1, Remove: true, ForceRemove: true,
		Dockerfile: "Dockerfile", NetworkMode: "default",
		Labels: make(map[string]string),
	}
	for key, values := range query {
		if key != "t" && len(values) != 1 {
			return client.ImageBuildOptions{}, buildRequestDenied()
		}
		switch key {
		case "t":
			options.Tags, err = buildTags(values)
		case "dockerfile":
			options.Dockerfile, err = contextPath(values[0])
		case "target":
			if !buildTarget(values[0]) {
				return client.ImageBuildOptions{}, buildRequestDenied()
			}
			options.Target = values[0]
		case "buildargs":
			var arguments map[string]string
			arguments, err = buildStringMap(values[0])
			options.BuildArgs = make(map[string]*string, len(arguments))
			for name, value := range arguments {
				options.BuildArgs[name] = &value
			}
		case "labels":
			options.Labels, err = buildStringMap(values[0])
			for label := range options.Labels {
				if strings.HasPrefix(strings.ToLower(label), "groundplane.") {
					return client.ImageBuildOptions{}, buildRequestDenied()
				}
			}
		case "nocache":
			options.NoCache, err = buildBoolean(values[0])
		case "pull":
			options.PullParent, err = buildBoolean(values[0])
		case "q":
			options.SuppressOutput, err = buildBoolean(values[0])
		case "rm", "forcerm":
			var remove bool
			remove, err = buildBoolean(values[0])
			if !remove {
				return client.ImageBuildOptions{}, buildRequestDenied()
			}
		case "networkmode":
			if values[0] != "default" && values[0] != "none" {
				return client.ImageBuildOptions{}, buildRequestDenied()
			}
			options.NetworkMode = values[0]
		case "version":
			if values[0] != string(build.BuilderV1) {
				return client.ImageBuildOptions{}, buildRequestDenied()
			}
		case "platform":
			if values[0] != "linux/"+runtime.GOARCH {
				return client.ImageBuildOptions{}, buildRequestDenied()
			}
		default:
			// Includes remote contexts, BuildKit sessions, arbitrary cgroups,
			// security options, output paths and caller-selected host aliases.
			return client.ImageBuildOptions{}, buildRequestDenied()
		}
		if err != nil {
			return client.ImageBuildOptions{}, buildRequestDenied()
		}
	}
	if !prepared.HasDockerfile(options.Dockerfile) {
		return client.ImageBuildOptions{}, buildRequestDenied()
	}
	options.Labels[runnerOwnerLabel] = runnerID
	options.Labels[runnerEpochLabel] = strconv.FormatUint(epoch, 10)
	options.Labels[buildInputLabel] = prepared.Digest()
	return options, nil
}

func buildTags(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > 16 {
		return nil, buildRequestDenied()
	}
	tags := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if len(value) > 256 {
			return nil, buildRequestDenied()
		}
		named, err := reference.ParseNormalizedNamed(value)
		if err != nil {
			return nil, buildRequestDenied()
		}
		if _, digest := named.(reference.Digested); digest {
			return nil, buildRequestDenied()
		}
		name := reference.TagNameOnly(named).String()
		if seen[name] {
			return nil, buildRequestDenied()
		}
		seen[name] = true
		tags = append(tags, name)
	}
	return tags, nil
}

func buildStringMap(value string) (map[string]string, error) {
	if len(value) == 0 || len(value) > 32<<10 {
		return nil, buildRequestDenied()
	}
	canonical, err := jcs.Canonicalize([]byte(value))
	if err != nil {
		return nil, buildRequestDenied()
	}
	defer clear(canonical)
	values, err := jcs.Decode[map[string]string](canonical)
	if err != nil || values == nil || len(values) > 128 {
		return nil, buildRequestDenied()
	}
	for key, value := range values {
		if key == "" || len(key) > 128 || len(value) > 8192 || strings.ContainsAny(key+value, "\x00\r\n") {
			return nil, buildRequestDenied()
		}
	}
	return values, nil
}

func buildTarget(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '_' && character != '-' && character != '.' {
			return false
		}
	}
	return true
}

func buildBoolean(value string) (bool, error) {
	switch value {
	case "1", "true":
		return true, nil
	case "0", "false":
		return false, nil
	default:
		return false, buildRequestDenied()
	}
}

func buildRequestDenied() error {
	return errs.New(errs.KindScopeUnauthorized, "Runner Docker build request is outside the supported policy")
}
