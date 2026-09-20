package scriptrunner

import (
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"strconv"
	"strings"
)

func createOptions(request scriptexecution.Request, bodyPath string) (client.ContainerCreateOptions, error) {
	projection := request.Projection
	platform, err := parsePlatform(projection.Platform)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	dns, err := parseDNS(projection.Dns)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	mounts, err := dockerMounts(projection.Mounts, bodyPath, request.Entries)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	resources, err := dockerResources(projection)
	if err != nil {
		return client.ContainerCreateOptions{}, err
	}
	networks := make(map[string]*network.EndpointSettings, 1)
	networkMode := container.NetworkMode("none")
	if len(projection.Networks) != 0 {
		primary := projection.Networks[0]
		name := primary.RenderedAttachment.DockerNetworkName
		networkMode = container.NetworkMode(name)
		networks[name] = scriptEndpointSettings(primary)
	}
	return client.ContainerCreateOptions{
		Name: request.Projection.Name, Platform: platform,
		Config: &container.Config{
			Image: projection.Image, User: strconv.FormatUint(uint64(projection.Uid), 10) + ":" + strconv.FormatUint(uint64(projection.Gid), 10),
			WorkingDir: projection.WorkingDir, Env: scriptEnvironment(projection.Environment, request.Entries), Labels: pairMap(projection.Labels),
			Entrypoint: append(
				[]string(nil),
				projection.Entrypoint...), Cmd: append([]string(nil), projection.Command...),
			AttachStdout: true, AttachStderr: true, StopSignal: "SIGTERM", StopTimeout: intPointer(stopSeconds),
		},
		HostConfig: &container.HostConfig{
			LogConfig: container.LogConfig{Type: "none"}, NetworkMode: networkMode,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
			DNS:           dns, DNSOptions: append([]string(nil), projection.DnsOpt...), DNSSearch: append([]string(nil), projection.DnsSearch...),
			ExtraHosts: append(
				[]string(nil),
				projection.ExtraHosts...), GroupAdd: append([]string(nil), projection.GroupAdd...),
			CapDrop: append([]string(nil), projection.CapDrop...), ReadonlyRootfs: projection.ReadOnly,
			SecurityOpt: append([]string(nil), projection.SecurityOpt...), StorageOpt: pairMap(projection.StorageOpt),
			Tmpfs: tmpfsMap(projection.Tmpfs), ShmSize: projection.ShmSize, Sysctls: pairMap(projection.Sysctls),
			Runtime: projection.Runtime, Isolation: container.Isolation(projection.Isolation), Resources: resources,
			Mounts: mounts, Init: projection.Init,
		},
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: networks},
	}, nil
}

func parsePlatform(value string) (*ocispec.Platform, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, "/")
	if (len(parts) != 2 && len(parts) != 3) || !validPlatformPart(parts[0]) || !validPlatformPart(parts[1]) {
		return nil, errs.New(errs.KindInternal, "Script runner: invalid sealed platform")
	}
	parsed := ocispec.Platform{OS: parts[0], Architecture: parts[1]}
	if len(parts) == 3 {
		if !validPlatformPart(parts[2]) {
			return nil, errs.New(errs.KindInternal, "Script runner: invalid sealed platform")
		}
		parsed.Variant = parts[2]
	}
	return &parsed, nil
}

func validPlatformPart(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func intPointer(value int) *int       { return &value }
func int64Pointer(value int64) *int64 { return &value }
func boolPointer(value bool) *bool    { return &value }
