package services

import (
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func serviceAPIResponse(record servicerecord.ServiceRecord) apiTypes.Service {
	response := apiTypes.Service{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Name: record.Desired.Name,
		Image: record.Desired.Image, RuntimeIntent: apiTypes.ServiceRuntimeIntent(record.Runtime.RuntimeIntent),
		Zones: append([]string(nil), record.Desired.Zones...), Strategy: string(record.Desired.Strategy),
		OnFailure: apiTypes.OnFailure(record.Desired.OnFailure),
		Resources: apiTypes.ServiceResources{Mem: record.Desired.Resources.Mem, CPUs: record.Desired.Resources.CPUs},
		Expose:    append([]string(nil), record.Desired.Expose...), Restart: record.Desired.Restart,
		Replicas: record.Desired.Replicas, Adapter: record.Desired.Adapter,
		FactsPrefix: record.Desired.FactsPrefix, Label: record.Desired.Label, BackingNetworkID: record.BackingNetworkID,
		Hooks: taskplanning.BackingHookConfigurationToAPI(record.Desired.Hooks),
	}
	if record.Desired.Healthcheck != (core.Healthcheck{}) {
		response.Healthcheck = &apiTypes.ServiceHealthcheck{
			HTTP:        record.Desired.Healthcheck.HTTP,
			TCP:         record.Desired.Healthcheck.TCP,
			Pgrep:       record.Desired.Healthcheck.Pgrep,
			Interval:    record.Desired.Healthcheck.Interval,
			Timeout:     record.Desired.Healthcheck.Timeout,
			StartPeriod: record.Desired.Healthcheck.StartPeriod,
			Retries:     record.Desired.Healthcheck.Retries,
		}
	}
	response.Command = append([]string(nil), record.Desired.Command...)
	response.Mounts = make([]apiTypes.ServiceMount, len(record.Desired.Mounts))
	for index, mount := range record.Desired.Mounts {
		response.Mounts[index] = apiTypes.ServiceMount{
			Volume: mount.Volume,
			File:   mount.File,
			Mount:  mount.Mount,
			RO:     mount.RO,
		}
	}
	response.Aliases = cloneServiceStringSliceMap(record.Desired.Aliases)
	response.DependsOn = make(map[string]apiTypes.ServiceDependency, len(record.Desired.DependsOn))
	for name, dependency := range record.Desired.DependsOn {
		response.DependsOn[name] = apiTypes.ServiceDependency{
			Condition: dependency.Condition.String(),
			Phases:    dependency.PhaseStrings(),
		}
	}
	if len(response.DependsOn) == 0 {
		response.DependsOn = nil
	}
	response.Logging = apiTypes.ServiceLogging{
		MaxSize: record.Desired.Logging.MaxSize,
		MaxFile: record.Desired.Logging.MaxFile,
	}
	return response
}

func cloneServiceStringSliceMap(source map[string][]string) map[string][]string {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string][]string, len(source))
	for key, values := range source {
		result[key] = append([]string(nil), values...)
	}
	return result
}
