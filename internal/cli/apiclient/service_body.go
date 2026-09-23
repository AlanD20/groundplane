package apiclient

import (
	"math"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func serviceCreateBody(input apiTypes.ServiceCreate) (generated.ServiceCreateJSONRequestBody, error) {
	resources, err := serviceResourcesBody(input.Resources)
	if err != nil {
		return generated.ServiceCreateJSONRequestBody{}, err
	}
	return generated.ServiceCreateJSONRequestBody{
		EnvironmentId: input.EnvironmentID,
		Name:          input.Name,
		Image:         input.Image,
		Zones:         optionalServiceStrings(input.Zones),
		Strategy:      input.Strategy,
		OnFailure:     string(input.OnFailure),
		Healthcheck:   serviceHealthcheckBody(input.Healthcheck),
		Resources:     resources,
		Expose:        optionalServiceStrings(input.Expose),
		Restart:       input.Restart,
		Replicas:      int64(input.Replicas),
	}, nil
}

func serviceEditBody(input apiTypes.ServiceEdit) (generated.ServiceEditJSONRequestBody, error) {
	resources, err := serviceResourcesBody(input.Resources)
	if err != nil {
		return generated.ServiceEditJSONRequestBody{}, err
	}
	return generated.ServiceEditJSONRequestBody{
		Image:       input.Image,
		Zones:       optionalServiceStrings(input.Zones),
		Strategy:    input.Strategy,
		OnFailure:   string(input.OnFailure),
		Healthcheck: serviceHealthcheckBody(input.Healthcheck),
		Resources:   resources,
		Expose:      optionalServiceStrings(input.Expose),
		Restart:     input.Restart,
		Replicas:    int64(input.Replicas),
		Hooks:       backingHooksToGenerated(input.Hooks),
	}, nil
}

func serviceDeployBody(input apiTypes.DeployRequest) generated.ServiceDeployJSONRequestBody {
	return generated.ServiceDeployJSONRequestBody{
		Tag: optionalServiceString(input.Tag), Strategy: optionalServiceString(input.Strategy),
		OnFailure: optionalServiceString(string(input.OnFailure)),
	}
}

func serviceRollbackBody(input apiTypes.RollbackRequest) generated.ServiceRollbackJSONRequestBody {
	return generated.ServiceRollbackJSONRequestBody{Tag: optionalServiceString(input.Tag)}
}

func serviceHealthcheckBody(input apiTypes.ServiceHealthcheck) generated.ServiceHealthcheck {
	result := generated.ServiceHealthcheck{
		Http: optionalServiceString(input.HTTP), Tcp: optionalServiceString(input.TCP),
		Pgrep: optionalServiceString(input.Pgrep), Interval: optionalServiceString(input.Interval),
		Timeout: optionalServiceString(input.Timeout), StartPeriod: optionalServiceString(input.StartPeriod),
	}
	if input.Retries != 0 {
		value := int64(input.Retries)
		result.Retries = &value
	}
	return result
}

func serviceResourcesBody(input apiTypes.ServiceResources) (generated.ServiceResources, error) {
	if math.IsNaN(input.CPUs) || math.IsInf(input.CPUs, 0) {
		return generated.ServiceResources{}, errs.New(errs.KindInternal, "apiclient: marshal service resources")
	}
	result := generated.ServiceResources{Mem: optionalServiceString(input.Mem)}
	if input.CPUs != 0 {
		result.Cpus = &input.CPUs
	}
	return result, nil
}

func optionalServiceString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func optionalServiceStrings(value []string) *[]string {
	if value == nil {
		return nil
	}
	return &value
}
