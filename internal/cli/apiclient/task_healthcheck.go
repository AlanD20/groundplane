package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/taskdescription"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskHealthchecksFromGenerated(
	values *[]generated.ConfiguredHealthcheck,
) ([]taskdescription.ConfiguredHealthcheck, error) {
	if values == nil {
		return nil, nil
	}
	result := make([]taskdescription.ConfiguredHealthcheck, len(*values))
	for index, value := range *values {
		if !value.Kind.Valid() || value.ExpectedReplicas <= 0 || value.ExpectedReplicas > 65536 {
			return nil, errs.New(errs.KindInternal, "apiclient: Task configured healthcheck is invalid")
		}
		check := taskdescription.ConfiguredHealthcheck{ServiceID: value.ServiceId, Workload: value.Workload,
			Kind: string(
				value.Kind,
			), RequiresHealthy: value.RequiresHealthy, ExpectedReplicas: uint32(value.ExpectedReplicas),
			Interval: valueOrEmpty(value.Interval), Timeout: valueOrEmpty(value.Timeout),
			StartPeriod: valueOrEmpty(value.StartPeriod), StartInterval: valueOrEmpty(value.StartInterval)}
		if value.HttpProtocol != nil {
			check.HTTPProtocol = string(*value.HttpProtocol)
		}
		if value.LoopbackPort != nil {
			if *value.LoopbackPort <= 0 || *value.LoopbackPort > 65535 {
				return nil, errs.New(errs.KindInternal, "apiclient: Task configured healthcheck port is invalid")
			}
			check.LoopbackPort = uint16(*value.LoopbackPort)
		}
		if value.Retries != nil {
			if *value.Retries < 0 || *value.Retries > 100 {
				return nil, errs.New(errs.KindInternal, "apiclient: Task configured healthcheck retry count is invalid")
			}
			retries := uint32(*value.Retries)
			check.Retries = &retries
		}
		result[index] = check
	}
	if err := taskdescription.ValidateHealthchecks(result); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return result, nil
}
