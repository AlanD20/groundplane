package backinghookruntime

import (
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"strconv"
	"time"
)

type backingHookEnvironment struct {
	name  string
	value string
}

func backingHookEnvironmentFor(input backinghook.Input) []backingHookEnvironment {
	result := []backingHookEnvironment{
		{name: "GP_EVENT", value: string(input.Context.Event)},
		{name: "GP_BACKING_SERVICE_ID", value: input.Context.BackingServiceID},
	}
	for _, item := range []backingHookEnvironment{
		{name: "GP_ATTACH_ID", value: input.Context.AttachID},
		{name: "GP_TENANT_ID", value: input.Context.TenantID},
		{name: "GP_PROJECT_ID", value: input.Context.ProjectID},
		{name: "GP_ENVIRONMENT_ID", value: input.Context.EnvironmentID},
		{name: "GP_SERVICE_ID", value: input.Context.ServiceID},
	} {
		if item.value != "" {
			result = append(result, item)
		}
	}
	for _, value := range input.Values {
		result = append(result, backingHookEnvironment{name: "GP_INPUT_" + value.Key, value: string(value.Value)})
	}
	for _, value := range input.Facts {
		result = append(result, backingHookEnvironment{name: "GP_FACT_" + value.Key, value: string(value.Value)})
	}
	return result
}

func backingHookDockerExecArgs(
	containerID string,
	environment []backingHookEnvironment,
	command []string,
) []string {
	args := []string{"container", "exec", "-i"}
	for _, variable := range environment {
		args = append(args, "--env", variable.name)
	}
	args = append(args, containerID)
	return append(args, command...)
}

func backingHookDurationArgument(value time.Duration) string {
	return strconv.FormatInt(int64(value/time.Second), 10) + "s"
}

func backingHookProcessEnvironment(environment []backingHookEnvironment) []string {
	result := make([]string, 0, len(environment))
	for _, variable := range environment {
		result = append(result, variable.name+"="+variable.value)
	}
	return result
}

func clearBackingHookEnvironment(environment []backingHookEnvironment) {
	for index := range environment {
		environment[index].name = ""
		environment[index].value = ""
	}
}
