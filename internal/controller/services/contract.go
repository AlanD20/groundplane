package services

import (
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/docker/go-units"
)

func normalizeServiceCreate(input apiTypes.ServiceCreate) apiTypes.ServiceCreate {
	if input.Strategy == "" {
		input.Strategy = string(core.StrategyRecreate)
	}
	if input.OnFailure == "" {
		input.OnFailure = apiTypes.OnFailureSwitchBack
	}
	if input.Replicas == 0 {
		input.Replicas = 1
	}
	input.Zones = normalizeServiceStringSet(input.Zones)
	return input
}

func normalizeServiceEdit(input apiTypes.ServiceEdit) apiTypes.ServiceEdit {
	input.Zones = normalizeServiceStringSet(input.Zones)
	if input.Command != nil {
		value := cloneServiceStringsPreservingEmpty(*input.Command)
		input.Command = &value
	}
	if input.Entrypoint != nil {
		value := cloneServiceStringsPreservingEmpty(*input.Entrypoint)
		input.Entrypoint = &value
	}
	if input.Aliases != nil {
		value := make(map[string][]string, len(*input.Aliases))
		for zone, aliases := range *input.Aliases {
			aliases = normalizeServiceStringSet(aliases)
			if len(aliases) != 0 {
				value[zone] = aliases
			}
		}
		input.Aliases = &value
	}
	if input.DependsOn != nil {
		value := make(map[string]apiTypes.ServiceDependency, len(*input.DependsOn))
		for name, dependency := range *input.DependsOn {
			dependency.Phases = append([]string(nil), dependency.Phases...)
			sort.Strings(dependency.Phases)
			value[name] = dependency
		}
		input.DependsOn = &value
	}
	if input.Logging != nil {
		value := *input.Logging
		input.Logging = &value
	}
	return input
}

func cloneServiceStringsPreservingEmpty(values []string) []string {
	result := make([]string, len(values))
	copy(result, values)
	return result
}

func normalizeServiceStringSet(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	if len(result) < 2 {
		return result
	}
	write := 1
	for read := 1; read < len(result); read++ {
		if result[read] != result[write-1] {
			result[write] = result[read]
			write++
		}
	}
	return result[:write]
}

func serviceDesiredFromCreate(input apiTypes.ServiceCreate) core.Service {
	return core.Service{
		Name: input.Name, Image: input.Image, Zones: append([]string(nil), input.Zones...),
		Strategy: core.Strategy(input.Strategy), OnFailure: core.OnFailure(input.OnFailure),
		Healthcheck: serviceHealthcheckToCore(input.Healthcheck),
		Resources:   core.Resources{Mem: input.Resources.Mem, CPUs: input.Resources.CPUs},
		Expose:      append([]string(nil), input.Expose...), Restart: input.Restart, Replicas: input.Replicas,
	}
}

func applyServiceEdit(current core.Service, input apiTypes.ServiceEdit) core.Service {
	desired := current
	if input.VolumeMounts != nil {
		desired.Mounts = nil
		for _, mount := range current.Mounts {
			if mount.File != "" {
				desired.Mounts = append(desired.Mounts, mount)
			}
		}
		for _, mount := range *input.VolumeMounts {
			desired.Mounts = append(desired.Mounts, core.Mount{Volume: mount.Volume, Mount: mount.Mount, RO: mount.RO})
		}
	}
	if input.Command != nil {
		desired.Command = append([]string(nil), (*input.Command)...)
	}
	if input.Entrypoint != nil {
		desired.Entrypoint = append([]string(nil), (*input.Entrypoint)...)
	}
	if input.WorkingDir != nil {
		desired.WorkingDir = *input.WorkingDir
	}
	if input.User != nil {
		desired.User = *input.User
	}
	if input.Aliases != nil {
		desired.Aliases = cloneServiceStringSliceMap(*input.Aliases)
	}
	if input.DependsOn != nil {
		desired.DependsOn = make(map[string]core.ServiceDependency, len(*input.DependsOn))
		for name, dependency := range *input.DependsOn {
			phases := make([]core.ServiceDependencyPhase, len(dependency.Phases))
			for index, phase := range dependency.Phases {
				phases[index] = core.ServiceDependencyPhase(phase)
			}
			desired.DependsOn[name] = core.ServiceDependency{
				Condition: core.ServiceDependencyCondition(dependency.Condition),
				Phases:    phases,
			}
		}
		if len(desired.DependsOn) == 0 {
			desired.DependsOn = nil
		}
	}
	if input.Logging != nil {
		desired.Logging = core.ServiceLogging{MaxSize: input.Logging.MaxSize, MaxFile: input.Logging.MaxFile}
	}
	desired.Image = input.Image
	desired.Zones = append([]string(nil), input.Zones...)
	desired.Strategy = core.Strategy(input.Strategy)
	desired.OnFailure = core.OnFailure(input.OnFailure)
	desired.Healthcheck = serviceHealthcheckToCore(input.Healthcheck)
	desired.Resources = core.Resources{Mem: input.Resources.Mem, CPUs: input.Resources.CPUs}
	desired.Expose = append([]string(nil), input.Expose...)
	desired.Restart = input.Restart
	desired.Replicas = input.Replicas
	if input.Hooks != nil {
		desired.Hooks = taskplanning.BackingHookConfigurationFromAPI(input.Hooks)
	}
	return desired
}

func serviceHealthcheckToCore(input apiTypes.ServiceHealthcheck) core.Healthcheck {
	return core.Healthcheck{
		HTTP:        input.HTTP,
		TCP:         input.TCP,
		Pgrep:       input.Pgrep,
		Interval:    input.Interval,
		Timeout:     input.Timeout,
		StartPeriod: input.StartPeriod,
		Retries:     input.Retries,
	}
}

func validateDirectServiceDesired(environmentID string, desired core.Service) error {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Service mutation requires a stable Environment id")
	}
	if desired.ID == "" {
		desired.ID = ids.New(ids.KindService)
	}
	if desired.Replicas < 1 {
		return errs.New(errs.KindValidationFailed, "Service replicas must be positive")
	}
	if err := desired.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	for zoneName := range desired.Aliases {
		index := sort.SearchStrings(desired.Zones, zoneName)
		if index == len(desired.Zones) || desired.Zones[index] != zoneName {
			return errs.Newf(errs.KindValidationFailed, "Service aliases reference unjoined Zone %q", zoneName)
		}
	}
	return nil
}

func validateServiceEditSettings(input apiTypes.ServiceEdit) error {
	for _, values := range []*[]string{input.Command, input.Entrypoint} {
		if values == nil {
			continue
		}
		for _, value := range *values {
			if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
				return errs.New(
					errs.KindValidationFailed,
					"Service command and entrypoint arguments must be valid NUL-free UTF-8",
				)
			}
		}
	}
	for _, value := range []*string{input.WorkingDir, input.User} {
		if value != nil && (!utf8.ValidString(*value) || strings.ContainsAny(*value, "\x00\r\n")) {
			return errs.New(errs.KindValidationFailed, "Service native settings must be valid single-line UTF-8")
		}
	}
	if input.Aliases != nil {
		for zone, aliases := range *input.Aliases {
			if zone == "" {
				return errs.New(errs.KindValidationFailed, "Service alias Zone name is required")
			}
			for _, alias := range aliases {
				if alias == "" || !utf8.ValidString(alias) || strings.ContainsAny(alias, "\x00\r\n") {
					return errs.New(errs.KindValidationFailed, "Service aliases must be non-empty single-line UTF-8")
				}
			}
		}
	}
	if input.Logging != nil {
		if input.Logging.MaxFile < 0 {
			return errs.New(errs.KindValidationFailed, "Service logging max_file must be non-negative")
		}
		if input.Logging.MaxSize != "" {
			value, err := units.RAMInBytes(input.Logging.MaxSize)
			if err != nil || value <= 0 {
				return errs.New(errs.KindValidationFailed, "Service logging max_size must be a positive byte size")
			}
		}
	}
	return nil
}

func serviceCreateIntentValue(input apiTypes.ServiceCreate) requestidempotency.Value {
	return serviceIntentValue(
		input.EnvironmentID,
		input.Name,
		input.Image,
		input.Zones,
		input.Strategy,
		input.OnFailure,
		input.Healthcheck,
		input.Resources,
		input.Expose,
		input.Restart,
		input.Replicas,
	)
}

func serviceEditIntentValue(input apiTypes.ServiceEdit) requestidempotency.Value {
	value := serviceIntentValue(
		"",
		"",
		input.Image,
		input.Zones,
		input.Strategy,
		input.OnFailure,
		input.Healthcheck,
		input.Resources,
		input.Expose,
		input.Restart,
		input.Replicas,
	)
	value = serviceEditSettingsIntentValue(value, input)
	if input.VolumeMounts != nil {
		mounts := make([]requestidempotency.Value, len(*input.VolumeMounts))
		for index, mount := range *input.VolumeMounts {
			mounts[index] = requestidempotency.Object(
				requestidempotency.Field{Name: "volume", Value: requestidempotency.String(mount.Volume)},
				requestidempotency.Field{Name: "mount", Value: requestidempotency.String(mount.Mount)},
				requestidempotency.Field{Name: "ro", Value: requestidempotency.Bool(mount.RO)},
			)
		}
		value = requestidempotency.Object(
			requestidempotency.Field{Name: "service", Value: value},
			requestidempotency.Field{Name: "volume_mounts", Value: requestidempotency.List(mounts...)},
		)
	}
	if input.Hooks == nil {
		return value
	}
	return requestidempotency.Object(
		requestidempotency.Field{Name: "service", Value: value},
		requestidempotency.Field{Name: "hooks", Value: backingHookIntentValue(input.Hooks)},
	)
}

func serviceEditSettingsIntentValue(
	service requestidempotency.Value,
	input apiTypes.ServiceEdit,
) requestidempotency.Value {
	fields := []requestidempotency.Field{{Name: "service", Value: service}}
	if input.Command != nil {
		fields = append(fields, requestidempotency.Field{
			Name: "command", Value: serviceStringListIntentValue(*input.Command),
		})
	}
	if input.Entrypoint != nil {
		fields = append(fields, requestidempotency.Field{
			Name: "entrypoint", Value: serviceStringListIntentValue(*input.Entrypoint),
		})
	}
	if input.WorkingDir != nil {
		fields = append(fields, requestidempotency.Field{
			Name: "working_dir", Value: requestidempotency.String(*input.WorkingDir),
		})
	}
	if input.User != nil {
		fields = append(fields, requestidempotency.Field{
			Name: "user", Value: requestidempotency.String(*input.User),
		})
	}
	if input.Aliases != nil {
		fields = append(fields, requestidempotency.Field{
			Name: "aliases", Value: serviceStringSliceMapIntentValue(*input.Aliases),
		})
	}
	if input.DependsOn != nil {
		fields = append(fields, requestidempotency.Field{
			Name: "depends_on", Value: serviceDependenciesIntentValue(*input.DependsOn),
		})
	}
	if input.Logging != nil {
		fields = append(fields, requestidempotency.Field{Name: "logging", Value: requestidempotency.Object(
			requestidempotency.Field{Name: "max_file", Value: requestidempotency.Integer(int64(input.Logging.MaxFile))},
			requestidempotency.Field{Name: "max_size", Value: requestidempotency.String(input.Logging.MaxSize)},
		)})
	}
	if len(fields) == 1 {
		return service
	}
	return requestidempotency.Object(fields...)
}

func serviceStringSliceMapIntentValue(values map[string][]string) requestidempotency.Value {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]requestidempotency.Field, len(keys))
	for index, key := range keys {
		fields[index] = requestidempotency.Field{Name: key, Value: serviceStringListIntentValue(values[key])}
	}
	return requestidempotency.Object(fields...)
}

func serviceDependenciesIntentValue(values map[string]apiTypes.ServiceDependency) requestidempotency.Value {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]requestidempotency.Field, len(keys))
	for index, key := range keys {
		dependency := values[key]
		fields[index] = requestidempotency.Field{Name: key, Value: requestidempotency.Object(
			requestidempotency.Field{Name: "condition", Value: requestidempotency.String(dependency.Condition)},
			requestidempotency.Field{Name: "phases", Value: serviceStringListIntentValue(dependency.Phases)},
		)}
	}
	return requestidempotency.Object(fields...)
}

func backingHookIntentValue(configuration *apiTypes.BackingHookConfiguration) requestidempotency.Value {
	definition := func(value *apiTypes.BackingHookDefinition) requestidempotency.Value {
		if value == nil {
			return requestidempotency.Null()
		}
		command := make([]requestidempotency.Value, len(value.Command))
		for index, argument := range value.Command {
			command[index] = requestidempotency.String(argument)
		}
		return requestidempotency.Object(
			requestidempotency.Field{Name: "command", Value: requestidempotency.List(command...)},
			requestidempotency.Field{
				Name:  "timeout_seconds",
				Value: requestidempotency.Integer(int64(value.TimeoutSeconds)),
			},
		)
	}
	facts := make([]requestidempotency.Value, len(configuration.Facts))
	for index, fact := range configuration.Facts {
		facts[index] = requestidempotency.Object(
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(fact.Key)},
			requestidempotency.Field{Name: "secret", Value: requestidempotency.Bool(fact.Secret)},
		)
	}
	inputs := make([]requestidempotency.Value, len(configuration.Inputs))
	for index, input := range configuration.Inputs {
		literal := requestidempotency.Null()
		if input.Value != nil {
			literal = requestidempotency.String(*input.Value)
		}
		inputs[index] = requestidempotency.Object(
			requestidempotency.Field{Name: "generate", Value: requestidempotency.String(input.Generate)},
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(input.Key)},
			requestidempotency.Field{Name: "secret_ref", Value: requestidempotency.String(input.SecretRef)},
			requestidempotency.Field{Name: "value", Value: literal},
		)
	}
	return requestidempotency.Object(
		requestidempotency.Field{Name: "after_start", Value: definition(configuration.AfterStart)},
		requestidempotency.Field{Name: "attach", Value: definition(configuration.Attach)},
		requestidempotency.Field{Name: "before_stop", Value: definition(configuration.BeforeStop)},
		requestidempotency.Field{Name: "detach", Value: definition(configuration.Detach)},
		requestidempotency.Field{Name: "facts", Value: requestidempotency.List(facts...)},
		requestidempotency.Field{Name: "inputs", Value: requestidempotency.List(inputs...)},
	)
}

func serviceIntentValue(
	environmentID string,
	name string,
	image string,
	zones []string,
	strategy string,
	onFailure apiTypes.OnFailure,
	healthcheck apiTypes.ServiceHealthcheck,
	resources apiTypes.ServiceResources,
	expose []string,
	restart string,
	replicas int,
) requestidempotency.Value {
	fields := []requestidempotency.Field{
		{Name: "expose", Value: serviceStringListIntentValue(expose)},
		{Name: "healthcheck", Value: serviceHealthcheckIntentValue(healthcheck)},
		{Name: "image", Value: requestidempotency.String(image)},
		{Name: "on_failure", Value: requestidempotency.String(string(onFailure))},
		{Name: "replicas", Value: requestidempotency.Integer(int64(replicas))},
		{Name: "resources", Value: requestidempotency.Object(
			requestidempotency.Field{
				Name:  "cpus",
				Value: requestidempotency.String(strconv.FormatFloat(resources.CPUs, 'g', -1, 64)),
			},
			requestidempotency.Field{Name: "mem", Value: requestidempotency.String(resources.Mem)},
		)},
		{Name: "restart", Value: requestidempotency.String(restart)},
		{Name: "strategy", Value: requestidempotency.String(strategy)},
		{Name: "zones", Value: serviceStringListIntentValue(zones)},
	}
	if environmentID != "" {
		fields = append(fields,
			requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(environmentID)},
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(name)},
		)
	}
	return requestidempotency.Object(fields...)
}

func serviceStringListIntentValue(values []string) requestidempotency.Value {
	items := make([]requestidempotency.Value, len(values))
	for index, value := range values {
		items[index] = requestidempotency.String(value)
	}
	return requestidempotency.List(items...)
}

func serviceHealthcheckIntentValue(value apiTypes.ServiceHealthcheck) requestidempotency.Value {
	return requestidempotency.Object(
		requestidempotency.Field{Name: "http", Value: requestidempotency.String(value.HTTP)},
		requestidempotency.Field{Name: "interval", Value: requestidempotency.String(value.Interval)},
		requestidempotency.Field{Name: "pgrep", Value: requestidempotency.String(value.Pgrep)},
		requestidempotency.Field{Name: "retries", Value: requestidempotency.Integer(int64(value.Retries))},
		requestidempotency.Field{Name: "start_period", Value: requestidempotency.String(value.StartPeriod)},
		requestidempotency.Field{Name: "tcp", Value: requestidempotency.String(value.TCP)},
		requestidempotency.Field{Name: "timeout", Value: requestidempotency.String(value.Timeout)},
	)
}
