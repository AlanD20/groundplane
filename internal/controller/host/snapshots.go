package host

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/localdiag"
	"github.com/AlanD20/groundplane/internal/controller/agentmanagement"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	hostEtcdNodeLabel   = "single-node"
	hostUnavailable     = "unavailable"
	ControllerUnit      = "groundplane-controller.service"
	hostMaximumPercent  = 100
	hostBinaryUnitScale = 1024
)

type hostDockerVersionSource interface {
	DockerVersion(ctx context.Context) (string, error)
}

type SystemSource struct {
	system hoststats.Source
	docker hostDockerVersionSource
}

func NewSystemSource(system hoststats.Source, docker hostDockerVersionSource) *SystemSource {
	return &SystemSource{system: system, docker: docker}
}

func (source *SystemSource) SystemSnapshot(ctx context.Context) (SystemSnapshot, error) {
	if source == nil || source.system == nil || source.docker == nil {
		return SystemSnapshot{}, errs.New(errs.KindInternal, "host system source is incomplete")
	}
	snapshot, err := source.system.Snapshot(ctx)
	if err != nil {
		return SystemSnapshot{}, err
	}
	dockerVersion, err := source.docker.DockerVersion(ctx)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return SystemSnapshot{}, contextErr
		}
		dockerVersion = hostUnavailable
	}
	return SystemSnapshot{
		Hostname: snapshot.Hostname,
		Arch:     snapshot.Arch,
		OS:       snapshot.OS,
		Uptime:   formatHostUptime(snapshot.Uptime),
		CPU: apiTypes.HostCPU{
			Model: snapshot.CPUModel,
			Cores: snapshot.CPUCores,
			Load:  hostPercentage(snapshot.LoadOne, float64(snapshot.CPUCores)),
		},
		Memory: hostResource(snapshot.Memory),
		Disk:   hostResource(snapshot.Disk),
		Swap:   hostResource(snapshot.Swap),
		Docker: dockerVersion,
	}, nil
}

type hostEtcdProbe func(context.Context, []string) ([]localdiag.EtcdEndpoint, error)

type EtcdSource struct {
	endpoints []string
	probe     hostEtcdProbe
}

func NewEtcdSource(endpoints []string, probe hostEtcdProbe) *EtcdSource {
	return &EtcdSource{endpoints: append([]string(nil), endpoints...), probe: probe}
}

func (source *EtcdSource) EtcdSnapshot(ctx context.Context) (EtcdSnapshot, error) {
	if source == nil || len(source.endpoints) == 0 || source.probe == nil {
		return EtcdSnapshot{}, errs.New(errs.KindInternal, "host etcd source is incomplete")
	}
	rows, err := source.probe(ctx, append([]string(nil), source.endpoints...))
	if contextErr := ctx.Err(); contextErr != nil {
		return EtcdSnapshot{}, contextErr
	}
	if err != nil && !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		return EtcdSnapshot{}, err
	}
	if len(rows) != len(source.endpoints) {
		return EtcdSnapshot{}, errs.New(
			errs.KindInternal,
			"host etcd probe returned an incomplete result set",
		)
	}
	healthy := 0
	maximumDBSize := uint64(0)
	for _, row := range rows {
		if !row.Healthy {
			continue
		}
		healthy++
		if row.DBSizeBytes == nil || *row.DBSizeBytes < 0 {
			return EtcdSnapshot{}, errs.New(errs.KindInternal, "host etcd probe returned an invalid DB size")
		}
		if size := uint64(*row.DBSizeBytes); size > maximumDBSize {
			maximumDBSize = size
		}
	}
	status := apiTypes.HealthFailed
	dbSize := hostUnavailable
	if healthy > 0 {
		status = apiTypes.HealthDegraded
		dbSize = formatHostBytes(maximumDBSize)
	}
	if healthy == len(rows) {
		status = apiTypes.HealthHealthy
	}
	return EtcdSnapshot{Node: hostEtcdNodeLabel, Status: status, DBSize: dbSize}, nil
}

type hostAgentHealthSource interface {
	ListHealth(ctx context.Context) ([]localagent.Health, error)
}

type AgentSource struct {
	health   hostAgentHealthSource
	fallback localagent.Config
}

func NewAgentSource(health hostAgentHealthSource, fallback localagent.Config) *AgentSource {
	return &AgentSource{health: health, fallback: fallback}
}

func (source *AgentSource) AgentSnapshot(ctx context.Context) (AgentSnapshot, error) {
	if source == nil || source.health == nil {
		return AgentSnapshot{}, errs.New(errs.KindInternal, "host Agent source is incomplete")
	}
	health, err := source.health.ListHealth(ctx)
	if err != nil {
		return AgentSnapshot{}, err
	}
	if len(health) > 1 {
		return AgentSnapshot{}, errs.New(errs.KindInternal, "local Agent singleton invariant is violated")
	}
	if len(health) == 0 {
		return hostAgentConfigSnapshot(apiTypes.HealthStopped, source.fallback), nil
	}
	status, err := agentmanagement.ProjectStatus(health[0])
	if err != nil {
		return AgentSnapshot{}, err
	}
	return hostAgentConfigSnapshot(apiTypes.HealthState(status), health[0].Agent.Config), nil
}

func hostAgentConfigSnapshot(status apiTypes.HealthState, config localagent.Config) AgentSnapshot {
	labels := make([]string, 0, len(config.Labels))
	for key, value := range config.Labels {
		labels = append(labels, key+"="+value)
	}
	sort.Strings(labels)
	return AgentSnapshot{
		Status:        status,
		PullInterval:  (time.Duration(config.PullIntervalSeconds) * time.Second).String(),
		MaxConcurrent: int(config.MaxConcurrentTasks),
		Labels:        labels,
	}
}

func hostResource(resource hoststats.Resource) apiTypes.HostResource {
	return apiTypes.HostResource{
		Total: formatHostBytes(resource.Total),
		Used:  formatHostBytes(resource.Used),
		UsedPct: hostPercentage(
			float64(resource.Used),
			float64(resource.Total),
		),
	}
}

func formatHostUptime(uptime time.Duration) string {
	seconds := int64(math.Floor(uptime.Seconds()))
	value := seconds
	unit := "second"
	switch {
	case seconds >= int64(24*time.Hour/time.Second):
		value = seconds / int64(24*time.Hour/time.Second)
		unit = "day"
	case seconds >= int64(time.Hour/time.Second):
		value = seconds / int64(time.Hour/time.Second)
		unit = "hour"
	case seconds >= int64(time.Minute/time.Second):
		value = seconds / int64(time.Minute/time.Second)
		unit = "minute"
	}
	if value != 1 {
		unit += "s"
	}
	return strconv.FormatInt(value, 10) + " " + unit
}

func formatHostBytes(bytes uint64) string {
	units := [...]string{"B", "KiB", "MiB", "GiB", "TiB"}
	value := float64(bytes)
	unit := 0
	for value >= hostBinaryUnitScale && unit < len(units)-1 {
		value /= hostBinaryUnitScale
		unit++
	}
	value = math.Round(value*10) / 10
	formatted := fmt.Sprintf("%.1f", value)
	formatted = strings.TrimSuffix(formatted, ".0")
	return formatted + " " + units[unit]
}

func hostPercentage(used, total float64) int {
	if total <= 0 || used <= 0 {
		return 0
	}
	percentage := math.Round((used / total) * hostMaximumPercent)
	return int(math.Max(0, math.Min(hostMaximumPercent, percentage)))
}
