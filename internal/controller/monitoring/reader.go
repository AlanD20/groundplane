package monitoring

import (
	"context"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backingrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LocalHost is the singleton Host selector. It is independent of an Agent's
// enrollment or process generation and remains stable across reinstalls.
const LocalHost = "local"

type Query struct {
	Kind           string
	ID             string
	Range          string
	Points         int
	Limit          int
	Cursor         string
	ConsumerCursor string
}

type ReadHierarchy interface {
	HierarchyInventory
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
}

type ReadServices interface {
	ServiceInventory
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
}

type ReadBacking interface {
	GetBackingService(context.Context, string) (etcdstore.Versioned[backingrecord.Record], error)
}

type ReadConsumers interface {
	ListAttachesByBackingProject(
		context.Context,
		string,
		etcdstore.PageRequest,
	) (etcdstore.Page[attachrecord.Record], error)
}

type HealthReader interface {
	ObserveServices(context.Context, []etcdstore.Versioned[servicerecord.ServiceRecord]) []api.ServiceObservation
}

type WorkloadCache interface {
	CurrentService(string) (ServiceSample, bool)
	Snapshot() (CollectionSnapshot, bool)
}

// Reader never initiates metric collection. All viewers use shared caches;
// history queries have their own deadline and cannot hold a collector lock.
type Reader struct {
	hierarchy    ReadHierarchy
	services     ReadServices
	backing      ReadBacking
	consumers    ReadConsumers
	observations HealthReader
	health       HealthCache
	host         *HostSampler
	workloads    WorkloadCache
	history      HistoryReader
	now          func() time.Time
}

func NewReader(hierarchy ReadHierarchy, services ReadServices, backing ReadBacking, consumers ReadConsumers,
	observations HealthReader, health HealthCache, host *HostSampler, workloads WorkloadCache,
	history HistoryReader, now func() time.Time) (*Reader, error) {
	if hierarchy == nil || services == nil || backing == nil || consumers == nil || observations == nil || health == nil ||
		host == nil || workloads == nil ||
		now == nil {
		return nil, errs.New(errs.KindInternal, "monitoring read dependencies are required")
	}
	return &Reader{hierarchy: hierarchy, services: services, backing: backing, consumers: consumers,
		observations: observations, health: health, host: host, workloads: workloads, history: history, now: now}, nil
}

func (reader *Reader) Show(ctx context.Context, query Query) (api.MonitoringResponse, error) {
	if ctx == nil || reader == nil {
		return api.MonitoringResponse{}, errs.New(errs.KindInternal, "monitoring read context is required")
	}
	window, err := HistoryWindow(reader.now(), query.Range, query.Points)
	if err != nil {
		return api.MonitoringResponse{}, err
	}
	if query.Limit == 0 {
		query.Limit = 25
	}
	if query.Limit < 1 || query.Limit > etcdstore.MaximumPageLimit || len(query.Cursor) > 8192 ||
		len(query.ConsumerCursor) > 8192 {
		return api.MonitoringResponse{}, errs.New(
			errs.KindValidationFailed,
			"monitoring page limit must be between 1 and 200",
		)
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response := api.MonitoringResponse{Window: window.Public, HistoryStatus: "available"}
	if query.Kind != "backing" && query.ConsumerCursor != "" {
		return response, invalidScope()
	}
	switch query.Kind {
	case "host":
		if query.ID != LocalHost || query.Cursor != "" {
			return response, errs.New(
				errs.KindValidationFailed,
				"Host monitoring requires the local singleton without a cursor",
			)
		}
		response.Scope = api.MonitoringScope{Kind: "host", ID: LocalHost, Name: "Host"}
		value := api.HostMonitoring{
			Live:       unavailableLive(),
			Interfaces: []api.MonitoringInterface{},
			Devices:    []api.MonitoringDevice{},
		}
		if sample, ok := reader.host.Current(); ok {
			value.Live = PublicHostLive(sample, reader.now())
			value.Interfaces, value.Devices = HostDetails(sample)
		}
		value.History, err = ReadHostHistory(bounded, reader.history, LocalHost, window)
		response.Host = &value
	case "tenant", "project":
		err = reader.aggregate(bounded, query, window, &response)
		return response, err
	case "environment":
		err = reader.environment(bounded, query, &response)
		return response, err
	case "service", "backing":
		err = reader.service(bounded, query, window, &response)
		return response, err
	default:
		return response, errs.New(
			errs.KindValidationFailed,
			"monitoring scope must be host, tenant, project, environment, service or backing",
		)
	}
	if err != nil {
		response.HistoryStatus = "unavailable"
	}
	return response, nil
}

func unavailableLive() api.MonitoringLive {
	return api.MonitoringLive{Status: "unavailable"}
}

func livePoint(point metrics.MetricPoint, summaries metrics.MetricSummaries,
	coverage metrics.Coverage, available bool, now time.Time) api.MonitoringLive {
	if point.At.IsZero() {
		return unavailableLive()
	}
	at := point.At.UTC()
	status := "fresh"
	if !available {
		status = "unavailable"
	} else if now.Before(at) || now.Sub(at) >= 3*LiveInterval {
		status = "stale"
	}
	return api.MonitoringLive{Status: status, ObservedAt: &at,
		Values: PublicValues(summaries), Coverage: PublicCoverage(coverage)}
}
