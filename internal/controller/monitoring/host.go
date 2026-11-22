// Package monitoring coordinates read-only resource telemetry. Sampling and
// history failures never grant work authority or change serving workloads.
package monitoring

import (
	"context"
	"crypto/sha256"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const LiveInterval = 5 * time.Second

type HostSource interface {
	Measure(context.Context) (hoststats.UsageSnapshot, error)
}

type HostSample struct {
	Observed hoststats.UsageSnapshot
	Point    metrics.MetricPoint
	Previous *hoststats.UsageSnapshot
}

type HostSampler struct {
	mu       sync.RWMutex
	source   HostSource
	hostID   string
	previous *hoststats.UsageSnapshot
	raw      *metrics.RawHostSample
	current  *HostSample
}

func NewHostSampler(source HostSource, hostID string) (*HostSampler, error) {
	if source == nil || hostID == "" {
		return nil, errs.New(errs.KindValidationFailed, "host monitoring source and identity are required")
	}
	return &HostSampler{source: source, hostID: hostID}, nil
}

// Collect is called by the sole monitoring loop, never by every viewer. Current
// readers share its immutable snapshot instead of triggering host reads.
func (sampler *HostSampler) Collect(ctx context.Context) error {
	if ctx == nil || sampler == nil {
		return errs.New(errs.KindInternal, "host monitoring context is required")
	}
	observed, err := sampler.source.Measure(ctx)
	if err != nil {
		return err
	}
	raw, err := rawHost(sampler.hostID, observed)
	if err != nil {
		return err
	}
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	point, _, err := metrics.DeriveHostPoint(sampler.raw, raw, 2*LiveInterval)
	if err != nil {
		return err
	}
	value := HostSample{Observed: observed, Point: point, Previous: sampler.previous}
	sampler.current = &value
	sampler.previous = &observed
	sampler.raw = &raw
	return nil
}

// Current returns a detached snapshot. Derived rates are unavailable until two
// valid samples exist; a failed collection does not refresh this timestamp.
func (sampler *HostSampler) Current() (HostSample, bool) {
	if sampler == nil {
		return HostSample{}, false
	}
	sampler.mu.RLock()
	defer sampler.mu.RUnlock()
	if sampler.current == nil {
		return HostSample{}, false
	}
	value := *sampler.current
	value.Observed.Interfaces = append([]hoststats.InterfaceUsage(nil), value.Observed.Interfaces...)
	value.Observed.Devices = append([]hoststats.DeviceUsage(nil), value.Observed.Devices...)
	if value.Previous != nil {
		previous := *value.Previous
		previous.Interfaces = append([]hoststats.InterfaceUsage(nil), previous.Interfaces...)
		previous.Devices = append([]hoststats.DeviceUsage(nil), previous.Devices...)
		value.Previous = &previous
	}
	// Point pointers are private values; clone through the domain projection so
	// callers cannot mutate the collector baseline through a response.
	value.Point = metrics.ClonePoint(value.Point)
	return value, true
}

func rawHost(hostID string, observed hoststats.UsageSnapshot) (metrics.RawHostSample, error) {
	if observed.At.IsZero() || observed.BootID == "" || observed.Capacity.CPUCores <= 0 ||
		uint64(observed.Capacity.CPUCores) > math.MaxUint32 {
		return metrics.RawHostSample{}, errs.New(errs.KindStorageUnavailable, "host monitoring source is incomplete")
	}
	rootKey := strconv.FormatUint(
		uint64(observed.RootDeviceMajor),
		10,
	) + ":" + strconv.FormatUint(
		uint64(observed.RootDeviceMinor),
		10,
	)
	result := metrics.RawHostSample{
		At:     observed.At,
		HostID: hostID,
		Boot:   sha256.Sum256([]byte(observed.BootID + "/" + rootKey)),
		LogicalCPUCores: uint32(
			observed.Capacity.CPUCores,
		),
		CPUTotalTicks:    observed.CPUTotalTicks,
		CPUBusyTicks:     observed.CPUBusyTicks,
		MemoryUsedBytes:  &observed.Capacity.Memory.Used,
		MemoryTotalBytes: &observed.Capacity.Memory.Total,
		DiskUsedBytes:    &observed.Capacity.Disk.Used,
		DiskTotalBytes:   &observed.Capacity.Disk.Total,
	}
	var names []string
	var receive, transmit uint64
	valid := true
	for _, network := range observed.Interfaces {
		if !network.Physical || network.Name == "lo" {
			continue
		}
		names = append(names, network.Name)
		if math.MaxUint64-receive < network.ReceiveBytes || math.MaxUint64-transmit < network.TransmitBytes {
			valid = false
			break
		}
		receive, transmit = receive+network.ReceiveBytes, transmit+network.TransmitBytes
	}
	if valid && len(names) > 0 {
		sort.Strings(names)
		result.NetworkInterfaces = sha256.Sum256([]byte(strings.Join(names, "\x00")))
		result.NetworkReceiveBytes, result.NetworkTransmitBytes = &receive, &transmit
	}
	for _, device := range observed.Devices {
		if device.Major != observed.RootDeviceMajor || device.Minor != observed.RootDeviceMinor {
			continue
		}
		result.BlockReadBytes, result.BlockWriteBytes = &device.ReadBytes, &device.WriteBytes
		result.BlockReadOperations, result.BlockWriteOperations = &device.ReadOperations, &device.WriteOperations
		break
	}
	return result, nil
}
