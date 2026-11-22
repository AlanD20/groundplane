package monitoringreader

import (
	"encoding/json"
	"io"
	"math"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
)

// Pointers retain the difference between an unsupported counter and measured
// zero. Engine responses may add fields; only these numerical fields are read.
type statistics struct {
	ID   string    `json:"id"`
	Read time.Time `json:"read"`
	CPU  *struct {
		Usage *struct {
			Total *uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
	} `json:"cpu_stats"`
	Memory *struct {
		Usage  *uint64           `json:"usage"`
		Values map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	Networks map[string]struct {
		Receive  uint64 `json:"rx_bytes"`
		Transmit uint64 `json:"tx_bytes"`
	} `json:"networks"`
	Block *struct {
		Bytes      []blockCounter `json:"io_service_bytes_recursive"`
		Operations []blockCounter `json:"io_serviced_recursive"`
	} `json:"blkio_stats"`
}

type blockCounter struct {
	Op    string `json:"op"`
	Value uint64 `json:"value"`
}

func decodeStatistics(body io.Reader, id string, now time.Time) (*agentpb.MonitoringContainerSample, error) {
	data, err := io.ReadAll(io.LimitReader(body, (1<<20)+1))
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	var stats statistics
	if len(data) > 1<<20 || json.Unmarshal(data, &stats) != nil || stats.ID != id || stats.Read.IsZero() ||
		stats.Read.After(now.Add(time.Second)) || now.Sub(stats.Read) > 8*time.Second {
		return nil, errs.New(errs.KindValidationFailed, "invalid container monitoring statistics")
	}
	row := &agentpb.MonitoringContainerSample{ContainerId: id, ObservedAtUnixNano: stats.Read.UnixNano()}
	if stats.CPU != nil && stats.CPU.Usage != nil {
		row.CpuTotalNanoseconds = stats.CPU.Usage.Total
	}
	if stats.Memory != nil {
		row.MemoryUsageBytes = stats.Memory.Usage
		inactive, ok := stats.Memory.Values["total_inactive_file"]
		if !ok {
			inactive, ok = stats.Memory.Values["inactive_file"]
		}
		if ok && row.MemoryUsageBytes != nil && inactive <= *row.MemoryUsageBytes {
			row.MemoryInactiveFileBytes = &inactive
		}
	}
	if stats.Networks != nil {
		var receive, transmit uint64
		valid := true
		for _, network := range stats.Networks {
			if math.MaxUint64-receive < network.Receive || math.MaxUint64-transmit < network.Transmit {
				valid = false
				break
			}
			receive, transmit = receive+network.Receive, transmit+network.Transmit
		}
		if valid {
			row.NetworkReceiveBytes, row.NetworkTransmitBytes = &receive, &transmit
		}
	}
	if stats.Block != nil {
		row.BlockReadBytes, row.BlockWriteBytes = blockTotals(stats.Block.Bytes)
		row.BlockReadOperations, row.BlockWriteOperations = blockTotals(stats.Block.Operations)
	}
	return row, nil
}

func blockTotals(counters []blockCounter) (*uint64, *uint64) {
	var read, write uint64
	var hasRead, hasWrite bool
	for _, counter := range counters {
		switch strings.ToLower(counter.Op) {
		case "read":
			if math.MaxUint64-read < counter.Value {
				return nil, nil
			}
			read, hasRead = read+counter.Value, true
		case "write":
			if math.MaxUint64-write < counter.Value {
				return nil, nil
			}
			write, hasWrite = write+counter.Value, true
		}
	}
	var readValue, writeValue *uint64
	if hasRead {
		readValue = &read
	}
	if hasWrite {
		writeValue = &write
	}
	return readValue, writeValue
}

func addContainerDetails(
	row *agentpb.MonitoringContainerSample,
	inspected container.InspectResponse,
	replica uint64,
	storage bool,
) {
	row.Replica, row.RestartCount = uint32(replica), uint64(inspected.RestartCount)
	if config := inspected.HostConfig; config != nil {
		if config.Memory > 0 {
			value := uint64(config.Memory)
			row.MemoryLimitBytes = &value
		}
		var cores float64
		if config.NanoCPUs > 0 {
			cores = float64(config.NanoCPUs) / 1e9
		} else if config.CPUQuota > 0 && config.CPUPeriod > 0 {
			cores = float64(config.CPUQuota) / float64(config.CPUPeriod)
		}
		if cores > 0 {
			row.CpuLimitCores = &cores
		}
	}
	if storage && inspected.SizeRw != nil && *inspected.SizeRw >= 0 {
		value := uint64(*inspected.SizeRw)
		row.WritableLayerBytes = &value
	}
}
