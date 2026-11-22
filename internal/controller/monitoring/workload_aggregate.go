package monitoring

import (
	"math"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
)

type floatTotal struct {
	value float64
	valid uint32
}

type uintTotal struct {
	value    uint64
	valid    uint32
	overflow bool
}

func (total *floatTotal) add(value *float64) {
	if value != nil {
		total.value, total.valid = total.value+*value, total.valid+1
	}
}

func (total *uintTotal) add(value *uint64) {
	if value == nil {
		return
	}
	total.valid++
	if total.overflow {
		return
	}
	if math.MaxUint64-total.value < *value {
		total.overflow = true
		return
	}
	total.value += *value
}

func (total floatTotal) result(expected uint32) (*float64, metrics.MetricSummary) {
	var value *float64
	if expected > 0 && total.valid == expected {
		value = cloneFloat64(&total.value)
	}
	return value, instantFloatSummary(value, total.valid, expected)
}

func (total uintTotal) result(expected uint32) (*uint64, metrics.MetricSummary) {
	var value *uint64
	if expected > 0 && total.valid == expected && !total.overflow {
		value = cloneUint64(&total.value)
	}
	return value, instantUintSummaryWithCoverage(value, total.valid, expected)
}

func aggregateContainers(
	at time.Time,
	expected uint32,
	containers []ContainerSample,
) (metrics.MetricPoint, metrics.MetricSummaries) {
	var cpu, cpuLimit, receive, transmit, read, write, readOps, writeOps floatTotal
	var memory, memoryLimit, writable uintTotal
	for _, container := range containers {
		point := container.Point
		cpu.add(point.CPUCores)
		cpuLimit.add(point.CPULimitCores)
		memory.add(point.MemoryWorkingSetBytes)
		memoryLimit.add(point.MemoryLimitBytes)
		receive.add(point.NetworkReceiveBytesPerSecond)
		transmit.add(point.NetworkTransmitBytesPerSecond)
		writable.add(point.WritableLayerBytes)
		read.add(point.BlockReadBytesPerSecond)
		write.add(point.BlockWriteBytesPerSecond)
		readOps.add(point.BlockReadOperationsPerSecond)
		writeOps.add(point.BlockWriteOperationsPerSecond)
	}
	point := metrics.MetricPoint{At: at}
	var summaries metrics.MetricSummaries
	point.CPUCores, summaries.CPUCores = cpu.result(expected)
	point.CPULimitCores, summaries.CPULimitCores = cpuLimit.result(expected)
	point.MemoryWorkingSetBytes, summaries.MemoryWorkingSetBytes = memory.result(expected)
	point.MemoryLimitBytes, summaries.MemoryLimitBytes = memoryLimit.result(expected)
	point.NetworkReceiveBytesPerSecond, summaries.NetworkReceiveBytesPerSecond = receive.result(expected)
	point.NetworkTransmitBytesPerSecond, summaries.NetworkTransmitBytesPerSecond = transmit.result(expected)
	point.WritableLayerBytes, summaries.WritableLayerBytes = writable.result(expected)
	point.BlockReadBytesPerSecond, summaries.BlockReadBytesPerSecond = read.result(expected)
	point.BlockWriteBytesPerSecond, summaries.BlockWriteBytesPerSecond = write.result(expected)
	point.BlockReadOperationsPerSecond, summaries.BlockReadOperationsPerSecond = readOps.result(expected)
	point.BlockWriteOperationsPerSecond, summaries.BlockWriteOperationsPerSecond = writeOps.result(expected)
	return point, summaries
}

func aggregateVolumes(volumes []metrics.VolumeUsage) (*uint64, metrics.MetricSummary) {
	var total uintTotal
	for _, volume := range volumes {
		total.add(volume.UsedBytes)
	}
	return total.result(uint32(len(volumes)))
}

func instantFloatSummary(value *float64, valid, expected uint32) metrics.MetricSummary {
	result := metrics.MetricSummary{Expected: 1, ContributorValid: valid, ContributorExpected: expected}
	if value != nil {
		result.Average, result.Minimum, result.Maximum = cloneFloat64(value), cloneFloat64(value), cloneFloat64(value)
		result.Valid = 1
	}
	return result
}

func instantUintSummary(value *uint64, expected uint32) metrics.MetricSummary {
	valid := uint32(0)
	if value != nil {
		valid = 1
	}
	return instantUintSummaryWithCoverage(value, valid, expected)
}

func instantUintSummaryWithCoverage(value *uint64, valid, expected uint32) metrics.MetricSummary {
	result := metrics.MetricSummary{Expected: 1, ContributorValid: valid, ContributorExpected: expected}
	if value != nil {
		converted := float64(*value)
		result.Average, result.Minimum, result.Maximum = &converted, cloneFloat64(&converted), cloneFloat64(&converted)
		result.Valid = 1
	}
	return result
}
