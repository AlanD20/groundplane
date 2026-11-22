//go:build linux

package hoststats

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func (collector *Collector) Measure(ctx context.Context) (UsageSnapshot, error) {
	capacity, err := collector.Snapshot(ctx)
	if err != nil {
		return UsageSnapshot{}, err
	}
	read := func(path string) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		file, err := os.Open(path)
		if err != nil {
			return "", errs.Wrap(errs.KindStorageUnavailable, err)
		}
		defer file.Close()
		data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		if err != nil {
			return "", errs.Wrap(errs.KindStorageUnavailable, err)
		}
		if len(data) > 1<<20 {
			return "", measurementError()
		}
		return string(data), nil
	}
	boot, err := read("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return UsageSnapshot{}, err
	}
	cpu, err := read("/proc/stat")
	if err != nil {
		return UsageSnapshot{}, err
	}
	total, busy, err := cpuTicks(cpu)
	if err != nil {
		return UsageSnapshot{}, err
	}
	network, err := read("/proc/net/dev")
	if err != nil {
		return UsageSnapshot{}, err
	}
	interfaces, err := networkCounters(network)
	if err != nil {
		return UsageSnapshot{}, err
	}
	for index := range interfaces {
		_, err := os.Stat(filepath.Join("/sys/class/net", interfaces[index].Name, "device"))
		interfaces[index].Physical = err == nil
	}
	disk, err := read("/proc/diskstats")
	if err != nil {
		return UsageSnapshot{}, err
	}
	devices, err := deviceCounters(disk)
	if err != nil {
		return UsageSnapshot{}, err
	}
	for index := range devices {
		path := filepath.Join(
			"/sys/dev/block",
			strconv.FormatUint(
				uint64(devices[index].Major),
				10,
			)+":"+strconv.FormatUint(
				uint64(devices[index].Minor),
				10,
			),
			"partition",
		)
		_, err := os.Stat(path)
		devices[index].Partition = err == nil
	}
	var root unix.Stat_t
	if err := unix.Stat("/", &root); err != nil {
		return UsageSnapshot{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if err := ctx.Err(); err != nil {
		return UsageSnapshot{}, err
	}
	boot = strings.TrimSpace(boot)
	if len(boot) != 36 {
		return UsageSnapshot{}, measurementError()
	}
	return UsageSnapshot{
		At:              time.Now().UTC(),
		BootID:          boot,
		Capacity:        capacity,
		CPUTotalTicks:   total,
		CPUBusyTicks:    busy,
		Interfaces:      interfaces,
		Devices:         devices,
		RootDeviceMajor: unix.Major(root.Dev),
		RootDeviceMinor: unix.Minor(root.Dev),
	}, nil
}

func cpuTicks(contents string) (uint64, uint64, error) {
	line, _, _ := strings.Cut(contents, "\n")
	fields := strings.Fields(line)
	if len(fields) < 9 || fields[0] != "cpu" {
		return 0, 0, measurementError()
	}
	var total, idle uint64
	// guest and guest_nice are already included in user/nice; only the first
	// eight counters contribute to total elapsed CPU time.
	for index, field := range fields[1:9] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil || math.MaxUint64-total < value {
			return 0, 0, measurementError()
		}
		total += value
		if index == 3 || index == 4 {
			idle += value
		}
	}
	return total, total - idle, nil
}

func networkCounters(contents string) ([]InterfaceUsage, error) {
	var result []InterfaceUsage
	seen := make(map[string]bool)
	for _, line := range strings.Split(contents, "\n") {
		name, raw, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		name = strings.TrimSpace(name)
		if !deviceName(name) || seen[name] || len(result) >= 256 {
			return nil, measurementError()
		}
		fields := strings.Fields(raw)
		if len(fields) != 16 {
			return nil, measurementError()
		}
		receive, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, measurementError()
		}
		transmit, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return nil, measurementError()
		}
		seen[name] = true
		result = append(result, InterfaceUsage{Name: name, ReceiveBytes: receive, TransmitBytes: transmit})
	}
	return result, nil
}

func deviceCounters(contents string) ([]DeviceUsage, error) {
	var result []DeviceUsage
	seen := make(map[string]bool)
	for _, line := range strings.Split(contents, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 14 || !deviceName(fields[2]) || len(result) >= 256 || seen[fields[2]] {
			return nil, measurementError()
		}
		values := make([]uint64, 8)
		for index, column := range []int{0, 1, 3, 5, 7, 9} {
			value, err := strconv.ParseUint(fields[column], 10, 64)
			if err != nil {
				return nil, measurementError()
			}
			values[index] = value
		}
		if values[0] > math.MaxUint32 || values[1] > math.MaxUint32 || values[3] > math.MaxUint64/512 ||
			values[5] > math.MaxUint64/512 {
			return nil, measurementError()
		}
		seen[fields[2]] = true
		// Linux diskstats sector accounting always uses 512-byte sectors,
		// independently of the physical block-device sector size.
		result = append(
			result,
			DeviceUsage{Name: fields[2], Major: uint32(values[0]), Minor: uint32(values[1]), ReadOperations: values[2],
				ReadBytes: values[3] * 512, WriteOperations: values[4], WriteBytes: values[5] * 512},
		)
	}
	return result, nil
}

func deviceName(value string) bool {
	if value == "" || len(value) > 64 || value == "." || value == ".." {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("_.-", char)) {
			return false
		}
	}
	return true
}

func measurementError() error {
	return errs.New(errs.KindStorageUnavailable, "host monitoring counters are invalid or exceed collection bounds")
}
