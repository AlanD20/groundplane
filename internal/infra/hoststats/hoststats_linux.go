//go:build linux

package hoststats

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const (
	osReleasePath = "/etc/os-release"
	cpuInfoPath   = "/proc/cpuinfo"
	uptimePath    = "/proc/uptime"
	loadAvgPath   = "/proc/loadavg"
	memInfoPath   = "/proc/meminfo"
	diskPath      = "/"
)

func (collector *Collector) Snapshot(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, errs.New(errs.KindInternal, "host stats context is required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return Snapshot{}, errs.Wrap(errs.KindInternal, fmt.Errorf("host stats: resolve hostname: %w", err))
	}
	osRelease, err := readHostFile(ctx, osReleasePath)
	if err != nil {
		return Snapshot{}, err
	}
	cpuInfo, err := readHostFile(ctx, cpuInfoPath)
	if err != nil {
		return Snapshot{}, err
	}
	uptimeData, err := readHostFile(ctx, uptimePath)
	if err != nil {
		return Snapshot{}, err
	}
	loadData, err := readHostFile(ctx, loadAvgPath)
	if err != nil {
		return Snapshot{}, err
	}
	memoryData, err := readHostFile(ctx, memInfoPath)
	if err != nil {
		return Snapshot{}, err
	}
	var filesystem unix.Statfs_t
	if err := unix.Statfs(diskPath, &filesystem); err != nil {
		return Snapshot{}, errs.Wrap(errs.KindInternal, fmt.Errorf("host stats: stat root filesystem: %w", err))
	}

	operatingSystem, err := parseOSRelease(string(osRelease))
	if err != nil {
		return Snapshot{}, err
	}
	cpuModel := parseCPUModel(string(cpuInfo))
	if cpuModel == "" {
		cpuModel = runtime.GOARCH
	}
	uptime, err := parseUptime(string(uptimeData))
	if err != nil {
		return Snapshot{}, err
	}
	loadOne, err := parseLoadOne(string(loadData))
	if err != nil {
		return Snapshot{}, err
	}
	memory, swap, err := parseMemory(string(memoryData))
	if err != nil {
		return Snapshot{}, err
	}
	disk, err := filesystemResource(filesystem)
	if err != nil {
		return Snapshot{}, err
	}
	cores := runtime.NumCPU()
	if cores <= 0 {
		return Snapshot{}, errs.New(errs.KindInternal, "host stats returned no logical CPU cores")
	}
	return Snapshot{
		Hostname: strings.TrimSpace(hostname), Arch: runtime.GOARCH, OS: operatingSystem,
		Uptime: uptime, CPUModel: cpuModel, CPUCores: cores, LoadOne: loadOne,
		Memory: memory, Disk: disk, Swap: swap,
	}, nil
}

func readHostFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("host stats: read %s: %w", path, err))
	}
	return contents, nil
}

func parseOSRelease(contents string) (string, error) {
	values := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(contents))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		decoded, err := decodeOSReleaseValue(strings.TrimSpace(value))
		if err != nil {
			return "", errs.Wrap(errs.KindInternal, fmt.Errorf("host stats: decode os-release %s: %w", key, err))
		}
		values[key] = decoded
	}
	if err := scanner.Err(); err != nil {
		return "", errs.Wrap(errs.KindInternal, fmt.Errorf("host stats: scan os-release: %w", err))
	}
	if value := strings.TrimSpace(values["PRETTY_NAME"]); value != "" {
		return value, nil
	}
	fallback := strings.TrimSpace(strings.TrimSpace(values["NAME"]) + " " + strings.TrimSpace(values["VERSION"]))
	if fallback == "" {
		return "", errs.New(errs.KindInternal, "host stats: os-release has no display name")
	}
	return fallback, nil
}

func decodeOSReleaseValue(value string) (string, error) {
	if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') ||
		(value[0] == '\'' && value[len(value)-1] == '\'')) {
		if value[0] == '\'' {
			return value[1 : len(value)-1], nil
		}
		return strconv.Unquote(value)
	}
	return value, nil
}

func parseCPUModel(contents string) string {
	fallback := ""
	for _, line := range strings.Split(contents, "\n") {
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(key) {
		case "model name":
			if value != "" {
				return value
			}
		case "Processor", "Hardware":
			if fallback == "" {
				fallback = value
			}
		}
	}
	return fallback
}

func parseUptime(contents string) (time.Duration, error) {
	fields := strings.Fields(contents)
	if len(fields) == 0 {
		return 0, errs.New(errs.KindInternal, "host stats: uptime is empty")
	}
	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 ||
		seconds > float64(math.MaxInt64)/float64(time.Second) {
		return 0, errs.New(errs.KindInternal, "host stats: uptime is invalid")
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func parseLoadOne(contents string) (float64, error) {
	fields := strings.Fields(contents)
	if len(fields) == 0 {
		return 0, errs.New(errs.KindInternal, "host stats: load average is empty")
	}
	load, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(load) || math.IsInf(load, 0) || load < 0 {
		return 0, errs.New(errs.KindInternal, "host stats: one-minute load is invalid")
	}
	return load, nil
}

func parseMemory(contents string) (Resource, Resource, error) {
	values := make(map[string]uint64)
	scanner := bufio.NewScanner(strings.NewReader(contents))
	for scanner.Scan() {
		key, raw, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		switch key {
		case "MemTotal", "MemAvailable", "SwapTotal", "SwapFree":
			fields := strings.Fields(raw)
			if len(fields) != 2 || fields[1] != "kB" {
				return Resource{}, Resource{}, errs.New(errs.KindInternal, "host stats: meminfo unit is invalid")
			}
			kilobytes, err := strconv.ParseUint(fields[0], 10, 64)
			if err != nil || kilobytes > math.MaxUint64/1024 {
				return Resource{}, Resource{}, errs.New(errs.KindInternal, "host stats: meminfo value is invalid")
			}
			values[key] = kilobytes * 1024
		}
	}
	if err := scanner.Err(); err != nil {
		return Resource{}, Resource{}, errs.Wrap(errs.KindInternal, fmt.Errorf("host stats: scan meminfo: %w", err))
	}
	memoryTotal, hasMemoryTotal := values["MemTotal"]
	memoryAvailable, hasMemoryAvailable := values["MemAvailable"]
	swapTotal, hasSwapTotal := values["SwapTotal"]
	swapFree, hasSwapFree := values["SwapFree"]
	if !hasMemoryTotal || !hasMemoryAvailable || !hasSwapTotal || !hasSwapFree ||
		memoryAvailable > memoryTotal || swapFree > swapTotal {
		return Resource{}, Resource{}, errs.New(errs.KindInternal, "host stats: meminfo is incomplete or inconsistent")
	}
	return Resource{Total: memoryTotal, Used: memoryTotal - memoryAvailable},
		Resource{Total: swapTotal, Used: swapTotal - swapFree}, nil
}

func filesystemResource(filesystem unix.Statfs_t) (Resource, error) {
	if filesystem.Bsize <= 0 {
		return Resource{}, errs.New(errs.KindInternal, "host stats: filesystem block size is invalid")
	}
	blockSize := uint64(filesystem.Bsize)
	blocks := uint64(filesystem.Blocks)
	freeBlocks := uint64(filesystem.Bfree)
	if freeBlocks > blocks || (blocks > 0 && blockSize > math.MaxUint64/blocks) ||
		(freeBlocks > 0 && blockSize > math.MaxUint64/freeBlocks) {
		return Resource{}, errs.New(errs.KindInternal, "host stats: filesystem values are invalid")
	}
	total := blocks * blockSize
	free := freeBlocks * blockSize
	return Resource{Total: total, Used: total - free}, nil
}
