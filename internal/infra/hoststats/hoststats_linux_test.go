//go:build linux

package hoststats

import (
	"testing"
	"time"
)

func TestParseLinuxIdentityAndLoadSources(t *testing.T) {
	// Rationale: Host identity and load must use the locked Linux fields
	// without shelling out or leaking the raw source documents.
	t.Parallel()

	operatingSystem, err := parseOSRelease("NAME=Ubuntu\nPRETTY_NAME=\"Ubuntu 24.04.3 LTS\"\n")
	if err != nil {
		t.Fatalf("parseOSRelease() error = %v", err)
	}
	if operatingSystem != "Ubuntu 24.04.3 LTS" {
		t.Fatalf("parseOSRelease() = %q", operatingSystem)
	}
	if model := parseCPUModel("processor: 0\nmodel name: Example CPU 9000\n"); model != "Example CPU 9000" {
		t.Fatalf("parseCPUModel() = %q", model)
	}
	uptime, err := parseUptime("123.75 99.00\n")
	if err != nil || uptime != 123750*time.Millisecond {
		t.Fatalf("parseUptime() = %s, %v", uptime, err)
	}
	load, err := parseLoadOne("1.50 1.00 0.50 1/100 42\n")
	if err != nil || load != 1.5 {
		t.Fatalf("parseLoadOne() = %v, %v", load, err)
	}
}

func TestParseLinuxMemorySources(t *testing.T) {
	// Rationale: the Host card must distinguish available memory and free swap
	// and must convert the kernel's kB unit exactly once.
	t.Parallel()

	memory, swap, err := parseMemory(
		"MemTotal:       8388608 kB\nMemAvailable:   3145728 kB\nSwapTotal:      4194304 kB\nSwapFree:       3932160 kB\n",
	)
	if err != nil {
		t.Fatalf("parseMemory() error = %v", err)
	}
	if memory.Total != 8<<30 || memory.Used != 5<<30 {
		t.Fatalf("memory = %#v", memory)
	}
	if swap.Total != 4<<30 || swap.Used != 256<<20 {
		t.Fatalf("swap = %#v", swap)
	}
}
