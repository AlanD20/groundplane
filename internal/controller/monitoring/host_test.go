package monitoring

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/hoststats"
)

type hostFixture struct {
	value hoststats.UsageSnapshot
	err   error
}

func (source *hostFixture) Measure(context.Context) (hoststats.UsageSnapshot, error) {
	return source.value, source.err
}

// Whole-host network readings must not count bridge/overlay traffic twice, and
// a failed read must not make a retained observation appear freshly sampled.
func TestHostSamplingUsesPhysicalTrafficAndPreservesFreshness(t *testing.T) {
	source := &hostFixture{value: hoststats.UsageSnapshot{
		At: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC), BootID: "boot", CPUTotalTicks: 100, CPUBusyTicks: 25,
		Capacity: hoststats.Snapshot{
			CPUCores: 4,
			Memory:   hoststats.Resource{Total: 1024, Used: 256},
			Disk:     hoststats.Resource{Total: 8192, Used: 1024},
		},
		Interfaces: []hoststats.InterfaceUsage{
			{Name: "eth0", Physical: true, ReceiveBytes: 100, TransmitBytes: 200},
			{Name: "tailscale0", ReceiveBytes: 10000, TransmitBytes: 20000},
		},
		RootDeviceMajor: 8, RootDeviceMinor: 1,
		Devices: []hoststats.DeviceUsage{
			{Name: "sda", Major: 8, Minor: 0, ReadBytes: 9000},
			{Name: "sda1", Major: 8, Minor: 1, ReadBytes: 100, WriteBytes: 200},
		},
	}}
	sampler, err := NewHostSampler(source, "host")
	if err != nil {
		t.Fatal(err)
	}
	if err = sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	first, _ := sampler.Current()
	if first.Point.CPUCores != nil || first.Point.NetworkReceiveBytesPerSecond != nil {
		t.Fatal("first sample invented counter rates")
	}
	source.value.At = source.value.At.Add(5 * time.Second)
	source.value.CPUTotalTicks, source.value.CPUBusyTicks = 200, 50
	source.value.Interfaces = []hoststats.InterfaceUsage{
		{Name: "eth0", Physical: true, ReceiveBytes: 150, TransmitBytes: 250},
		{Name: "tailscale0", ReceiveBytes: 99000, TransmitBytes: 99000},
	}
	source.value.Devices = []hoststats.DeviceUsage{
		{Name: "sda", Major: 8, Minor: 0, ReadBytes: 99999},
		{Name: "sda1", Major: 8, Minor: 1, ReadBytes: 150, WriteBytes: 250},
	}
	if err = sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, _ := sampler.Current()
	if *second.Point.CPUCores != 1 || *second.Point.NetworkReceiveBytesPerSecond != 10 ||
		*second.Point.BlockReadBytesPerSecond != 10 {
		t.Fatalf("host rates double-counted or used load average: %#v", second.Point)
	}
	*second.Point.CPUCores = 99
	second.Observed.Interfaces[0].Name = "changed"
	retained, _ := sampler.Current()
	if *retained.Point.CPUCores != 1 || retained.Observed.Interfaces[0].Name != "eth0" {
		t.Fatal("reader mutated retained host observation")
	}
	source.value.At = source.value.At.Add(5 * time.Second)
	source.err = errors.New("host read unavailable")
	if sampler.Collect(t.Context()) == nil {
		t.Fatal("failed host read was accepted")
	}
	retained, _ = sampler.Current()
	if !retained.Observed.At.Equal(second.Observed.At) {
		t.Fatal("failed read replaced the observation time")
	}
}
