package hoststats

import "time"

// UsageSnapshot contains host counters, not container totals. Rates require
// consecutive samples from the same boot and must not bridge missing samples.
type UsageSnapshot struct {
	At              time.Time
	BootID          string
	Capacity        Snapshot
	CPUTotalTicks   uint64
	CPUBusyTicks    uint64
	Interfaces      []InterfaceUsage
	Devices         []DeviceUsage
	RootDeviceMajor uint32
	RootDeviceMinor uint32
}

type InterfaceUsage struct {
	Name          string
	Physical      bool
	ReceiveBytes  uint64
	TransmitBytes uint64
}

type DeviceUsage struct {
	Name            string
	Major           uint32
	Minor           uint32
	Partition       bool
	ReadBytes       uint64
	WriteBytes      uint64
	ReadOperations  uint64
	WriteOperations uint64
}
