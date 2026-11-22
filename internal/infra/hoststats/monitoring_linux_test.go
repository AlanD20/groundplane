//go:build linux

package hoststats

import "testing"

// Guest CPU time is already charged to user/nice. Counting it again inflates
// host utilization, while idle and iowait must remain outside busy time.
func TestCPUCountersExcludeDoubleCountedGuest(t *testing.T) {
	total, busy, err := cpuTicks("cpu 100 10 20 200 5 2 3 7 999 500\ncpu0 1 2 3 4\n")
	if err != nil || total != 347 || busy != 142 {
		t.Fatalf("CPU accounting = %d/%d, %v", busy, total, err)
	}
	if _, _, err := cpuTicks("cpu 18446744073709551615 1 0 0 0 0 0 0"); err == nil {
		t.Fatal("overflowing counters accepted")
	}
}

// Interface and device counters retain identities; the caller must never sum
// overlapping virtual/physical interfaces or whole disks and partitions.
func TestHostCountersRetainIndividualDevices(t *testing.T) {
	network, err := networkCounters(
		"Inter-| Receive | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n wlp4s0: 100 0 0 0 0 0 0 0 200 0 0 0 0 0 0 0\n tailscale0: 10 0 0 0 0 0 0 0 20 0 0 0 0 0 0 0\n",
	)
	if err != nil || len(network) != 2 || network[0].Name != "wlp4s0" || network[0].ReceiveBytes != 100 ||
		network[1].TransmitBytes != 20 {
		t.Fatalf("network evidence = %#v, %v", network, err)
	}
	disks, err := deviceCounters("259 0 nvme0n1 10 0 20 0 30 0 40 0 0 0 0\n259 1 nvme0n1p1 1 0 2 0 3 0 4 0 0 0 0\n")
	if err != nil || len(disks) != 2 || disks[0].Major != 259 || disks[1].Minor != 1 || disks[0].ReadBytes != 10240 ||
		disks[0].WriteBytes != 20480 ||
		disks[0].ReadOperations != 10 {
		t.Fatalf("disk evidence = %#v, %v", disks, err)
	}
	if _, err := networkCounters("../../etc: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"); err == nil {
		t.Fatal("unsafe interface name accepted")
	}
	if _, err := deviceCounters("8 0 sda 1 0 18446744073709551615 0 1 0 1 0 0 0 0"); err == nil {
		t.Fatal("sector conversion overflow accepted")
	}
}
