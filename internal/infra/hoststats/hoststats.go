// Package hoststats reads bounded operating-system facts for the Host
// projection. It returns raw values; operator-facing formatting belongs to the
// application adapter fixed by ADR 0044.
package hoststats

import (
	"context"
	"time"
)

type Resource struct {
	Total uint64
	Used  uint64
}

type Snapshot struct {
	Hostname string
	Arch     string
	OS       string
	Uptime   time.Duration
	CPUModel string
	CPUCores int
	LoadOne  float64
	Memory   Resource
	Disk     Resource
	Swap     Resource
}

type Collector struct{}

func New() *Collector {
	return &Collector{}
}

type Source interface {
	Snapshot(ctx context.Context) (Snapshot, error)
}
