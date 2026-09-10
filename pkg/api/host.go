package api

type HostCPU struct {
	Model string `json:"model"`
	Cores int    `json:"cores"`
	Load  int    `json:"load"`
}

type HostResource struct {
	Total   string `json:"total"`
	Used    string `json:"used"`
	UsedPct int    `json:"used_pct"`
}

type HostEtcd struct {
	Node   string      `json:"node"`
	Status HealthState `json:"status"`
	DBSize string      `json:"db_size"`
}

type HostController struct {
	Service string                `json:"service"`
	Status  HealthState           `json:"status"`
	Version string                `json:"version"`
	Update  ControllerUpdateState `json:"update"`
}

type HostAgent struct {
	Status        HealthState `json:"status"`
	PullInterval  string      `json:"pull_interval"`
	MaxConcurrent int         `json:"max_concurrent"`
	Labels        []string    `json:"labels"`
}

type Host struct {
	Hostname   string         `json:"hostname"`
	Arch       string         `json:"arch"`
	OS         string         `json:"os"`
	Uptime     string         `json:"uptime"`
	CPU        HostCPU        `json:"cpu"`
	Memory     HostResource   `json:"memory"`
	Disk       HostResource   `json:"disk"`
	Swap       HostResource   `json:"swap"`
	Docker     string         `json:"docker"`
	Etcd       HostEtcd       `json:"etcd"`
	Controller HostController `json:"controller"`
	Agent      HostAgent      `json:"agent"`
}
