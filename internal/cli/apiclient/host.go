package apiclient

import (
	"bytes"
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (c *Client) ShowHost(ctx context.Context) (apiTypes.Host, error) {
	client, err := c.generatedHumanClient()
	if err != nil {
		return apiTypes.Host{}, err
	}
	response, err := client.HostShowWithResponse(ctx)
	if err != nil {
		return apiTypes.Host{}, generatedCallError(ctx, http.MethodGet, "/api/v1/host", err)
	}
	if err := generatedResponseError(
		http.MethodGet, "/api/v1/host", response.HTTPResponse, response.Body, http.StatusOK,
	); err != nil {
		return apiTypes.Host{}, err
	}
	parsed := response.JSON200
	if parsed == nil {
		parsed = &generated.Host{}
		if err := decodeSingleJSON(http.MethodGet, "/api/v1/host", bytes.NewReader(response.Body), parsed); err != nil {
			return apiTypes.Host{}, err
		}
	}
	return hostFromGenerated(*parsed), nil
}

func hostFromGenerated(host generated.Host) apiTypes.Host {
	labels := make([]string, 0)
	if host.Agent.Labels != nil {
		labels = append(labels, (*host.Agent.Labels)...)
	}
	return apiTypes.Host{
		Hostname: host.Hostname, Arch: host.Arch, OS: host.Os, Uptime: host.Uptime,
		CPU: apiTypes.HostCPU{Model: host.Cpu.Model, Cores: int(host.Cpu.Cores), Load: int(host.Cpu.Load)},
		Memory: apiTypes.HostResource{
			Total: host.Memory.Total, Used: host.Memory.Used, UsedPct: int(host.Memory.UsedPct),
		},
		Disk: apiTypes.HostResource{
			Total: host.Disk.Total, Used: host.Disk.Used, UsedPct: int(host.Disk.UsedPct),
		},
		Swap: apiTypes.HostResource{
			Total: host.Swap.Total, Used: host.Swap.Used, UsedPct: int(host.Swap.UsedPct),
		},
		Docker: host.Docker,
		Etcd: apiTypes.HostEtcd{
			Node: host.Etcd.Node, Status: apiTypes.HealthState(host.Etcd.Status), DBSize: host.Etcd.DbSize,
		},
		Controller: apiTypes.HostController{
			Service: host.Controller.Service,
			Status:  apiTypes.HealthState(host.Controller.Status),
			Version: host.Controller.Version,
		},
		Agent: apiTypes.HostAgent{
			Status:        apiTypes.HealthState(host.Agent.Status),
			PullInterval:  host.Agent.PullInterval,
			MaxConcurrent: int(host.Agent.MaxConcurrent),
			Labels:        labels,
		},
	}
}
