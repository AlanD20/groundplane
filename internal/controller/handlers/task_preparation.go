package handlers

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func taskPreparationResponse(observed *etcd.ReleasePreparationSnapshot) *apiTypes.TaskPreparation {
	if observed == nil {
		return nil
	}
	var summary string
	switch observed.State {
	case "network_queued":
		summary = "Waiting for the Agent to prepare the captured internal DNS networks. No workload changes have started."
	case "network_preparing":
		summary = "Preparing the captured internal DNS networks before workload deployment."
	case "network_reconciling":
		summary = "A network operation lost its result. GP is checking the captured bridges without changing networks or replaying the workload."
	case "network_proof_required":
		summary = "GP could not prove the captured networks are absent or correctly owned. The release remains blocked; inspect the Agent logs and repair the network state. GP will not replay workload deployment."
	case "network_failed":
		summary = "Network preparation failed. GP is settling the original release; existing workloads are unchanged."
	case "dns_waiting":
		summary = "Networks are prepared. Waiting for CoreDNS to acknowledge their private listeners; the Agent remains available for other Tasks."
	case "dns_failed":
		summary = "CoreDNS could not activate the required private listeners. Workload deployment has not started."
	case "ready":
		summary = "Networks and CoreDNS are ready. Waiting for the Agent to start the captured workload deployment."
	}
	result := &apiTypes.TaskPreparation{State: observed.State, Summary: summary,
		NetworkCount: observed.NetworkCount, Deadline: observed.Deadline}
	if observed.Failure != nil {
		result.Failure = taskStepFailure(
			string(observed.FailureStatus),
			observed.Failure.FailedStepID,
			observed.Failure.Failure,
		)
	}
	return result
}
