// Package serviceruntime captures immutable serving and candidate container
// identity for transient operator inspection. It does not select new workloads.
package serviceruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Serving observations and candidate logs have different authority: a pending
// captured Release may be inspected without claiming it is running or healthy.
func CaptureSources(
	ctx context.Context,
	releases *etcd.ReleaseLedger,
	record etcdstore.Versioned[servicerecord.ServiceRecord],
) ([]*agentpb.ServiceObservationTarget, error) {
	targets := make([]*agentpb.ServiceObservationTarget, 0, 2)
	serving, err := serviceobservation.CaptureLogSource(ctx, releases, record)
	if err != nil {
		return nil, err
	}
	if serving != nil {
		targets = append(targets, serving)
	}
	if record.Record.BackingNetworkID != "" {
		return targets, nil
	}
	candidate, err := releases.CaptureCandidateLogSource(
		ctx,
		record.Record.EnvironmentID,
		record.Record.Desired.ID,
		record.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	if candidate != nil && (serving == nil || candidate.ReleaseId != serving.ReleaseId) {
		targets = append(targets, candidate)
	}
	return targets, nil
}
