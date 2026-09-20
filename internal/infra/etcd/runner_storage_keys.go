package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
)

const (
	runnerPrefix                 = "/v1/records/runners/"
	runnerLifecyclePrefix        = "/v1/runtime/runner-lifecycles/"
	runnerRuntimeOwnershipPrefix = "/v1/runtime/runner-ownership/"
	runnerObservationPrefix      = "/v1/runtime/observations/runners/"
)

func runnerKey(id string) string { return runnerPrefix + id }

func runnerLifecycleKey(id string) string { return runnerLifecyclePrefix + id }

func runnerRuntimeOwnershipKey(id string) string { return runnerRuntimeOwnershipPrefix + id }

func runnerObservationKey(id string) string { return runnerObservationPrefix + id }

func runnerOwnerPrefix(kind runnerrecord.RunnerOwnerKind, ownerID string) string {
	return "/v1/indexes/runners/by-owner/" + string(kind) + "/" + ownerID + "/"
}

func runnerOwnerKey(kind runnerrecord.RunnerOwnerKind, ownerID string, runnerID string) string {
	return runnerOwnerPrefix(kind, ownerID) + runnerID
}

func runnerTenantSlugKey(tenantID string, slug string) string {
	return "/v1/indexes/runners/by-slug/tenant/" + tenantID + "/" + recordcodec.EncodeKeySegment(slug)
}

func runnerTenantCursorPrefix(tenantID string) string {
	return "/v1/cursors/runners/by-tenant/" + tenantID + "/"
}
