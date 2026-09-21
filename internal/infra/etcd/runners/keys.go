package runners

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

const (
	runnerPrefix                 = "/v1/records/runners/"
	runnerLifecyclePrefix        = "/v1/runtime/runner-lifecycles/"
	runnerRuntimeOwnershipPrefix = "/v1/runtime/runner-ownership/"
	runnerObservationPrefix      = "/v1/runtime/observations/runners/"
)

func RunnerKey(id string) string { return runnerPrefix + id }

func RunnerLifecycleKey(id string) string { return runnerLifecyclePrefix + id }

func RunnerRuntimeOwnershipKey(id string) string { return runnerRuntimeOwnershipPrefix + id }

func RunnerObservationKey(id string) string { return runnerObservationPrefix + id }

func RunnerOwnerPrefix(kind RunnerOwnerKind, ownerID string) string {
	return "/v1/indexes/runners/by-owner/" + string(kind) + "/" + ownerID + "/"
}

func RunnerOwnerKey(kind RunnerOwnerKind, ownerID string, runnerID string) string {
	return RunnerOwnerPrefix(kind, ownerID) + runnerID
}

func RunnerTenantSlugKey(tenantID string, slug string) string {
	return "/v1/indexes/runners/by-slug/tenant/" + tenantID + "/" + recordcodec.EncodeKeySegment(slug)
}

func RunnerTenantCursorPrefix(tenantID string) string {
	return "/v1/cursors/runners/by-tenant/" + tenantID + "/"
}
