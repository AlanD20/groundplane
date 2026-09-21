package etcd

import (
	"crypto/sha256"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

func hierarchyDeletionStablePrivateID(prefix string, values ...string) string {
	digest := hierarchyDeletionFoldDigest("groundplane-deletion-private-id-v1", values...)
	return prefix + "_" + digest[:32]
}

func hierarchyDeletionStableOperationID(values ...string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("groundplane-deletion-child-operation-v1"))
	for _, value := range values {
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write([]byte(value))
	}
	value := digest.Sum(nil)
	var identifier ulid.ULID
	copy(identifier[:], value[:len(identifier)])
	clear(value)
	return string(ids.KindOperation) + "_" + identifier.String()
}

func terminalHierarchyDeletionNodes(nodes []HierarchyDeletionMembershipNode) []string {
	if len(nodes) == 0 {
		return nil
	}
	required := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		for _, prerequisite := range node.PrerequisiteNodeIDs {
			required[prerequisite] = struct{}{}
		}
	}
	terminal := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if _, isPrerequisite := required[node.NodeID]; !isPrerequisite {
			terminal = append(terminal, node.NodeID)
		}
	}
	sort.Strings(terminal)
	return terminal
}

func hierarchyDeletionAgentProcedure(action hierarchydeletion.HierarchyDeletionActionKind) string {
	return map[hierarchydeletion.HierarchyDeletionActionKind]string{
		HierarchyDeletionAttachGrantRevoke: "attach.grant-revoke", HierarchyDeletionAttachDetach: "attach.detach",
		HierarchyDeletionEnvironmentAgentCleanup: "environment.cleanup",
		HierarchyDeletionMaterializationRemove:   "materialization.remove",
		HierarchyDeletionNetworkRemove:           "network.remove", HierarchyDeletionRecoveryPointRemove: "recovery-point.remove",
		HierarchyDeletionOrphanObjectRemove:        "orphan-object.remove",
		HierarchyDeletionBackingRuntimeReconstruct: "backing.runtime-reconstruct",
	}[action]
}

func hierarchyDeletionControllerFinalizer(action hierarchydeletion.HierarchyDeletionActionKind) string {
	return map[hierarchydeletion.HierarchyDeletionActionKind]string{
		HierarchyDeletionServiceRemove:   "service.remove",
		HierarchyDeletionEntryRemove:     "entry.remove",
		HierarchyDeletionRouteRemove:     "route.remove",
		HierarchyDeletionComponentRemove: "component.remove",
		HierarchyDeletionScriptRemove:    "script.remove", HierarchyDeletionReleaseGroupRemove: "release-group.remove",
		HierarchyDeletionReleaseFinalize: "release.finalize", HierarchyDeletionBackupPolicyFinalize: "backup-policy.finalize",
		HierarchyDeletionKeyMaterialRemove:  "key-material.remove",
		HierarchyDeletionZoneRemove:         "zone.remove",
		HierarchyDeletionReservationRelease: "reservation.release", HierarchyDeletionConnectorFinalize: "connector.finalize",
		HierarchyDeletionEnvironmentFinalize: "environment.finalize", HierarchyDeletionRunnerLocalRemove: "runner.remove",
		HierarchyDeletionProjectSecretRemove: "secret.remove", HierarchyDeletionBackingServiceFinalize: "backing.finalize",
		HierarchyDeletionProjectFinalize: "project.finalize", HierarchyDeletionTenantFinalize: "tenant.finalize",
	}[action]
}

func validateHierarchyDeletionAttachOwner(value []byte, id, owner string) error {
	record, err := attachrecord.DecodeAttachRecord(value)
	if err != nil || record.ID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionServiceOwner(value []byte, id, owner string) error {
	record, err := servicerecord.DecodeServiceRuntimeRecord(value)
	if err != nil || record.ServiceID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionReleaseGroupOwner(value []byte, id, owner string) error {
	record, err := decodeReleaseGroupStored(value)
	if err != nil || record.ID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionEntryOwner(value []byte, id, owner string) error {
	record, err := entryrecord.DecodeRecord(value)
	if err != nil || record.Entry.ID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionRouteOwner(value []byte, id, owner string) error {
	record, err := routerecord.DecodeObservation(value)
	if err != nil || record.RouteID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionComponentOwner(value []byte, id, owner string) error {
	record, err := componentrecord.DecodeRecord(value)
	if err != nil || record.Desired.ID != id || record.Desired.OwnerID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionScriptOwner(value []byte, id, owner string) error {
	record, err := scriptrecord.DecodeRecord(value)
	if err != nil || record.Desired.ID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if record.ActiveReferences != 0 {
		return errs.New(errs.KindResourceInUse, "active Script executions fence hierarchy deletion")
	}
	return nil
}

func validateHierarchyDeletionZoneOwner(value []byte, id, owner string) error {
	evidence, err := decodeHierarchyDeletionZoneEvidence(value)
	if err != nil || evidence.ZoneID != id || evidence.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionConnectorOwner(value []byte, id, owner string) error {
	record, err := connectorrecord.DecodeRecord(value)
	if err != nil || record.Connector.ID != id || record.Connector.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionRunnerOwner(value []byte, id, owner string) error {
	record, err := runnerrecord.DecodeRunnerDesiredAggregate(value)
	if err != nil || record.Desired.ID != id || record.Desired.OwnerID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionSecretOwner(value []byte, id, owner string) error {
	record, err := secretrecord.DecodeRecord(value)
	if err != nil || record.Secret.ID != id || record.Secret.ProjectID != owner ||
		record.Secret.Scope != core.SecretScopeProject {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}
