package hierarchydeletionplanning

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
		hierarchydeletion.HierarchyDeletionAttachGrantRevoke: "attach.grant-revoke", hierarchydeletion.HierarchyDeletionAttachDetach: "attach.detach",
		hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup: "environment.cleanup",
		hierarchydeletion.HierarchyDeletionMaterializationRemove:   "materialization.remove",
		hierarchydeletion.HierarchyDeletionNetworkRemove:           "network.remove", hierarchydeletion.HierarchyDeletionRecoveryPointRemove: "recovery-point.remove",
		hierarchydeletion.HierarchyDeletionOrphanObjectRemove:        "orphan-object.remove",
		hierarchydeletion.HierarchyDeletionBackingRuntimeReconstruct: "backing.runtime-reconstruct",
	}[action]
}

func HierarchyDeletionControllerFinalizer(action hierarchydeletion.HierarchyDeletionActionKind) string {
	return map[hierarchydeletion.HierarchyDeletionActionKind]string{
		hierarchydeletion.HierarchyDeletionServiceRemove:   "service.remove",
		hierarchydeletion.HierarchyDeletionEntryRemove:     "entry.remove",
		hierarchydeletion.HierarchyDeletionRouteRemove:     "route.remove",
		hierarchydeletion.HierarchyDeletionComponentRemove: "component.remove",
		hierarchydeletion.HierarchyDeletionScriptRemove:    "script.remove", hierarchydeletion.HierarchyDeletionReleaseGroupRemove: "release-group.remove",
		hierarchydeletion.HierarchyDeletionReleaseFinalize: "release.finalize", hierarchydeletion.HierarchyDeletionBackupPolicyFinalize: "backup-policy.finalize",
		hierarchydeletion.HierarchyDeletionKeyMaterialRemove:  "key-material.remove",
		hierarchydeletion.HierarchyDeletionZoneRemove:         "zone.remove",
		hierarchydeletion.HierarchyDeletionReservationRelease: "reservation.release", hierarchydeletion.HierarchyDeletionConnectorFinalize: "connector.finalize",
		hierarchydeletion.HierarchyDeletionEnvironmentFinalize: "environment.finalize", hierarchydeletion.HierarchyDeletionRunnerLocalRemove: "runner.remove",
		hierarchydeletion.HierarchyDeletionProjectSecretRemove: "secret.remove", hierarchydeletion.HierarchyDeletionBackingServiceFinalize: "backing.finalize",
		hierarchydeletion.HierarchyDeletionProjectFinalize: "project.finalize", hierarchydeletion.HierarchyDeletionTenantFinalize: "tenant.finalize",
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
	record, err := groupstore.DecodeReleaseGroupStored(value)
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
