package etcd

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validateScriptExecutionSources(
	sources scriptsourcequeries.ScriptExecutionSources,
	execution scriptexecutions.ScriptExecutionRecord,
) error {
	if sources.Revision <= 0 || sources.Tenant.ReadRevision != sources.Revision ||
		sources.Project.ReadRevision != sources.Revision || sources.Environment.ReadRevision != sources.Revision ||
		sources.Service.ReadRevision != sources.Revision || sources.ScriptSet.ReadRevision != sources.Revision ||
		sources.Script.ReadRevision != sources.Revision ||
		sources.BodyGeneration.ReadRevision != sources.Revision || sources.RenderInput.ReadRevision != sources.Revision ||
		sources.DesiredHead.ReadRevision != sources.Revision || sources.DesiredHead.Revision <= 0 ||
		sources.DesiredProjection.ReadRevision != sources.Revision || sources.DesiredProjection.Revision <= 0 ||
		sources.Release.Revision != sources.Revision || sources.Script.Record.Desired.ID != execution.ScriptID ||
		sources.ScriptSet.Revision <= 0 || sources.ScriptSet.Record.EnvironmentID != execution.EnvironmentID ||
		sources.ScriptSet.Record.GenerationID != execution.ScriptSetGeneration ||
		sources.Script.Record.ScriptSetGeneration != execution.ScriptSetGeneration ||
		sources.Script.Record.ActiveGeneration != execution.ScriptGeneration ||
		sources.BodyGeneration.Record.ScriptID != execution.ScriptID ||
		sources.BodyGeneration.Record.Generation != execution.ScriptGeneration ||
		sources.Environment.Record.ID != execution.EnvironmentID || sources.Service.Record.Desired.ID != execution.ServiceID ||
		sources.Release.Intent.ID != execution.ReleaseID || sources.RenderInput.Record.ReleaseID != execution.ReleaseID ||
		sources.RenderInput.Record.Projection.RenderGeneration != execution.RenderGeneration ||
		sources.DesiredHead.Record.EnvironmentID != execution.EnvironmentID ||
		sources.DesiredHead.Record.RevisionID == "" ||
		sources.DesiredProjection.Record.EnvironmentID != execution.EnvironmentID ||
		sources.DesiredProjection.Record.RevisionID == "" ||
		sources.DesiredProjection.Record.RenderGeneration == 0 ||
		sources.BodyGeneration.Record.BodySHA256 != execution.BodySHA256 {
		return errs.New(errs.KindValidationFailed, "Script execution sources do not match the execution")
	}
	desiredZoneNames := make(map[string]string, len(sources.DesiredProjection.Record.DesiredZones))
	for _, desired := range sources.DesiredProjection.Record.DesiredZones {
		if _, duplicate := desiredZoneNames[desired.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Script execution desired zone identity is duplicated")
		}
		desiredZoneNames[desired.Desired.ID] = desired.Desired.Name
	}
	if len(sources.Networks) != len(desiredZoneNames) {
		return errs.New(errs.KindValidationFailed, "Script execution network sources are incomplete")
	}
	for _, network := range sources.Networks {
		identity, found := desiredZoneNames[network.Record.Desired.ID]
		if network.ReadRevision != sources.Revision || network.Revision != sources.DesiredProjection.Revision ||
			network.Record.EnvironmentID != execution.EnvironmentID || !found || network.Record.Desired.Name != identity {
			return errs.New(errs.KindValidationFailed, "Script execution network source revision is invalid")
		}
	}
	return scriptsourcequeries.ValidateStoredScriptContext(sources, execution)
}

func scriptExecutionProjectionConditions(sources scriptsourcequeries.ScriptExecutionSources) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{
			Key:         blueprints.EnvironmentBlueprintHeadKey(sources.Environment.Record.ID),
			ModRevision: sources.DesiredHead.Revision,
		},
		{
			Key: blueprints.EnvironmentBlueprintRootKey(
				sources.Environment.Record.ID, sources.DesiredProjection.Record.RevisionID,
			),
			ModRevision: sources.DesiredProjection.Revision,
		},
	}
	for _, network := range sources.Networks {
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), network.Record.Desired.ID),
		})
	}
	conditions = append(conditions, scriptsourcequeries.ScriptAttachSourceConditions(sources.AttachSources)...)
	return conditions
}
