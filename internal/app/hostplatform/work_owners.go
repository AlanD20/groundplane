package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/agentmanagement"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/releasegroup"

	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/attachwork"
	"github.com/AlanD20/groundplane/internal/controller/blueprint"
	componentexecution "github.com/AlanD20/groundplane/internal/controller/componentwork"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	"github.com/AlanD20/groundplane/internal/controller/detachwork"
	entryexecution "github.com/AlanD20/groundplane/internal/controller/entrywork"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/network"
	scriptremovalwork "github.com/AlanD20/groundplane/internal/controller/scripts"
	"github.com/AlanD20/groundplane/internal/controller/scriptwork"
	"github.com/AlanD20/groundplane/internal/controller/secrets"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/controller/serviceremoval"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// WorkExecution composes the native scheduling, operation and retention owners.
// The daemon starts one execution runtime, not parallel Task engines.
// workOwners keeps the concrete capability lifetimes together. Registration and
// channel wiring consume these same instances; there is no second execution path.
type workOwners struct {
	backupReports             *backup.AgentWork
	keyRotations              *backupkey.Work
	scripts                   *scriptwork.Service
	connections               *connectionWork
	runnerRecords             *runnerchecks.Repository
	blueprintLedger           *blueprintunits.Repository
	entryWork                 *blueprint.EntryWork
	definitionWork            *blueprint.DefinitionWork
	componentDefinitions      *blueprint.ComponentDefinitionWork
	hierarchyRecords          *etcd.HierarchyDeletionRepository
	hierarchyController       *hierarchydeletion.ControllerWork
	diagnostics               *executiondiagnostics.Repository
	agentWork                 *agentmanagement.Work
	backupRemoval             *hierarchydeletion.BackupWork
	attachRemovals            *attachplan.RemovalPlanner
	hierarchyAgent            *hierarchydeletion.AgentWork
	detaches                  *detachwork.Owner
	volumeRecords             *volumework.InputRepository
	volumeAgent               *volume.AgentWork
	volumeReports             *volume.WorkReports
	lifecycleAgent            *servicelifecycle.AgentWork
	lifecycleDecisions        *servicelifecycle.DecisionWork
	serviceRemovalAgent       *serviceremoval.AgentWork
	serviceRemovalDecision    *serviceremoval.DecisionWork
	serviceRemovalPreparation *serviceremoval.InputPreparation
	serviceRemovalReports     *serviceremoval.WorkReports
	memberReports             *workdispatch.LifecycleReportRouter
	networkRecords            *networkwork.InputRepository
	directReleases            directReleaseExecution
	networkAgent              *network.AgentWork
	networkReports            *network.WorkReports
	componentRecords          *componentwork.InputRepository
	entryRecords              *entrywork.InputRepository
	entryAgent                *entryexecution.AgentWork
	componentAgent            *componentexecution.AgentWork
	attachInputs              *attachwork.Inputs
	attachRecords             *attachprovision.InputRepository
	attachPreparation         *attachwork.Preparation
	provisionReports          *attachwork.Reports
	scriptRemoval             *scriptremovalwork.RemovalWork
	secretRemoval             *secrets.RemovalWork
	connectorRemoval          *connectors.RemovalWork
	provisioning              provisioningWork
	resolver                  resolverWork
	groupRemoval              *releasegroup.RemovalWork
}

func (platform *Platform) newWorkOwners(
	queue *workqueue.Repository,
	dependencies ExecutionDependencies,
) (*workOwners, error) {
	backupReports, keyRotations, err := platform.newBackupWork(queue, dependencies)
	if err != nil {
		return nil, err
	}
	scriptRecords, err := etcd.NewScriptRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	scripts, err := scriptwork.New(queue, scriptRecords, dependencies.ScriptArtifacts)
	if err != nil {
		return nil, err
	}
	connections, err := platform.newConnectionWork()
	if err != nil {
		return nil, err
	}
	runnerRecords, err := runnerchecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	blueprintLedger, err := blueprintunits.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	entryWork, err := blueprint.NewEntryWork(dependencies.Entries, blueprintLedger)
	if err != nil {
		return nil, err
	}
	definitionWork, err := blueprint.NewDefinitionWork(dependencies.Definitions, blueprintLedger)
	if err != nil {
		return nil, err
	}
	componentDefinitions, err := blueprint.NewComponentDefinitionWork(dependencies.Blueprints, blueprintLedger, queue)
	if err != nil {
		return nil, err
	}
	hierarchyRecords, err := etcd.NewHierarchyDeletionRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	hierarchyController, err := hierarchydeletion.NewControllerWork(hierarchyRecords)
	if err != nil {
		return nil, err
	}
	diagnostics, err := executiondiagnostics.New(platform.dependencies.Store, queue)
	if err != nil {
		return nil, err
	}
	agentWork, err := platform.newAgentWork(queue, diagnostics)
	if err != nil {
		return nil, err
	}
	backupRemoval, err := hierarchydeletion.NewBackupWork(
		hierarchyRecords,
		queue,
		dependencies.BackupRemoval,
		diagnostics,
	)
	if err != nil {
		return nil, err
	}
	attachRemovals, err := attachplan.NewRemovalPlanner(dependencies.AttachFacts)
	if err != nil {
		return nil, err
	}
	hierarchyAgent, err := hierarchydeletion.NewAgentWork(queue, hierarchyRecords, attachRemovals)
	if err != nil {
		return nil, err
	}
	detachRecords, err := attachremoval.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	detaches, err := detachwork.New(queue, detachRecords, attachRemovals)
	if err != nil {
		return nil, err
	}
	volumeRecords, err := volumework.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	volumeAgent, err := volume.NewAgentWork(volumeRecords, queue)
	if err != nil {
		return nil, err
	}
	volumeReports, err := volume.NewWorkReports(queue, volumeRecords)
	if err != nil {
		return nil, err
	}
	lifecycleRecords, err := servicelifecyclework.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	lifecycleAgent, err := servicelifecycle.NewAgentWork(lifecycleRecords, queue)
	if err != nil {
		return nil, err
	}
	lifecycleDecisions, err := servicelifecycle.NewDecisionWork(lifecycleRecords)
	if err != nil {
		return nil, err
	}
	lifecycleReports, err := servicelifecycle.NewWorkReports(queue, lifecycleRecords, dependencies.AttachFacts)
	if err != nil {
		return nil, err
	}
	serviceRemovalInputs, err := serviceremovalwork.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	serviceRemovalRecords, err := etcd.NewServiceRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	serviceRemovalAgent, err := serviceremoval.NewAgentWork(serviceRemovalInputs, queue, serviceRemovalRecords)
	if err != nil {
		return nil, err
	}
	serviceRemovalDecision, err := serviceremoval.NewDecisionWork(serviceRemovalRecords, serviceRemovalInputs, queue)
	if err != nil {
		return nil, err
	}
	serviceRemovalPreparation, err := serviceremoval.NewInputPreparation(serviceRemovalRecords)
	if err != nil {
		return nil, err
	}
	serviceRemovalReports, err := serviceremoval.NewWorkReports(
		queue,
		serviceRemovalInputs,
		serviceRemovalRecords,
		dependencies.CapturedFiles,
	)
	if err != nil {
		return nil, err
	}
	memberReports, err := workdispatch.NewLifecycleReportRouter(queue, lifecycleReports, serviceRemovalReports)
	if err != nil {
		return nil, err
	}
	networkRecords, err := networkwork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	directReleases, err := platform.newDirectReleaseWork(
		queue,
		networkRecords,
		dependencies.Releases,
		scriptRecords,
		dependencies.ScriptArtifacts,
	)
	if err != nil {
		return nil, err
	}
	networkAgent, err := network.NewAgentWork(networkRecords, queue)
	if err != nil {
		return nil, err
	}
	networkReports, err := network.NewWorkReports(queue, networkRecords)
	if err != nil {
		return nil, err
	}
	componentRecords, err := componentwork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	entryRecords, err := entrywork.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	entryAgent, err := entryexecution.NewAgentWork(entryRecords, queue)
	if err != nil {
		return nil, err
	}
	componentAgent, err := componentexecution.NewAgentWork(componentRecords, queue, blueprintLedger)
	if err != nil {
		return nil, err
	}
	attachRecords, err := attachprovision.NewInputRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	attachPlanner, err := attachplan.NewProvisionPlanner(dependencies.AttachFacts)
	if err != nil {
		return nil, err
	}
	attachInputs, err := attachwork.NewInputs(attachRecords, attachPlanner)
	if err != nil {
		return nil, err
	}
	attachPreparation, err := attachwork.NewPreparation(attachInputs, queue, blueprintLedger)
	if err != nil {
		return nil, err
	}
	provisionReports, err := attachwork.NewReports(
		queue,
		attachInputs,
		dependencies.AttachFacts,
		blueprintLedger,
		diagnostics,
	)
	if err != nil {
		return nil, err
	}
	scriptRemovalRecords, err := scriptremoval.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	scriptRemoval, err := scriptremovalwork.NewRemovalWork(scriptRemovalRecords, queue)
	if err != nil {
		return nil, err
	}
	secretRemovalRecords, err := secretremoval.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	secretRemoval, err := secrets.NewRemovalWork(secretRemovalRecords, queue)
	if err != nil {
		return nil, err
	}
	connectorRemovalRecords, err := connectorremoval.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	connectorRemoval, err := connectors.NewRemovalWork(connectorRemovalRecords, queue)
	if err != nil {
		return nil, err
	}
	provisioning, err := platform.newProvisioningWork(queue, dependencies, diagnostics)
	if err != nil {
		return nil, err
	}
	resolver, err := platform.newResolverWork(queue, diagnostics)
	if err != nil {
		return nil, err
	}
	groupRemoval, err := platform.newReleaseGroupWork(queue)
	if err != nil {
		return nil, err
	}

	return &workOwners{
		backupReports:             backupReports,
		keyRotations:              keyRotations,
		scripts:                   scripts,
		connections:               connections,
		runnerRecords:             runnerRecords,
		blueprintLedger:           blueprintLedger,
		entryWork:                 entryWork,
		definitionWork:            definitionWork,
		componentDefinitions:      componentDefinitions,
		hierarchyRecords:          hierarchyRecords,
		hierarchyController:       hierarchyController,
		diagnostics:               diagnostics,
		agentWork:                 agentWork,
		backupRemoval:             backupRemoval,
		attachRemovals:            attachRemovals,
		hierarchyAgent:            hierarchyAgent,
		detaches:                  detaches,
		volumeRecords:             volumeRecords,
		volumeAgent:               volumeAgent,
		volumeReports:             volumeReports,
		lifecycleAgent:            lifecycleAgent,
		lifecycleDecisions:        lifecycleDecisions,
		serviceRemovalAgent:       serviceRemovalAgent,
		serviceRemovalDecision:    serviceRemovalDecision,
		serviceRemovalPreparation: serviceRemovalPreparation,
		serviceRemovalReports:     serviceRemovalReports,
		memberReports:             memberReports,
		networkRecords:            networkRecords,
		directReleases:            directReleases,
		networkAgent:              networkAgent,
		networkReports:            networkReports,
		componentRecords:          componentRecords,
		entryRecords:              entryRecords,
		entryAgent:                entryAgent,
		componentAgent:            componentAgent,
		attachInputs:              attachInputs,
		attachPreparation:         attachPreparation,
		provisionReports:          provisionReports,
		attachRecords:             attachRecords,
		scriptRemoval:             scriptRemoval,
		secretRemoval:             secretRemoval,
		connectorRemoval:          connectorRemoval,
		provisioning:              provisioning,
		resolver:                  resolver,
		groupRemoval:              groupRemoval,
	}, nil
}
