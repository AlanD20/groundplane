package hostplatform

import "github.com/AlanD20/groundplane/internal/app/softwareplatform"

func (platform *Platform) NewSoftware() (*softwareplatform.Composition, error) {
	software, err := softwareplatform.New(softwareplatform.Dependencies{
		Store: platform.dependencies.Store, Tasks: platform.dependencies.Tasks, Evidence: platform.dependencies.Idempotency, Intents: platform.dependencies.Intents,
		Agents: platform.agents, AgentUpdates: platform.AgentMutations, ControllerUpdates: platform.Upgrades,
		Releases: platform.releases, WorkspaceRoot: platform.dependencies.Config.Storage.VolumeRoot,
		Interval: platform.dependencies.Tick, Logger: platform.dependencies.Logger,
	})
	if err != nil {
		return nil, err
	}
	platform.preparations = software.Preparations
	return software, nil
}
