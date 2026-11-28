package remoteagentplatform

import (
	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/agent/remotefetchruntime"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
)

func ConfigureImageFetch(client *agent.Client, journal remotefetchruntime.Journal) error {
	executor, err := remotefetchruntime.New(journal, registryimages.Local{})
	if err != nil {
		return err
	}
	return client.ConfigureRemoteImage(executor)
}
