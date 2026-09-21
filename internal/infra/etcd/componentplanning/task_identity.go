package componentplanning

import (
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateComponentTaskOwner(task TaskIdentity, intent environmentchanges.ComponentTaskIntent) error {
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskUpdate ||
		task.ID != intent.TaskID ||
		task.Target != intent.EnvironmentID ||
		!task.CreatedAt.Equal(intent.CreatedAt) {
		return errs.New(errs.KindStateConflict, "Component candidate does not belong to its Task")
	}
	return nil
}

func ValidateComponentTaskReservations(
	intent environmentchanges.ComponentTaskIntent,
	registries map[string]networkreservations.ComponentAddressRegistry,
) error {
	for _, candidate := range intent.Candidates {
		for _, record := range []componentrecord.Record{candidate.Current, candidate.Candidate} {
			binding, present, err := environmentchanges.ComponentTaskAddress(record)
			if err != nil {
				return err
			}
			if !present {
				continue
			}
			registry, exists := registries[binding.ZoneID()]
			if !exists || registry.Reservations[record.Desired.ID] != binding.Address() {
				return errs.New(errs.KindStateConflict, "Component address reservation changed")
			}
		}
	}
	return nil
}
