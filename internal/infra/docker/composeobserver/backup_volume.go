package composeobserver

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (observer *Observer) ObserveBackupConsumer(ctx context.Context, request *agentpb.BackupConsumerHelperRequest) (*agentpb.ObservedProject, error) {
	if observer == nil || observer.engine == nil || observer.now == nil {
		return nil, errs.New(errs.KindInternal, "Backup Volume observer is not configured")
	}
	input, err := executionplan.OpenBackupConsumerHelper(ctx, request)
	if err != nil {
		return nil, err
	}
	observed, err := observer.observeArtifact(ctx, input.Consumer.Artifact, nil, true)
	if err != nil {
		return nil, err
	}
	selected := proto.CloneOf(observed)
	selected.Containers = nil
	for _, container := range observed.GetContainers() {
		if container.ServiceId != input.Consumer.Fact.ServiceId {
			continue
		}
		proxy := false
		for _, member := range input.Consumer.Artifact.Services {
			if member.ServiceId != container.ServiceId || member.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				continue
			}
			if len(member.ExpectedLabels) != len(container.Labels) {
				continue
			}
			matches := true
			for index := range member.ExpectedLabels {
				if !proto.Equal(member.ExpectedLabels[index], container.Labels[index]) {
					matches = false
					break
				}
			}
			if matches {
				proxy = true
				break
			}
		}
		if !proxy {
			selected.Containers = append(selected.Containers, proto.CloneOf(container))
		}
	}
	if _, err := executionplan.BackupConsumerObservation(input.Consumer, selected); err != nil {
		return nil, err
	}
	return selected, nil
}
