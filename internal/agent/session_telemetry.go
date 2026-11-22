package agent

import (
	"context"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func deliverTelemetryEvent(ctx context.Context, stream agentStream, event sessionEvent,
	observations *observationSession, monitoring *monitoringSession) error {
	if event.kind == sessionMonitoring {
		if err := monitoring.Close(); err != nil {
			return err
		}
		return sendMonitoringSamples(ctx, stream, event.monitoring)
	}
	if err := observations.Close(); err != nil {
		return err
	}
	return sendServiceObservation(ctx, stream, event.observation)
}

func handleTelemetryRead(ctx context.Context, stream agentStream, message *agentpb.ControllerMessage,
	observations *observationSession, monitoring *monitoringSession) (bool, error) {
	if handled, err := monitoring.handle(ctx, stream, message); handled {
		return true, err
	}
	return observations.handle(ctx, stream, message)
}
