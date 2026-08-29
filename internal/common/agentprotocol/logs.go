package agentprotocol

import (
	"context"
	"io"

	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

type LogSourceSet interface {
	Run(context.Context, chan<- *agentpb.LogEvent) error
	io.Closer
}

type LogReader interface {
	Open(context.Context, *agentpb.LogSubscribe) (LogSourceSet, error)
}
