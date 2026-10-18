package componentruntime

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ConfigurationSource interface {
	DescribeComponent(context.Context, componentexecution.HelperInput) (*agentpb.WorkConfigurationReference, []entrymaterialization.Header, error)
	OpenComponentFile(context.Context, componentexecution.HelperInput, entrymaterialization.Header) (io.ReadCloser, error)
}

type Configuration struct {
	runtime *materialization.Runtime
	source  ConfigurationSource
}

func NewConfiguration(runtime *materialization.Runtime, source ConfigurationSource) (*Configuration, error) {
	if runtime == nil || source == nil {
		return nil, errs.New(errs.KindInternal, "Component configuration requires materializer and captured sources")
	}
	return &Configuration{runtime: runtime, source: source}, nil
}

func (configuration *Configuration) Run(ctx context.Context, input componentexecution.HelperInput, inspectOnly bool) (bool, error) {
	if configuration == nil || ctx == nil || input.Procedure == nil {
		return false, invalidInput()
	}
	reference, files, err := configuration.source.DescribeComponent(ctx, input)
	if err != nil {
		return false, err
	}
	expected, _, _, selectionErr := componentexecution.ConfigurationSelection(input)
	if selectionErr != nil || !proto.Equal(reference, expected) || componentexecution.ValidateConfigurationFiles(input, files) != nil {
		return false, errs.New(errs.KindStateConflict, "Component configuration response differs from the captured file set")
	}
	if input.Recovery != nil {
		if err := configuration.runtime.StopComponentFiles(ctx, input, files); err != nil {
			return false, err
		}
	}
	if !inspectOnly {
		for index := range files {
			if _, err := configuration.file(ctx, input, files, index, false); err != nil {
				return false, err
			}
		}
	}
	ready := true
	for index := range files {
		matches, err := configuration.file(ctx, input, files, index, true)
		if err != nil {
			return false, err
		}
		ready = ready && matches
	}
	if !inspectOnly && !ready {
		return false, errs.New(errs.KindStateConflict, "Component configuration changed before all written files could be verified")
	}
	return ready, nil
}

func (configuration *Configuration) file(ctx context.Context, input componentexecution.HelperInput, files []entrymaterialization.Header, index int, inspectOnly bool) (bool, error) {
	source, err := configuration.source.OpenComponentFile(ctx, input, files[index])
	if err != nil {
		if source != nil {
			return false, errors.Join(err, source.Close())
		}
		return false, err
	}
	err = configuration.runtime.RunComponentFile(ctx, input, files, index, source, inspectOnly)
	if inspectOnly && errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		return false, nil
	}
	return err == nil, err
}
