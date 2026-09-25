package blueprintparser

import (
	"bytes"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// BuildDesiredInput freezes the complete typed operator decisions returned by
// Parse together with the normalized Compose source and the final selected
// companion files. Runtime files are explicit because reconciliation may
// preserve files from the pinned predecessor that are still referenced after a
// partial direct mutation.
func BuildDesiredInput(
	parsed Result,
	normalizedCompose []byte,
	runtimeFiles []core.BlueprintFile,
) (core.BlueprintDesiredInput, error) {
	if parsed.Project == nil || parsed.Extensions.NetworkPool == "" {
		return core.BlueprintDesiredInput{}, errs.New(
			errs.KindInternal,
			"parsed Environment Blueprint desired input is incomplete",
		)
	}
	if err := validateNormalizedDesiredCompose(normalizedCompose); err != nil {
		return core.BlueprintDesiredInput{}, err
	}
	if err := core.ValidateNormalizedBlueprintFiles(runtimeFiles); err != nil {
		return core.BlueprintDesiredInput{}, errs.Wrap(errs.KindInternal, err)
	}
	input := core.BlueprintDesiredInput{
		NormalizedCompose: normalizedCompose,
		RuntimeFiles:      runtimeFiles,
		ServiceExtensions: parsed.ServiceExtensions,
		NetworkPool:       parsed.Extensions.NetworkPool,
		Requires:          parsed.Extensions.Requires,
		Attachments:       parsed.Extensions.Attachments,
		Entries:           parsed.Extensions.Entries,
		Routes:            parsed.Extensions.Routes,
		Scripts:           parsed.Extensions.Scripts,
		Components:        parsed.Extensions.Components,
		Backup:            parsed.Extensions.Backup,
		ReleaseGroups:     parsed.Extensions.ReleaseGroups,
	}
	return core.CloneBlueprintDesiredInput(input), nil
}

func validateNormalizedDesiredCompose(value []byte) error {
	if len(value) == 0 {
		return errs.New(errs.KindInternal, "normalized Environment Compose is missing")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(value))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return errs.New(errs.KindInternal, "normalized Environment Compose is invalid")
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errs.New(errs.KindInternal, "normalized Environment Compose contains multiple documents")
	}
	return nil
}
