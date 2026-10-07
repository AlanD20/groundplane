package cli

import (
	"encoding/json"
	"io"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func loadServiceAliases(cmd *cobra.Command, path string) (*map[string][]string, error) {
	content, err := readValueFile(path, cmd.InOrStdin(), 256*1024, "Service aliases")
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var aliases map[string][]string
	if err := decoder.Decode(&aliases); err != nil || aliases == nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"aliases file must contain a JSON object keyed by Zone name; use {} to clear",
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errs.New(errs.KindValidationFailed, "aliases file must contain only one JSON object")
	}
	return &aliases, nil
}

func loadServiceDependencies(
	cmd *cobra.Command,
	path string,
) (*map[string]apiTypes.ServiceDependency, error) {
	content, err := readValueFile(path, cmd.InOrStdin(), 256*1024, "Service dependencies")
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var dependencies map[string]apiTypes.ServiceDependency
	if err := decoder.Decode(&dependencies); err != nil || dependencies == nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"dependencies file must contain a JSON object keyed by Service name; use {} to clear",
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errs.New(errs.KindValidationFailed, "dependencies file must contain only one JSON object")
	}
	return &dependencies, nil
}
