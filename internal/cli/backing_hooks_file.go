package cli

import (
	"encoding/json"
	"io"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// loadBackingHooks reads operator-authored configuration, not executable output
// or secret plaintext. Use secret_ref for credentials in the existing store.
func loadBackingHooks(cmd *cobra.Command, path string) (*apiTypes.BackingHookConfiguration, error) {
	content, err := readValueFile(path, cmd.InOrStdin(), 256*1024, "Backing hooks")
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var hooks *apiTypes.BackingHookConfiguration
	if err := decoder.Decode(&hooks); err != nil || hooks == nil {
		return nil, errs.New(errs.KindValidationFailed, "backing hooks file must contain one configuration object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errs.New(errs.KindValidationFailed, "backing hooks file must contain only one JSON object")
	}
	return hooks, nil
}
