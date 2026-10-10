package cli

import (
	"encoding/json"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
	"io"
	"strings"
)

func loadServiceHealthcheck(cmd *cobra.Command, path string) (apiTypes.ServiceHealthcheck, error) {
	content, err := readValueFile(path, cmd.InOrStdin(), 32*1024, "Service healthcheck")
	if err != nil {
		return apiTypes.ServiceHealthcheck{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var value *apiTypes.ServiceHealthcheck
	if err := decoder.Decode(&value); err != nil || value == nil {
		return apiTypes.ServiceHealthcheck{}, errs.New(
			errs.KindValidationFailed,
			"healthcheck file must contain a native Compose healthcheck JSON object; use {\"test\":[\"NONE\"]} to disable",
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return apiTypes.ServiceHealthcheck{}, errs.New(
			errs.KindValidationFailed,
			"healthcheck file must contain one JSON object",
		)
	}
	return *value, nil
}
