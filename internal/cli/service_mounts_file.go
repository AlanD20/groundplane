package cli

import (
	"encoding/json"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
	"io"
	"strings"
)

func loadServiceVolumeMounts(cmd *cobra.Command, path string) (*[]apiTypes.ServiceVolumeMount, error) {
	content, err := readValueFile(path, cmd.InOrStdin(), 256*1024, "Service mounts")
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(content))
	decoder.DisallowUnknownFields()
	var mounts []apiTypes.ServiceVolumeMount
	if err := decoder.Decode(&mounts); err != nil || mounts == nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"mounts file must contain a JSON array; use [] to unmount all Volumes",
		)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errs.New(errs.KindValidationFailed, "mounts file must contain only one JSON array")
	}
	return &mounts, nil
}
