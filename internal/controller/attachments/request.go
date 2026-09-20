package attachments

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func normalizeAttachRequest(request apiTypes.AttachRequest) (apiTypes.AttachRequest, error) {
	request.GrantAttachIDs = append([]string(nil), request.GrantAttachIDs...)
	if ids.Validate(ids.KindService, request.ServiceID) != nil ||
		ids.Validate(ids.KindService, request.BackingServiceID) != nil {
		return apiTypes.AttachRequest{}, errs.New(
			errs.KindValidationFailed,
			"Attach requires exactly one valid consumer Service and one valid backing Service",
		)
	}
	switch request.Credential.Mode {
	case apiTypes.AttachCredentialNew:
		if request.Credential.AttachID != "" {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"New Attach credential cannot reference another Attach",
			)
		}
	case apiTypes.AttachCredentialExisting:
		if ids.Validate(ids.KindAttach, request.Credential.AttachID) != nil {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"Existing Attach credential requires a valid owner Attach id",
			)
		}
		if len(request.GrantAttachIDs) != 0 {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"Existing Attach credential cannot declare grants",
			)
		}
	default:
		return apiTypes.AttachRequest{}, errs.New(
			errs.KindValidationFailed,
			"Attach credential mode must be new or existing",
		)
	}
	if request.Name != "" {
		if err := attachrecord.ValidateAttachName(request.Name); err != nil {
			return apiTypes.AttachRequest{}, err
		}
	}
	if len(request.GrantAttachIDs) > attachrecord.MaximumAttachGrants {
		return apiTypes.AttachRequest{}, errs.Newf(
			errs.KindValidationFailed,
			"Attach may have at most %d grants",
			attachrecord.MaximumAttachGrants,
		)
	}
	slices.Sort(request.GrantAttachIDs)
	previous := ""
	for _, grantID := range request.GrantAttachIDs {
		if ids.Validate(ids.KindAttach, grantID) != nil || grantID == previous {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"Attach grant ids must be valid and unique",
			)
		}
		previous = grantID
	}
	return request, nil
}
