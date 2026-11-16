package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newServiceAttachCmd() *cobra.Command {
	var attachAfter []string
	var attachName string
	var grants []string
	var newCredential bool
	var credentialAttach string
	attach := &cobra.Command{
		Use:   "attach <name> <backing>",
		Short: "Attach a backing service to this service (provisions its own database + role)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			after, err := validatedTaskAfter(attachAfter)
			if err != nil {
				return err
			}
			if newCredential == (credentialAttach != "") {
				return errs.New(errs.KindValidationFailed, "choose exactly one of --new-credential or --credential")
			}
			if credentialAttach != "" && len(grants) > 0 {
				return errs.New(errs.KindValidationFailed, "--grant requires --new-credential")
			}
			serviceID, err := resolveServiceTarget(cmd, args[0])
			if err != nil {
				return err
			}
			backingServiceID, err := resolveBackingAdapterServiceTarget(cmd, args[1])
			if err != nil {
				return err
			}
			credential := apiTypes.AttachCredential{Mode: apiTypes.AttachCredentialNew}
			if credentialAttach != "" {
				credentialID, resolveErr := resolveAttachTarget(cmd, credentialAttach)
				if resolveErr != nil {
					return resolveErr
				}
				credential = apiTypes.AttachCredential{Mode: apiTypes.AttachCredentialExisting, AttachID: credentialID}
			}
			grantAttachIDs := make([]string, len(grants))
			for index, grant := range grants {
				grantAttachIDs[index], err = resolveAttachTarget(cmd, grant)
				if err != nil {
					return err
				}
			}
			accepted, err := fromContext(cmd).Client.CreateAttach(cmd.Context(), apiTypes.AttachRequest{
				After:            after,
				ServiceID:        serviceID,
				BackingServiceID: backingServiceID,
				Name:             attachName,
				Credential:       credential,
				GrantAttachIDs:   grantAttachIDs,
			})
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	attach.Flags().
		StringVar(&attachName, "name", "", "attach name (default: Controller-suggested, unique within the environment)")
	attach.Flags().BoolVar(&newCredential, "new-credential", false, "provision a new backing-service credential")
	attach.Flags().
		StringVar(&credentialAttach, "credential", "", "reuse the credential owned by an existing attach id/name")
	attach.Flags().StringSliceVar(&grants, "grant", nil, "other attach id/name(s) this attach's role may also access")
	addTaskAfterFlag(attach, &attachAfter)
	return attach

}

func newServiceDetachCmd() *cobra.Command {

	var detachAfter []string
	detach := &cobra.Command{
		Use:   "detach <attach>",
		Short: "Detach backing access and run its configured cleanup",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			after, err := validatedTaskAfter(detachAfter)
			if err != nil {
				return err
			}
			id, err := resolveAttachTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.DetachAttach(cmd.Context(), id, after)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	addTaskAfterFlag(detach, &detachAfter)
	return detach

}
