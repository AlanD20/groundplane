package cli

import (
	"os"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func newContainerTerminalCmd(resolve func(*cobra.Command, string) (string, error)) *cobra.Command {
	var container, user, directory string
	cmd := &cobra.Command{
		Use: "exec <name> [-- command [args...]]", Short: "Open an interactive shell in an exact running container",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			stdin, ok := cmd.InOrStdin().(*os.File)
			if !ok || !term.IsTerminal(int(stdin.Fd())) {
				return errs.New(errs.KindValidationFailed, "container shell requires an interactive TTY")
			}
			serviceID, err := resolve(cmd, args[0])
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			service, err := app.Client.GetService(cmd.Context(), serviceID)
			if err != nil {
				return err
			}
			if container == "" {
				if service.Observation != nil {
					for _, item := range service.Observation.Containers {
						if item.State != "running" {
							continue
						}
						if container != "" {
							return errs.New(
								errs.KindValidationFailed,
								"multiple containers are running; choose an exact ID with --container",
							)
						}
						container = item.ID
					}
				}
				if container == "" {
					return errs.New(errs.KindStateConflict, "no running container is available; inspect Service status")
				}
			}
			command := args[1:]
			if len(command) == 0 {
				command = []string{"/bin/sh"}
			}
			columns, rows, err := term.GetSize(int(stdin.Fd()))
			if err != nil {
				return errs.Wrap(errs.KindRequestFailed, err)
			}
			session, err := app.Client.OpenContainerTerminal(cmd.Context(), serviceID, apiTypes.ContainerTerminalOpen{
				ContainerID: container, User: user, Command: command, WorkingDirectory: directory, Columns: columns, Rows: rows,
			})
			if err != nil {
				return err
			}
			conn, err := app.Client.DialTerminal(cmd.Context(), session, 0)
			if err != nil {
				return err
			}
			defer conn.CloseNow()
			state, err := term.MakeRaw(int(stdin.Fd()))
			if err != nil {
				return errs.Wrap(errs.KindRequestFailed, err)
			}
			defer term.Restore(int(stdin.Fd()), state)
			return runInteractiveTerminal(cmd.Context(), stdin, cmd.OutOrStdout(), conn)
		},
	}
	cmd.Flags().
		StringVar(&container, "container", "", "exact running container ID; required when more than one is running")
	cmd.Flags().StringVar(&user, "user", "root", "container user or UID[:GID]")
	cmd.Flags().StringVar(&directory, "workdir", "", "container working directory; empty uses its configured default")
	return cmd
}
