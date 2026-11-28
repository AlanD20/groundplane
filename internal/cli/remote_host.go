package cli

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newRemoteHostCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "remote-host", Short: "Manage remote Host enrollment"}
	cmd.AddCommand(newRemoteHostListCmd(), newRemoteHostShowCmd(), newRemoteHostCreateCmd(),
		newRemoteHostPrepareCmd(), newRemoteHostStageCmd(), newRemoteHostRevokeCmd(), newRemoteHostFetchImageCmd())
	return withExecutionClass(cmd, executionAPI)
}

func newRemoteHostListCmd() *cobra.Command {
	var limit int
	var cursor string
	cmd := &cobra.Command{Use: "list", Short: "List registered remote Hosts", Args: cobra.NoArgs}
	cmd.Flags().IntVar(&limit, "limit", 0, "limit the response page to this many Hosts")
	cmd.Flags().StringVar(&cursor, "cursor", "", "continue at the returned page cursor")
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		app := fromContext(cmd)
		page, err := app.Client.ListRemoteHosts(cmd.Context(), limit, cursor)
		if err != nil {
			return err
		}
		rows := make([][]string, 0, len(page.Items))
		for _, host := range page.Items {
			rows = append(rows, []string{host.ID, host.Name, host.State, host.AgentID, fmt.Sprint(host.Generation)})
		}
		return app.Out.Render([]string{"ID", "NAME", "STATE", "AGENT", "GENERATION"}, rows, page)
	}
	return cmd
}

func newRemoteHostShowCmd() *cobra.Command {
	return &cobra.Command{
		Use: "show <host>", Short: "Show current remote Host enrollment", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveRemoteHost(cmd, args[0])
			if err != nil {
				return err
			}
			view, err := fromContext(cmd).Client.ShowRemoteHost(cmd.Context(), id)
			return renderRemoteHost(cmd, view, err)
		},
	}
}

func newRemoteHostCreateCmd() *cobra.Command {
	var key string
	cmd := &cobra.Command{
		Use: "create <name>", Short: "Register a remote Host before enrolling its Agent", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			view, err := fromContext(cmd).Client.CreateRemoteHost(cmd.Context(), args[0], key)
			return renderRemoteHost(cmd, view, err)
		},
	}
	remoteHostKeyFlag(cmd, &key)
	return cmd
}

func newRemoteHostStageCmd() *cobra.Command {
	var csrPath, certificatePath, key string
	var generation uint64
	cmd := &cobra.Command{
		Use:   "stage-enrollment <host>",
		Short: "Approve a signed CSR and stage the Agent's public certificate",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveRemoteHost(cmd, args[0])
			if err != nil {
				return err
			}
			csr, err := readRemoteHostCSR(csrPath)
			if err != nil {
				return err
			}
			if certificatePath != "" {
				if _, err := os.Lstat(certificatePath); !errors.Is(err, os.ErrNotExist) {
					return errs.New(errs.KindValidationFailed, "Choose a new path for the staged public certificate")
				}
			}
			replayKey := key
			if replayKey == "" {
				replayKey = ids.NewULID()
			}
			app := fromContext(cmd)
			result, err := app.Client.StageRemoteHostEnrollment(cmd.Context(), id, api.RemoteHostEnrollmentRequest{
				ExpectedGeneration: generation, CSR: csr,
			}, replayKey)
			if err != nil {
				return err
			}
			if certificatePath != "" {
				if err := writeEnrollmentFile(certificatePath, []byte(result.Certificate), 0600); err != nil {
					return errs.Newf(
						errs.KindStateConflict,
						"Certificate staged but file write failed. Retry the same request with --idempotency-key %s: %v",
						replayKey,
						err,
					)
				}
			}
			fields, values := fieldsOfVia(map[string]any{
				"id": result.Host.ID, "agent_id": result.Host.AgentID, "generation": result.Host.Generation,
				"expires_at": result.ExpiresAt, "certificate": result.Certificate,
			})
			return app.Out.RenderOne(fields, values, result)
		},
	}
	cmd.Flags().StringVar(&csrPath, "csr-file", "", "PEM certificate signing request from the Agent host")
	cmd.Flags().StringVar(&certificatePath, "certificate-file", "", "new file for the staged public certificate")
	cmd.Flags().Uint64Var(&generation, "generation", 0, "Host generation to enroll or rotate, zero for a new Host")
	_ = cmd.MarkFlagRequired("csr-file")
	_ = cmd.MarkFlagRequired("generation")
	remoteHostKeyFlag(cmd, &key)
	return cmd
}

func newRemoteHostRevokeCmd() *cobra.Command {
	var generation uint64
	var key string
	cmd := &cobra.Command{
		Use: "revoke-enrollment <host>", Short: "Revoke the specified Agent generation", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveRemoteHost(cmd, args[0])
			if err != nil {
				return err
			}
			return fromContext(cmd).Client.RevokeRemoteHostEnrollment(cmd.Context(), id, generation, key)
		},
	}
	cmd.Flags().Uint64Var(&generation, "generation", 0, "exact Agent generation to revoke")
	_ = cmd.MarkFlagRequired("generation")
	remoteHostKeyFlag(cmd, &key)
	return cmd
}

func resolveRemoteHost(cmd *cobra.Command, name string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return name, ids.Validate(ids.KindHost, name)
	}
	var cursor, found string
	for {
		page, err := app.Client.ListRemoteHosts(cmd.Context(), 100, cursor)
		if err != nil {
			return "", err
		}
		for _, host := range page.Items {
			if host.Name != name {
				continue
			}
			if found != "" {
				return "", errs.New(
					errs.KindStateConflict,
					"Multiple remote Hosts use that name. Select the Host with --id",
				)
			}
			found = host.ID
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			return "", errs.New(errs.KindInternal, "Remote Host listing did not advance")
		}
		cursor = page.NextCursor
	}
	if found == "" {
		return "", errs.New(errs.KindRequestNotFound, "Remote Host was not found")
	}
	return found, nil
}

func renderRemoteHost(cmd *cobra.Command, view api.RemoteHost, err error) error {
	if err != nil {
		return err
	}
	fields, values := fieldsOfVia(map[string]any{
		"id": view.ID, "name": view.Name, "state": view.State, "agent_id": view.AgentID, "generation": view.Generation,
		"credential_expiry": view.CredentialExpiry,
	})
	return fromContext(cmd).Out.RenderOne(fields, values, view)
}

func remoteHostKeyFlag(cmd *cobra.Command, key *string) {
	cmd.Flags().StringVar(key, "idempotency-key", "", "reuse this key to retry the same request")
}

func readRemoteHostCSR(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", errs.Wrap(errs.KindValidationFailed, err)
	}
	defer file.Close()
	const limit = 24000
	body, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return "", errs.Wrap(errs.KindValidationFailed, err)
	}
	if len(body) == 0 || len(body) > limit {
		return "", errs.New(errs.KindValidationFailed, "Agent CSR is empty or too large")
	}
	return string(body), nil
}
