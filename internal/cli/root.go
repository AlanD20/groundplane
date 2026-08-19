// Package cli implements the CLI surface: Cobra commands, one file per
// noun, ZERO logic — every RunE builds a path/body and delegates to the
// human API via apiclient. Root owns the global flags and scope
// resolution; resource.go holds the shared plumbing every noun file
// calls into. See api-cli.md, sections 1 and 3.
package cli

import (
	"context"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/sample-tenant/groundplane/internal/cli/apiclient"
	clicommon "github.com/sample-tenant/groundplane/internal/cli/common"
	"github.com/sample-tenant/groundplane/internal/common/config"
)

// Scope carries the resolved tenant/project/environment slugs (or ids,
// with --id) — resolved ONCE in PersistentPreRunE, never repeated per
// command. See api-cli.md, "Scope is resolved once."
type Scope struct {
	Tenant      string
	Project     string
	Environment string
	AsID        bool
}

// App is threaded through every command via the cobra context.
type App struct {
	Client *apiclient.Client
	Out    *clicommon.Writer
	Scope  Scope
}

type appKey struct{}

func fromContext(cmd *cobra.Command) *App {
	return cmd.Context().Value(appKey{}).(*App)
}

var (
	flagConfig  string
	flagHost    string
	flagTenant  string
	flagProject string
	flagEnv     string
	flagOutput  string
	flagNoColor bool
	flagAsID    bool
)

// NewRootCmd builds the full command tree from api-cli.md, section 3,
// verbatim: flat nouns, verbs last, scope resolved once.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "groundplane",
		Short:         "Run many projects on one machine without hand-maintaining Docker Compose",
		Long:          "groundplane [global flags] <resource> <action> [target...] [flags]\n\nSee api-cli.md for the full resource model and command tree.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVarP(&flagConfig, "config", "c", defaultConfigPath(), "config file")
	root.PersistentFlags().StringVar(&flagHost, "host", "", "controller address (default http://127.0.0.1:8080)")
	root.PersistentFlags().StringVarP(&flagTenant, "tenant", "t", "", "scope: tenant slug")
	root.PersistentFlags().StringVarP(&flagProject, "project", "p", "", "scope: project slug")
	root.PersistentFlags().StringVarP(&flagEnv, "env", "e", "", "scope: environment slug")
	root.PersistentFlags().StringVarP(&flagOutput, "output", "o", "", "output format: TABLE|JSON|YAML (default TABLE)")
	root.PersistentFlags().BoolVar(&flagNoColor, "no-color", false, "plain output")
	root.PersistentFlags().BoolVar(&flagAsID, "id", false, "treat targets as ids instead of slugs (ids never change; slugs can be renamed)")

	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		cfg := config.DefaultCLIConfig()
		if err := config.Load(cmd.Context(), flagConfig, &cfg); err != nil {
			return err
		}

		// Merge order everywhere: built-in defaults < config file < env vars < flags.
		host := firstNonEmpty(flagHost, os.Getenv("GROUNDPLANE_HOST"), cfg.Host)
		tenant := firstNonEmpty(flagTenant, cfg.Tenant)
		project := firstNonEmpty(flagProject, cfg.Project)
		env := firstNonEmpty(flagEnv, cfg.Environment)
		outFmt := firstNonEmpty(flagOutput, cfg.Output)

		format, err := clicommon.ParseFormat(outFmt)
		if err != nil {
			return err
		}

		app := &App{
			Client: apiclient.New(host),
			Out:    clicommon.NewWriter(format, flagNoColor, cmd.OutOrStdout()),
			Scope: Scope{
				Tenant:      tenant,
				Project:     project,
				Environment: env,
				AsID:        flagAsID,
			},
		}
		cmd.SetContext(context.WithValue(cmd.Context(), appKey{}, app))
		return nil
	}

	addCommands(root)
	return root
}

// Execute is called from cmd/groundplane/main.go (via internal/app). It
// runs the root command and returns a process exit code — errors are
// routed through internal/cli/common.HandleErrors exactly once, so
// cmd/groundplane never needs to import pkg/errs itself.
func Execute(ctx context.Context) int {
	root := NewRootCmd()
	root.SetContext(ctx)
	return clicommon.HandleErrors(root.Execute())
}

func defaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "groundplane", "config.yaml")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
