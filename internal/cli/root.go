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

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	"github.com/AlanD20/groundplane/internal/common/config"
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

// ControllerRunner starts the local Controller process and blocks until it
// exits. It is injected by cmd/groundplane so constructing the command tree
// never initializes Controller infrastructure.
type ControllerRunner func(context.Context) error

// Dependencies are the local process operations available to the CLI. Human
// API commands continue to construct their REST client in PersistentPreRunE.
type Dependencies struct {
	RunController ControllerRunner
}

type executionClass string

const (
	executionAnnotation = "groundplane.io/execution-class"
	executionAPI        = executionClass("api")
	executionLocal      = executionClass("local")
	executionTool       = executionClass("tool")
)

type appKey struct{}

func fromContext(cmd *cobra.Command) *App {
	return cmd.Context().Value(appKey{}).(*App)
}

type rootFlags struct {
	config  string
	host    string
	tenant  string
	project string
	env     string
	output  string
	noColor bool
	asID    bool
}

// NewRootCmd builds the full command tree from api-cli.md, section 3,
// verbatim: flat nouns, verbs last, scope resolved once for API commands.
func NewRootCmd(deps Dependencies) *cobra.Command {
	flags := rootFlags{}
	root := &cobra.Command{
		Use:           "groundplane",
		Short:         "Run many projects on one machine without hand-maintaining Docker Compose",
		Long:          "groundplane [global flags] <resource> <action> [target...] [flags]\n\nSee api-cli.md for the full resource model and command tree.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.CompletionOptions.DisableDefaultCmd = true

	root.PersistentFlags().StringVarP(&flags.config, "config", "c", defaultConfigPath(), "config file")
	root.PersistentFlags().StringVar(&flags.host, "host", "", "controller address (default http://127.0.0.1:8080)")
	root.PersistentFlags().StringVarP(&flags.tenant, "tenant", "t", "", "scope: tenant slug")
	root.PersistentFlags().StringVarP(&flags.project, "project", "p", "", "scope: project slug")
	root.PersistentFlags().StringVarP(&flags.env, "env", "e", "", "scope: environment slug")
	root.PersistentFlags().StringVarP(&flags.output, "output", "o", "", "output format: TABLE|JSON|YAML (default TABLE)")
	root.PersistentFlags().BoolVar(&flags.noColor, "no-color", false, "plain output")
	root.PersistentFlags().BoolVar(&flags.asID, "id", false, "treat targets as ids instead of slugs (ids never change; slugs can be renamed)")

	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		cfg := config.DefaultCLIConfig()
		switch commandExecutionClass(cmd) {
		case executionTool:
			return nil
		case executionLocal:
			format, err := clicommon.ParseFormat(flags.output)
			if err != nil {
				return err
			}
			app := &App{Out: clicommon.NewWriter(format, flags.noColor, cmd.OutOrStdout())}
			cmd.SetContext(context.WithValue(cmd.Context(), appKey{}, app))
			return nil
		case executionAPI:
			if err := config.Load(cmd.Context(), flags.config, &cfg); err != nil {
				return err
			}
		}
		// Merge order everywhere: built-in defaults < config file < env vars < flags.
		host := firstNonEmpty(flags.host, os.Getenv("GROUNDPLANE_HOST"), cfg.Host)
		tenant := firstNonEmpty(flags.tenant, cfg.Tenant)
		project := firstNonEmpty(flags.project, cfg.Project)
		env := firstNonEmpty(flags.env, cfg.Environment)
		outFmt := firstNonEmpty(flags.output, cfg.Output)
		noColor := flags.noColor || cfg.NoColor
		cfg.Host = host
		cfg.Tenant = tenant
		cfg.Project = project
		cfg.Environment = env
		cfg.Output = outFmt
		cfg.NoColor = noColor
		if err := cfg.Validate(); err != nil {
			return err
		}

		format, err := clicommon.ParseFormat(outFmt)
		if err != nil {
			return err
		}

		app := &App{
			Client: apiclient.New(host),
			Out:    clicommon.NewWriter(format, noColor, cmd.OutOrStdout()),
			Scope: Scope{
				Tenant:      tenant,
				Project:     project,
				Environment: env,
				AsID:        flags.asID,
			},
		}
		cmd.SetContext(context.WithValue(cmd.Context(), appKey{}, app))
		return nil
	}

	addCommands(root, deps)
	return root
}

// Execute is called from cmd/groundplane/main.go (via internal/app). It
// runs the root command and returns a process exit code — errors are
// routed through internal/cli/common.HandleErrors exactly once, so
// cmd/groundplane never needs to import pkg/errs itself.
func Execute(ctx context.Context, deps Dependencies) int {
	root := NewRootCmd(deps)
	root.SetContext(ctx)
	return clicommon.HandleErrors(root.Execute())
}

func withExecutionClass(cmd *cobra.Command, class executionClass) *cobra.Command {
	if cmd.Annotations == nil {
		cmd.Annotations = make(map[string]string)
	}
	cmd.Annotations[executionAnnotation] = string(class)
	return cmd
}

func commandExecutionClass(cmd *cobra.Command) executionClass {
	for current := cmd; current != nil; current = current.Parent() {
		if class, ok := current.Annotations[executionAnnotation]; ok {
			return executionClass(class)
		}
	}
	return executionAPI
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
