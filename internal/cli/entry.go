package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// entry: list | add --type env|file [entry fields] | edit | remove. The
// unified environment-entry model — env variables AND files, plain and
// secret, including live fact references, all through one noun (there
// is no separate `file` noun anymore). See blueprint.md, "x-gp-entry",
// and api-cli.md section 4's discriminated source example.
func newEntryCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "entry", Short: "Environment entries — env variables and files, plain and secret"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List entries",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, "/api/v1/entries", scopeQuery(fromContext(cmd), "environment"))
		},
	})

	cmd.AddCommand(newEntryAddCmd())
	cmd.AddCommand(newEntryEditCmd())

	cmd.AddCommand(&cobra.Command{
		Use:     "remove <id>",
		Aliases: []string{"delete"},
		Short:   "Remove an entry (dispatches a task)",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDestroy(cmd, "/api/v1/entries/"+target(fromContext(cmd), args[0]))
		},
	})

	return cmd
}

func newEntryAddCmd() *cobra.Command {
	var (
		entryType           string
		key, path           string
		literal, secretRef  string
		factAttach, factKey string
		services            []string
		all                 bool
		secret              bool
	)

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add an entry",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)

			source, err := buildEntrySource(entrySourceOptions{
				literal:      literal,
				literalSet:   cmd.Flags().Changed("literal"),
				secretRef:    secretRef,
				secretRefSet: cmd.Flags().Changed("secret-ref"),
				factAttach:   factAttach,
				factKey:      factKey,
				factSet:      cmd.Flags().Changed("fact-attach") || cmd.Flags().Changed("fact-key"),
			})
			if err != nil {
				return err
			}
			exposure := entryExposure(services, all)

			body := map[string]interface{}{
				"type": entryType, "source": source, "exposure": exposure, "secret": secret,
				"environment": app.Scope.Environment,
			}
			switch entryType {
			case "env":
				body["key"] = key
			case "file":
				body["path"] = path
			default:
				return fmt.Errorf("--type must be %q or %q", "env", "file")
			}
			return runCreate(cmd, "/api/v1/entries", body)
		},
	}

	cmd.Flags().StringVar(&entryType, "type", "env", "env | file")
	cmd.Flags().StringVar(&key, "key", "", "env var name (--type env)")
	cmd.Flags().StringVar(&path, "path", "", "path relative to the environment's volume folder (--type file)")
	cmd.Flags().StringVar(&literal, "literal", "", "a literal value (plain config, or a secret value with --secret)")
	cmd.Flags().
		StringVar(&secretRef, "secret-ref", "", "a secret-store reference — mutually exclusive with --literal and --fact-*")
	cmd.Flags().
		StringVar(&factAttach, "fact-attach", "", "a live fact source: the attach name/id — pairs with --fact-key")
	cmd.Flags().
		StringVar(&factKey, "fact-key", "", "a live fact source: the fact key, e.g. pg16_URL — pairs with --fact-attach")
	cmd.Flags().StringSliceVar(&services, "service", nil, "expose to specific service(s) (repeatable); omit for --all")
	cmd.Flags().
		BoolVar(&all, "all", true, "expose to all services in the environment (default; overridden by --service)")
	cmd.Flags().BoolVar(&secret, "secret", false, "store in the encrypted secret store instead of desired state")
	return cmd
}

func newEntryEditCmd() *cobra.Command {
	var (
		literal, secretRef  string
		factAttach, factKey string
		services            []string
		all                 bool
	)

	cmd := &cobra.Command{
		Use:   "edit <id>",
		Short: "Edit an entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			body := map[string]interface{}{}
			literalSet := cmd.Flags().Changed("literal")
			secretRefSet := cmd.Flags().Changed("secret-ref")
			factSet := cmd.Flags().Changed("fact-attach") || cmd.Flags().Changed("fact-key")
			if literalSet || secretRefSet || factSet {
				source, err := buildEntrySource(entrySourceOptions{
					literal:      literal,
					literalSet:   literalSet,
					secretRef:    secretRef,
					secretRefSet: secretRefSet,
					factAttach:   factAttach,
					factKey:      factKey,
					factSet:      factSet,
				})
				if err != nil {
					return err
				}
				body["source"] = source
			}
			if len(services) > 0 || all {
				body["exposure"] = entryExposure(services, all)
			}
			return runPatch(cmd, "/api/v1/entries/"+target(fromContext(cmd), args[0]), body)
		},
	}

	cmd.Flags().StringVar(&literal, "literal", "", "replace the source with a literal value")
	cmd.Flags().StringVar(&secretRef, "secret-ref", "", "replace the source with a secret-store reference")
	cmd.Flags().
		StringVar(&factAttach, "fact-attach", "", "replace the source with a live fact reference: attach name/id")
	cmd.Flags().StringVar(&factKey, "fact-key", "", "replace the source with a live fact reference: fact key")
	cmd.Flags().StringSliceVar(&services, "service", nil, "new exposure: specific service(s)")
	cmd.Flags().BoolVar(&all, "all", false, "new exposure: all services")
	return cmd
}

type entrySourceOptions struct {
	literal      string
	literalSet   bool
	secretRef    string
	secretRefSet bool
	factAttach   string
	factKey      string
	factSet      bool
}

// buildEntrySource enforces the locked mutual exclusivity: literal,
// secret_ref, and fact are mutually exclusive (api-cli.md, section 4).
func buildEntrySource(options entrySourceOptions) (map[string]interface{}, error) {
	set := 0
	if options.literalSet {
		set++
	}
	if options.secretRefSet {
		set++
	}
	if options.factSet {
		set++
	}
	if set != 1 {
		return nil, fmt.Errorf("exactly one of --literal, --secret-ref, or --fact-attach/--fact-key is required")
	}

	switch {
	case options.literalSet:
		return map[string]interface{}{"kind": "literal", "literal": options.literal}, nil
	case options.secretRefSet:
		if options.secretRef == "" {
			return nil, fmt.Errorf("--secret-ref must not be empty")
		}
		return map[string]interface{}{"kind": "secret_ref", "secret_ref": options.secretRef}, nil
	default:
		if options.factAttach == "" || options.factKey == "" {
			return nil, fmt.Errorf("--fact-attach and --fact-key must both be set")
		}
		return map[string]interface{}{
			"kind": "fact",
			"fact": map[string]string{"attach_id": options.factAttach, "fact": options.factKey},
		}, nil
	}
}

// entryExposure builds the API's exposure list: specific service names
// if given, otherwise the single "all" sentinel (blueprint.md's
// `exposure: [all]` convention).
func entryExposure(services []string, all bool) []string {
	if len(services) > 0 {
		return services
	}
	if all {
		return []string{"all"}
	}
	return nil
}
