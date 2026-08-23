package cli

import (
	"fmt"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
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
			environmentID, err := resolveEnvironmentTarget(cmd, fromContext(cmd).Scope.Environment)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListEntries(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, entry := range page.Items {
				items[index] = entryFields(entry)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "show <id>",
		Short: "Show Entry metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			entry, err := fromContext(cmd).Client.ShowEntry(cmd.Context(), target(fromContext(cmd), args[0]))
			if err != nil {
				return err
			}
			return renderEntry(cmd, entry)
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
		literal, valueFile  string
		secretRef           string
		factAttach, factKey string
		factGrantAttach     string
		services            []string
		all                 bool
		secret              bool
		uid, gid            uint32
	)

	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add an entry",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			literalSet := cmd.Flags().Changed("literal")
			valueFileSet := cmd.Flags().Changed("value-file")
			if literalSet && valueFileSet {
				return fmt.Errorf("--literal and --value-file are mutually exclusive")
			}
			if secret && literalSet {
				return fmt.Errorf("a secret literal must use --value-file so plaintext is not placed in argv")
			}
			if valueFileSet {
				var err error
				literal, err = readEntryValue(valueFile, cmd.InOrStdin())
				if err != nil {
					return err
				}
			}
			source, err := buildEntrySource(entrySourceOptions{
				literal:      literal,
				literalSet:   literalSet || valueFileSet,
				secretRef:    secretRef,
				secretRefSet: cmd.Flags().Changed("secret-ref"),
				factAttach:   factAttach,
				factGrant:    factGrantAttach,
				factKey:      factKey,
				factSet: cmd.Flags().Changed("fact-attach") || cmd.Flags().Changed("fact-grant-attach") ||
					cmd.Flags().Changed("fact-key"),
			})
			if err != nil {
				return err
			}
			exposure := entryExposure(services, all)
			uidValue, gidValue, err := entryFileOwnership(
				entryType,
				uid,
				gid,
				cmd.Flags().Changed("uid"),
				cmd.Flags().Changed("gid"),
			)
			if err != nil {
				return err
			}

			environmentID, err := resolveEnvironmentTarget(cmd, app.Scope.Environment)
			if err != nil {
				return err
			}
			entry, err := app.Client.CreateEntry(cmd.Context(), apiTypes.EntryCreateRequest{
				EnvironmentID: environmentID, Type: entryType, Key: key, Path: path,
				UID: uidValue, GID: gidValue, Source: source, Exposure: exposure, Secret: secret,
			})
			if err != nil {
				return err
			}
			return renderEntry(cmd, entry)
		},
	}

	cmd.Flags().StringVar(&entryType, "type", "env", "env | file")
	cmd.Flags().StringVar(&key, "key", "", "env var name (--type env)")
	cmd.Flags().StringVar(&path, "path", "", "path relative to the environment's volume folder (--type file)")
	cmd.Flags().StringVar(&literal, "literal", "", "a non-secret literal stored in desired state")
	cmd.Flags().
		StringVar(&valueFile, "value-file", "", "read a literal from PATH, or - for stdin; required for secrets")
	cmd.Flags().
		StringVar(&secretRef, "secret-ref", "", "a secret-store reference — mutually exclusive with --literal and --fact-*")
	cmd.Flags().
		StringVar(&factAttach, "fact-attach", "", "a live fact source: the attach name/id — pairs with --fact-key")
	cmd.Flags().StringVar(
		&factGrantAttach,
		"fact-grant-attach",
		"",
		"optional granted Attach id selecting an additional fact set",
	)
	cmd.Flags().
		StringVar(&factKey, "fact-key", "", "a live fact source: the fact key, e.g. pg16_URL — pairs with --fact-attach")
	cmd.Flags().StringSliceVar(&services, "service", nil, "expose to specific service(s) (repeatable); omit for --all")
	cmd.Flags().
		BoolVar(&all, "all", true, "expose to all services in the environment (default; overridden by --service)")
	cmd.Flags().BoolVar(&secret, "secret", false, "store in the encrypted secret store instead of desired state")
	cmd.Flags().Uint32Var(&uid, "uid", 0, "numeric owner for a file Entry (required with --type file)")
	cmd.Flags().Uint32Var(&gid, "gid", 0, "numeric group for a file Entry (required with --type file)")
	return cmd
}

func newEntryEditCmd() *cobra.Command {
	var (
		literal, valueFile  string
		secretRef           string
		factAttach, factKey string
		factGrantAttach     string
		services            []string
		all                 bool
	)

	cmd := &cobra.Command{
		Use:   "edit <id>",
		Short: "Edit an entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			entry, err := app.Client.ShowEntry(cmd.Context(), target(app, args[0]))
			if err != nil {
				return err
			}
			literalSet := cmd.Flags().Changed("literal")
			valueFileSet := cmd.Flags().Changed("value-file")
			if literalSet && valueFileSet {
				return fmt.Errorf("--literal and --value-file are mutually exclusive")
			}
			if entry.Secret && literalSet {
				return fmt.Errorf("a secret literal must use --value-file so plaintext is not placed in argv")
			}
			if valueFileSet {
				literal, err = readEntryValue(valueFile, cmd.InOrStdin())
				if err != nil {
					return err
				}
			}
			secretRefSet := cmd.Flags().Changed("secret-ref")
			factSet := cmd.Flags().Changed("fact-attach") || cmd.Flags().Changed("fact-grant-attach") ||
				cmd.Flags().Changed("fact-key")
			source, err := buildEntrySource(entrySourceOptions{
				literal: literal, literalSet: literalSet || valueFileSet,
				secretRef: secretRef, secretRefSet: secretRefSet,
				factAttach: factAttach, factGrant: factGrantAttach, factKey: factKey, factSet: factSet,
			})
			if err != nil {
				return err
			}
			if all && len(services) > 0 {
				return fmt.Errorf("--all and --service are mutually exclusive")
			}
			exposure := entryExposure(services, all)
			if len(exposure) == 0 {
				return fmt.Errorf("exactly one of --all or --service is required")
			}
			updated, err := app.Client.EditEntry(cmd.Context(), entry.ID, apiTypes.EntryEditRequest{
				Source: source, Exposure: exposure,
			})
			if err != nil {
				return err
			}
			return renderEntry(cmd, updated)
		},
	}

	cmd.Flags().StringVar(&literal, "literal", "", "replace the source with a literal value")
	cmd.Flags().StringVar(
		&valueFile, "value-file", "", "read a replacement literal from PATH, or - for stdin; required for secrets",
	)
	cmd.Flags().StringVar(&secretRef, "secret-ref", "", "replace the source with a secret-store reference")
	cmd.Flags().
		StringVar(&factAttach, "fact-attach", "", "replace the source with a live fact reference: attach name/id")
	cmd.Flags().StringVar(
		&factGrantAttach,
		"fact-grant-attach",
		"",
		"replace the optional granted Attach id selecting an additional fact set",
	)
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
	factGrant    string
	factKey      string
	factSet      bool
}

// buildEntrySource enforces the locked mutual exclusivity: literal,
// secret_ref, and fact are mutually exclusive (api-cli.md, section 4).
func buildEntrySource(options entrySourceOptions) (apiTypes.EntrySource, error) {
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
		return apiTypes.EntrySource{}, fmt.Errorf(
			"exactly one of --literal/--value-file, --secret-ref, or --fact-attach/--fact-key is required",
		)
	}

	switch {
	case options.literalSet:
		return apiTypes.EntrySource{Kind: "literal", Literal: options.literal}, nil
	case options.secretRefSet:
		if options.secretRef == "" {
			return apiTypes.EntrySource{}, fmt.Errorf("--secret-ref must not be empty")
		}
		return apiTypes.EntrySource{Kind: "secret_ref", SecretRef: options.secretRef}, nil
	default:
		if options.factAttach == "" || options.factKey == "" {
			return apiTypes.EntrySource{}, fmt.Errorf("--fact-attach and --fact-key must both be set")
		}
		return apiTypes.EntrySource{
			Kind: "fact", AttachID: options.factAttach, GrantAttachID: options.factGrant, Fact: options.factKey,
		}, nil
	}
}

func entryFileOwnership(
	entryType string,
	uid uint32,
	gid uint32,
	uidSet bool,
	gidSet bool,
) (*int64, *int64, error) {
	switch entryType {
	case "env":
		if uidSet || gidSet {
			return nil, nil, fmt.Errorf("--uid and --gid apply only to --type file")
		}
		return nil, nil, nil
	case "file":
		if !uidSet || !gidSet {
			return nil, nil, fmt.Errorf("--type file requires both --uid and --gid")
		}
		uidValue := int64(uid)
		gidValue := int64(gid)
		return &uidValue, &gidValue, nil
	default:
		return nil, nil, fmt.Errorf("--type must be %q or %q", "env", "file")
	}
}

func readEntryValue(path string, stdin interface{ Read([]byte) (int, error) }) (string, error) {
	return readValueFile(path, stdin, apiTypes.MaximumEntryValueBytes, "Entry")
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

func renderEntry(cmd *cobra.Command, entry apiTypes.Entry) error {
	fields := entryFields(entry)
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, entry)
}

func entryFields(entry apiTypes.Entry) map[string]any {
	return map[string]any{
		"id": entry.ID, "type": entry.Type, "key": entry.Key, "path": entry.Path,
		"uid": entry.UID, "gid": entry.GID, "source": entry.Source.Kind,
		"exposure": strings.Join(entry.Exposure, ","), "secret": entry.Secret,
	}
}
