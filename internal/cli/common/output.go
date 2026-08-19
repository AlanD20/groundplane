package common

// --output TABLE|JSON|YAML — shared, so the CLI is the only place
// output format is decided. Table is the default (go-pretty); JSON/YAML
// are for scripts. See api-cli.md, section 1.

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"gopkg.in/yaml.v3"
)

type Format string

const (
	FormatTable Format = "TABLE"
	FormatJSON  Format = "JSON"
	FormatYAML  Format = "YAML"
)

func ParseFormat(s string) (Format, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "", string(FormatTable):
		return FormatTable, nil
	case string(FormatJSON):
		return FormatJSON, nil
	case string(FormatYAML):
		return FormatYAML, nil
	default:
		return "", fmt.Errorf("output: unknown format %q (want TABLE, JSON, or YAML)", s)
	}
}

// Writer renders CLI results in the operator's chosen format.
type Writer struct {
	Format  Format
	NoColor bool
	Out     io.Writer
}

func NewWriter(format Format, noColor bool, out io.Writer) *Writer {
	return &Writer{Format: format, NoColor: noColor, Out: out}
}

// Render writes headers+rows as a table (headers always shown, even for
// an empty result set — see docs/standards.md, "Table commands
// always show headers, even when empty"), or marshals v directly for
// JSON/YAML.
func (w *Writer) Render(headers []string, rows [][]string, v any) error {
	switch w.Format {
	case FormatJSON:
		enc := json.NewEncoder(w.Out)
		enc.SetIndent("", "  ")
		return enc.Encode(v)

	case FormatYAML:
		b, err := yaml.Marshal(v)
		if err != nil {
			return fmt.Errorf("output: marshal yaml: %w", err)
		}
		_, err = w.Out.Write(b)
		return err

	default: // FormatTable
		t := table.NewWriter()
		t.SetOutputMirror(w.Out)
		if w.NoColor {
			t.SetStyle(table.StyleDefault)
		} else {
			t.SetStyle(table.StyleRounded)
		}
		hdr := make(table.Row, len(headers))
		for i, h := range headers {
			hdr[i] = h
		}
		t.AppendHeader(hdr) // always shown, even when rows is empty
		for _, r := range rows {
			row := make(table.Row, len(r))
			for i, c := range r {
				row[i] = c
			}
			t.AppendRow(row)
		}
		t.Render()
		return nil
	}
}

// RenderOne is Render for a single-entity `show` command, where a table
// doesn't fit — TABLE mode falls back to a two-column field/value table.
func (w *Writer) RenderOne(fields []string, values []string, v any) error {
	if w.Format != FormatTable {
		return w.Render(nil, nil, v)
	}
	rows := make([][]string, len(fields))
	for i := range fields {
		rows[i] = []string{fields[i], values[i]}
	}
	return w.Render([]string{"FIELD", "VALUE"}, rows, v)
}

// Tabulate renders a generic decoded-JSON []map[string]any as a table,
// picking common fields (id, slug, name, status, kind) when present.
// The map[string]any here is the same stdlib-driven exception as
// json.Decoder.Decode — this package decodes arbitrary API responses
// without a generated client yet (see pkg/api's code-first TODO).
func Tabulate(items []map[string]any) ([]string, [][]string) {
	preferred := []string{"id", "slug", "name", "status", "kind", "online"}
	seen := map[string]bool{}
	for _, it := range items {
		for _, p := range preferred {
			if _, ok := it[p]; ok {
				seen[p] = true
			}
		}
	}
	var headers, keys []string
	for _, p := range preferred {
		if seen[p] {
			headers = append(headers, strings.ToUpper(p))
			keys = append(keys, p)
		}
	}
	if len(headers) == 0 {
		headers = []string{"VALUE"}
	}

	rows := make([][]string, 0, len(items))
	for _, it := range items {
		row := make([]string, len(headers))
		for i, k := range keys {
			row[i] = fmt.Sprint(it[k])
		}
		rows = append(rows, row)
	}
	return headers, rows
}

// FieldsOf flattens a single decoded item into sorted field/value pairs
// for RenderOne.
func FieldsOf(item map[string]any) ([]string, []string) {
	keys := make([]string, 0, len(item))
	for k := range item {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]string, len(keys))
	for i, k := range keys {
		values[i] = fmt.Sprint(item[k])
	}
	return keys, values
}
