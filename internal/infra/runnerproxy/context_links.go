package runnerproxy

import (
	"archive/tar"
	"path"
	"strings"
	"unicode/utf8"
)

type contextEntry struct {
	kind byte
	link string
}

func contextPath(value string) (string, error) {
	if !validLinkText(value) || !utf8.ValidString(value) {
		return "", invalidContext()
	}
	parts := strings.Split(value, "/")
	if len(parts) > 128 {
		return "", invalidContext()
	}
	for _, part := range parts {
		if part == ".." {
			return "", invalidContext()
		}
	}
	return path.Clean(value), nil
}

func validateContextLinks(entries map[string]contextEntry) error {
	for name, entry := range entries {
		// No archive entry may be extracted through another entry's link or file.
		// Check after reading all headers so reversing their order cannot bypass it.
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if ancestor, exists := entries[parent]; exists && ancestor.kind != tar.TypeDir {
				return invalidContext()
			}
		}
		switch entry.kind {
		case tar.TypeLink:
			target, err := contextPath(entry.link)
			if err != nil {
				return err
			}
			linked, found := entries[target]
			if !found || linked.kind != tar.TypeReg {
				return invalidContext()
			}
		case tar.TypeSymlink:
			if err := resolveContextLink(entries, name); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolveContextLink(entries map[string]contextEntry, name string) error {
	remaining := strings.Split(name, "/")
	resolved := make([]string, 0, len(remaining))
	expansions := 0
	for len(remaining) != 0 {
		part := remaining[0]
		remaining = remaining[1:]
		switch part {
		case "", ".":
			continue
		case "..":
			if len(resolved) == 0 {
				return invalidContext()
			}
			resolved = resolved[:len(resolved)-1]
			continue
		}
		resolved = append(resolved, part)
		entry, exists := entries[strings.Join(resolved, "/")]
		if exists && entry.kind == tar.TypeSymlink {
			expansions++
			if expansions > 64 {
				return invalidContext()
			}
			resolved = resolved[:len(resolved)-1]
			remaining = append(strings.Split(entry.link, "/"), remaining...)
		}
		if len(remaining)+len(resolved) > 128 {
			return invalidContext()
		}
	}
	return nil
}
