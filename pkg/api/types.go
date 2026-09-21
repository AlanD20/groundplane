// Package api holds the public REST/JSON DTOs and the resource map's
// request/response shapes — the human surface Console, CLI, and scripts
// all speak. This document is generated CODE-FIRST from the Controller's
// typed handlers (Huma or oapi-codegen) at build time and served at
// /openapi.json; the types here are the source those handlers are built
// from. See architecture.md, "API contracts (locked)", and api-cli.md
// section 4 for the exact endpoint shapes these back.
//
// These DTOs are deliberately independent of internal/core's domain
// model, not aliases of it (standards.md's import matrix: pkg/api never
// imports internal/core). The API contract and the internal domain
// model are allowed to diverge in shape; internal/controller's handlers
// translate between them.
package api

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
	Revision   int64  `json:"revision,omitempty"`
}

// --- Host / core / agents ---
