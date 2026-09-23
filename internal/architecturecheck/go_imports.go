package architecturecheck

import (
	"go/ast"
	"strconv"
	"strings"
)

// checkGoImports owns dependency diagnostics for a parsed file. The AST driver
// does not need to distinguish forbidden language, layer, and Component imports.
func checkGoImports(unit *parsedGoFile, module string) []Finding {
	findings := make([]Finding, 0)
	for _, importSpec := range unit.ast.Imports {
		importPath, err := strconv.Unquote(importSpec.Path.Value)
		if err != nil {
			continue
		}
		position := unit.fset.Position(importSpec.Pos())
		add := func(rule, subject, message string) {
			findings = append(findings, Finding{
				Path: unit.file.rel, Line: position.Line, Column: position.Column,
				Rule: rule, Subject: subject, Message: message,
			})
		}
		if importPath == "unsafe" {
			add("unsafe-import", "", "unsafe imports are forbidden")
		}
		if importPath == "reflect" && !unit.file.isTest &&
			!isHumaSchemaTypeRegistration(unit, importSpec.Name) {
			add("reflect-import", "reflect", "reflection is a forbidden conversion dependency")
		}
		if subject, reason := forbiddenLayerImport(unit.file.rel, importPath, module); reason != "" {
			add("layer-import", subject, reason)
		}
		if subject, reason := forbiddenComponentModuleImport(unit.file.rel, importPath); reason != "" {
			add("component-module-import", subject, reason)
		}
	}
	return findings
}

// Huma's registry accepts reflect.Type rather than a generic type parameter.
// Permit only direct reflect.TypeFor calls passed as the first Schema argument
// in the HTTP adapter; ordinary reflection remains forbidden there and elsewhere.
func isHumaSchemaTypeRegistration(unit *parsedGoFile, alias *ast.Ident) bool {
	if alias != nil || !strings.HasPrefix(unit.file.rel, "internal/controller/handlers/") ||
		unit.ast.Name.Name != "handlers" {
		return false
	}
	typeForCalls := 0
	schemaCalls := 0
	valid := true
	ast.Inspect(unit.ast, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			packageName, ok := value.X.(*ast.Ident)
			if !ok || packageName.Name != "reflect" {
				break
			}
			if value.Sel.Name != "TypeFor" {
				valid = false
			} else {
				typeForCalls++
			}
		case *ast.CallExpr:
			method, ok := value.Fun.(*ast.SelectorExpr)
			if !ok || method.Sel.Name != "Schema" || len(value.Args) == 0 {
				break
			}
			typeCall, ok := value.Args[0].(*ast.CallExpr)
			if !ok || len(typeCall.Args) != 0 {
				break
			}
			indexed, ok := typeCall.Fun.(*ast.IndexExpr)
			if !ok {
				break
			}
			selector, ok := indexed.X.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "TypeFor" {
				break
			}
			packageName, ok := selector.X.(*ast.Ident)
			if ok && packageName.Name == "reflect" {
				schemaCalls++
			}
		}
		return true
	})
	return valid && typeForCalls > 0 && typeForCalls == schemaCalls
}
