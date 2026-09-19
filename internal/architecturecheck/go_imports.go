package architecturecheck

import "strconv"

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
		if importPath == "reflect" && !unit.file.isTest {
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
