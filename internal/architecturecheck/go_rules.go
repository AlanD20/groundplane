package architecturecheck

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
)

type parsedGoFile struct {
	file *sourceFile
	ast  *ast.File
	fset *token.FileSet
}

type goPackage struct {
	interfaces map[string]struct{}
	concrete   map[string]struct{}
	types      map[string]ast.Expr
}

func checkGoRules(ctx context.Context, root string, files []*sourceFile) []Finding {
	parsed := make([]*parsedGoFile, 0)
	packages := make(map[string]*goPackage)
	findings := make([]Finding, 0)
	module := modulePath(root)
	for _, file := range files {
		if file.ext != ".go" {
			continue
		}
		if contextErr(ctx) != nil {
			return append(findings, Finding{Path: file.rel, Line: 1, Column: 1, Rule: "context-canceled", Message: contextErr(ctx).Error()})
		}
		fset := token.NewFileSet()
		parsedFile, parseErr := parser.ParseFile(fset, file.abs, file.data, parser.ParseComments|parser.AllErrors)
		unit := &parsedGoFile{file: file, ast: parsedFile, fset: fset}
		parsed = append(parsed, unit)
		if parseErr != nil {
			line, column := 1, 1
			message := parseErr.Error()
			if list, ok := parseErr.(scanner.ErrorList); ok && len(list) > 0 {
				line, column = list[0].Pos.Line, list[0].Pos.Column
				message = list[0].Msg
			}
			findings = append(findings, Finding{Path: file.rel, Line: line, Column: column, Rule: "go-parse-error", Message: message})
		}
		if parsedFile == nil {
			continue
		}
		key := file.dir + "\x00" + parsedFile.Name.Name
		packageInfo := packages[key]
		if packageInfo == nil {
			packageInfo = &goPackage{interfaces: make(map[string]struct{}), concrete: make(map[string]struct{}), types: make(map[string]ast.Expr)}
			packages[key] = packageInfo
		}
		for _, declaration := range parsedFile.Decls {
			genDecl, ok := declaration.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}
			for _, specification := range genDecl.Specs {
				typeSpec, ok := specification.(*ast.TypeSpec)
				if !ok {
					continue
				}
				packageInfo.types[typeSpec.Name.Name] = typeSpec.Type
			}
		}
	}
	for _, packageInfo := range packages {
		resolveLocalInterfaces(packageInfo)
		for name := range packageInfo.types {
			if _, isInterface := packageInfo.interfaces[name]; !isInterface {
				packageInfo.concrete[name] = struct{}{}
			}
		}
	}
	for _, unit := range parsed {
		if unit.ast == nil {
			continue
		}
		for _, importSpec := range unit.ast.Imports {
			importPath, err := strconv.Unquote(importSpec.Path.Value)
			if err != nil {
				continue
			}
			position := unit.fset.Position(importSpec.Pos())
			if importPath == "unsafe" {
				findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "unsafe-import", Message: "unsafe imports are forbidden"})
			}
			if importPath == "reflect" && !unit.file.isTest {
				findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "reflect-import", Subject: "reflect", Message: "reflection is a forbidden conversion dependency"})
			}
			if subject, reason := forbiddenLayerImport(unit.file.rel, importPath, module); reason != "" {
				findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "layer-import", Subject: subject, Message: reason})
			}
		}
		if unit.file.isTest || unit.ast.Name == nil {
			continue
		}
		packageInfo := packages[unit.file.dir+"\x00"+unit.ast.Name.Name]
		if packageInfo == nil {
			continue
		}
		findings = append(findings, checkUnsafeMappings(unit, packageInfo)...)
		for _, declaration := range unit.ast.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Name == nil || !function.Name.IsExported() || !strings.HasPrefix(function.Name.Name, "New") || function.Type.Results == nil {
				continue
			}
			if !returnsLocalInterface(function.Type.Results, packageInfo.interfaces) {
				continue
			}
			position := unit.fset.Position(function.Pos())
			findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "interface-constructor", Subject: function.Name.Name, Message: fmt.Sprintf("exported constructor %s returns a locally declared interface", function.Name.Name)})
		}
	}
	return findings
}

func checkUnsafeMappings(unit *parsedGoFile, packageInfo *goPackage) []Finding {
	findings := make([]Finding, 0)
	structNames := make(map[*ast.StructType]string)
	for _, declaration := range unit.ast.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.TYPE {
			continue
		}
		for _, specification := range generic.Specs {
			typeSpec, ok := specification.(*ast.TypeSpec)
			if !ok {
				continue
			}
			if structType, ok := typeSpec.Type.(*ast.StructType); ok {
				structNames[structType] = typeSpec.Name.Name
			}
		}
	}
	ast.Inspect(unit.ast, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.TypeAssertExpr:
			if subject := localConcreteSubject(value.Type, packageInfo.concrete); subject != "" {
				position := unit.fset.Position(value.Pos())
				findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "local-concrete-recovery", Subject: subject, Message: "type assertion recovers a locally declared concrete type"})
			}
		case *ast.TypeSwitchStmt:
			for _, statement := range value.Body.List {
				clause, ok := statement.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expression := range clause.List {
					subject := localConcreteSubject(expression, packageInfo.concrete)
					if subject == "" {
						continue
					}
					position := unit.fset.Position(expression.Pos())
					findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "local-concrete-recovery", Subject: subject, Message: "type switch recovers a locally declared concrete type"})
				}
			}
		case *ast.StructType:
			for _, field := range value.Fields.List {
				if !isOpenModelField(field.Type) {
					continue
				}
				position := unit.fset.Position(field.Type.Pos())
				subject := structNames[value] + "." + fieldName(field)
				findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "open-model-field", Subject: subject, Message: "model fields must not use any or interface{}"})
			}
		}
		return true
	})
	findings = append(findings, checkJSONRoundTrips(unit)...)
	return findings
}

func localConcreteSubject(expression ast.Expr, concrete map[string]struct{}) string {
	if expression == nil {
		return ""
	}
	switch value := expression.(type) {
	case *ast.Ident:
		if _, ok := concrete[value.Name]; ok {
			return value.Name
		}
		return ""
	case *ast.StarExpr:
		return localConcreteSubject(value.X, concrete)
	case *ast.IndexExpr:
		return localConcreteSubject(value.X, concrete)
	case *ast.IndexListExpr:
		return localConcreteSubject(value.X, concrete)
	default:
		return ""
	}
}

func fieldName(field *ast.Field) string {
	if len(field.Names) == 0 {
		return "<anonymous>"
	}
	return field.Names[0].Name
}

func isOpenModelField(expression ast.Expr) bool {
	if isAnyType(expression) {
		return true
	}
	mapType, ok := expression.(*ast.MapType)
	return ok && isStringType(mapType.Key) && isAnyType(mapType.Value)
}

func isAnyType(expression ast.Expr) bool {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name == "any"
	case *ast.InterfaceType:
		return value.Methods != nil && len(value.Methods.List) == 0
	default:
		return false
	}
}

func isStringType(expression ast.Expr) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == "string"
}

func checkJSONRoundTrips(unit *parsedGoFile) []Finding {
	findings := make([]Finding, 0)
	for _, declaration := range unit.ast.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		marshaled := make(map[string]struct{})
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.AssignStmt:
				for index, right := range value.Rhs {
					if !isJSONCall(right, "Marshal", "MarshalIndent") || index >= len(value.Lhs) {
						continue
					}
					if identifier, ok := value.Lhs[index].(*ast.Ident); ok {
						marshaled[identifier.Name] = struct{}{}
					}
				}
			case *ast.CallExpr:
				if !isJSONCall(value, "Unmarshal") || len(value.Args) == 0 {
					return true
				}
				identifier, ok := value.Args[0].(*ast.Ident)
				if !ok {
					return true
				}
				if _, roundTrip := marshaled[identifier.Name]; roundTrip {
					position := unit.fset.Position(value.Pos())
					findings = append(findings, Finding{Path: unit.file.rel, Line: position.Line, Column: position.Column, Rule: "json-roundtrip-conversion", Subject: function.Name.Name, Message: "JSON marshal/unmarshal round trips are forbidden conversion strategies"})
				}
			}
			return true
		})
	}
	return findings
}

func isJSONCall(expression ast.Expr, names ...string) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	packageName, ok := selector.X.(*ast.Ident)
	if !ok || packageName.Name != "json" {
		return false
	}
	for _, name := range names {
		if selector.Sel.Name == name {
			return true
		}
	}
	return false
}

func resolveLocalInterfaces(packageInfo *goPackage) {
	changed := true
	for changed {
		changed = false
		for name, expression := range packageInfo.types {
			if _, resolved := packageInfo.interfaces[name]; resolved {
				continue
			}
			if !isLocalInterfaceType(expression, packageInfo.interfaces) {
				continue
			}
			packageInfo.interfaces[name] = struct{}{}
			changed = true
		}
	}
}

func isLocalInterfaceType(expression ast.Expr, interfaces map[string]struct{}) bool {
	switch value := expression.(type) {
	case *ast.InterfaceType:
		return true
	case *ast.Ident:
		_, ok := interfaces[value.Name]
		return ok
	case *ast.IndexExpr:
		return isLocalInterfaceType(value.X, interfaces)
	case *ast.IndexListExpr:
		return isLocalInterfaceType(value.X, interfaces)
	default:
		return false
	}
}

func modulePath(root string) string {
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			return fields[1]
		}
	}
	return ""
}

func forbiddenLayerImport(source, importPath, module string) (string, string) {
	if module == "" || !strings.HasPrefix(importPath, module+"/") {
		return "", ""
	}
	target := strings.TrimPrefix(importPath, module+"/")
	sourceDirectory := pathpkg.Dir(source)
	forbidden := func(prefixes ...string) bool {
		for _, prefix := range prefixes {
			if target == prefix || strings.HasPrefix(target, prefix+"/") {
				return true
			}
		}
		return false
	}
	var denied bool
	switch {
	case sourceDirectory == "cmd/groundplane":
		denied = !forbidden("internal/app", "internal/cli")
	case sourceDirectory == "cmd/controller" || sourceDirectory == "cmd/agent":
		denied = !forbidden("internal/app")
	case sourceDirectory == "console":
		denied = true
	case sourceDirectory == "internal/cli" || strings.HasPrefix(sourceDirectory, "internal/cli/"):
		denied = forbidden("internal/controller", "internal/agent", "internal/infra", "internal/adapters", "internal/components")
	case sourceDirectory == "internal/controller" || strings.HasPrefix(sourceDirectory, "internal/controller/"):
		denied = forbidden("internal/cli")
	case sourceDirectory == "internal/agent" || strings.HasPrefix(sourceDirectory, "internal/agent/"):
		denied = forbidden("internal/controller", "internal/cli", "internal/adapters", "internal/components")
	case sourceDirectory == "internal/core" || strings.HasPrefix(sourceDirectory, "internal/core/"):
		denied = forbidden("internal/infra", "internal/controller", "internal/agent", "internal/adapters", "internal/components", "internal/cli")
	case sourceDirectory == "internal/adapters" || strings.HasPrefix(sourceDirectory, "internal/adapters/"):
		denied = forbidden("internal/infra", "internal/cli", "internal/controller", "internal/agent", "internal/components")
	case sourceDirectory == "internal/components" || strings.HasPrefix(sourceDirectory, "internal/components/"):
		denied = forbidden("internal/infra", "internal/cli", "internal/controller", "internal/agent", "internal/adapters")
	case sourceDirectory == "internal/infra" || strings.HasPrefix(sourceDirectory, "internal/infra/"):
		denied = forbidden("internal/cli", "internal/controller", "internal/agent", "internal/adapters", "internal/components", "pkg/api")
	case sourceDirectory == "internal/common" || strings.HasPrefix(sourceDirectory, "internal/common/"):
		denied = forbidden("internal/app", "internal/cli", "internal/controller", "internal/agent", "internal/infra", "internal/adapters", "internal/components", "pkg/api")
	case sourceDirectory == "pkg/api" || strings.HasPrefix(sourceDirectory, "pkg/api/"):
		denied = forbidden("internal/infra")
	case sourceDirectory == "pkg/errs" || strings.HasPrefix(sourceDirectory, "pkg/errs/"):
		denied = true
	}
	if denied {
		return target, fmt.Sprintf("%s must not import %s", sourceDirectory, target)
	}
	return "", ""
}

func returnsLocalInterface(fields *ast.FieldList, interfaces map[string]struct{}) bool {
	for _, field := range fields.List {
		if containsLocalInterface(field.Type, interfaces) {
			return true
		}
	}
	return false
}

func containsLocalInterface(expression ast.Expr, interfaces map[string]struct{}) bool {
	switch value := expression.(type) {
	case *ast.Ident:
		_, ok := interfaces[value.Name]
		return ok
	case *ast.StarExpr:
		return containsLocalInterface(value.X, interfaces)
	case *ast.ArrayType:
		return containsLocalInterface(value.Elt, interfaces)
	case *ast.Ellipsis:
		return containsLocalInterface(value.Elt, interfaces)
	case *ast.MapType:
		return containsLocalInterface(value.Key, interfaces) || containsLocalInterface(value.Value, interfaces)
	case *ast.ChanType:
		return containsLocalInterface(value.Value, interfaces)
	case *ast.FuncType:
		return value.Results != nil && returnsLocalInterface(value.Results, interfaces)
	case *ast.IndexExpr:
		return containsLocalInterface(value.X, interfaces) || containsLocalInterface(value.Index, interfaces)
	case *ast.IndexListExpr:
		if containsLocalInterface(value.X, interfaces) {
			return true
		}
		for _, index := range value.Indices {
			if containsLocalInterface(index, interfaces) {
				return true
			}
		}
	}
	return false
}
