package architecturecheck

type tsToken struct {
	text   string
	line   int
	column int
}

type tsLexer struct {
	data   []byte
	pos    int
	line   int
	column int
}

func checkTypeScriptRules(files []*sourceFile) []Finding {
	findings := make([]Finding, 0)
	for _, file := range files {
		if file.ext != ".ts" && file.ext != ".tsx" && file.ext != ".mjs" {
			continue
		}
		tokens := lexTypeScript(file.data)
		for index, token := range tokens {
			if token.text == "as" && index+2 < len(tokens) && tokens[index+1].text == "unknown" && tokens[index+2].text == "as" && !likelyJSXText(tokens, index) {
				findings = append(findings, Finding{Path: file.rel, Line: token.line, Column: token.column, Rule: "ts-unsafe-assertion", Message: "double assertion through unknown is forbidden"})
			}
			if token.text == "as" && index+1 < len(tokens) && tokens[index+1].text == "any" && !likelyJSXText(tokens, index) {
				findings = append(findings, Finding{Path: file.rel, Line: token.line, Column: token.column, Rule: "ts-unsafe-assertion", Message: "assertion to any is forbidden"})
			}
			if file.ext != ".tsx" && token.text == "<" && index+2 < len(tokens) && tokens[index+1].text == "any" && tokens[index+2].text == ">" {
				findings = append(findings, Finding{Path: file.rel, Line: token.line, Column: token.column, Rule: "ts-unsafe-assertion", Message: "angle-bracket assertion to any is forbidden"})
			}
		}
	}
	return findings
}

func likelyJSXText(tokens []tsToken, index int) bool {
	return index > 0 && tokens[index-1].text == ">"
}

func lexTypeScript(data []byte) []tsToken {
	lexer := &tsLexer{data: data, line: 1, column: 1}
	tokens := make([]tsToken, 0)
	lexer.lexCode(&tokens, false)
	return tokens
}

func (lexer *tsLexer) lexCode(tokens *[]tsToken, stopAtTemplateEnd bool) {
	braceDepth := 0
	for lexer.pos < len(lexer.data) {
		current := lexer.data[lexer.pos]
		if stopAtTemplateEnd && current == '}' && braceDepth == 0 {
			lexer.advance()
			return
		}
		if isSpace(current) {
			lexer.advance()
			continue
		}
		if current == '/' && lexer.pos+1 < len(lexer.data) && lexer.data[lexer.pos+1] == '/' {
			lexer.skipLineComment()
			continue
		}
		if current == '/' && lexer.pos+1 < len(lexer.data) && lexer.data[lexer.pos+1] == '*' {
			lexer.skipBlockComment()
			continue
		}
		if current == '\'' || current == '"' {
			lexer.skipQuoted(current)
			continue
		}
		if current == '`' {
			lexer.skipTemplate(tokens)
			continue
		}
		if current == '/' && lexer.regexCanStart(*tokens) {
			lexer.skipRegex()
			continue
		}
		line, column := lexer.line, lexer.column
		if isIdentifierStart(current) {
			start := lexer.pos
			for lexer.pos < len(lexer.data) && isIdentifierPart(lexer.data[lexer.pos]) {
				lexer.advance()
			}
			*tokens = append(*tokens, tsToken{text: string(lexer.data[start:lexer.pos]), line: line, column: column})
			continue
		}
		if current == '{' {
			braceDepth++
		} else if current == '}' && braceDepth > 0 {
			braceDepth--
		}
		lexer.advance()
		*tokens = append(*tokens, tsToken{text: string(current), line: line, column: column})
	}
}

func (lexer *tsLexer) advance() {
	if lexer.pos >= len(lexer.data) {
		return
	}
	if lexer.data[lexer.pos] == '\n' {
		lexer.line++
		lexer.column = 1
	} else {
		lexer.column++
	}
	lexer.pos++
}

func (lexer *tsLexer) skipLineComment() {
	for lexer.pos < len(lexer.data) && lexer.data[lexer.pos] != '\n' {
		lexer.advance()
	}
}

func (lexer *tsLexer) skipBlockComment() {
	lexer.advance()
	lexer.advance()
	for lexer.pos < len(lexer.data) {
		if lexer.data[lexer.pos] == '*' && lexer.pos+1 < len(lexer.data) && lexer.data[lexer.pos+1] == '/' {
			lexer.advance()
			lexer.advance()
			return
		}
		lexer.advance()
	}
}

func (lexer *tsLexer) skipQuoted(quote byte) {
	lexer.advance()
	for lexer.pos < len(lexer.data) {
		if lexer.data[lexer.pos] == '\\' {
			lexer.advance()
			if lexer.pos < len(lexer.data) {
				lexer.advance()
			}
			continue
		}
		if lexer.data[lexer.pos] == quote {
			lexer.advance()
			return
		}
		lexer.advance()
	}
}

func (lexer *tsLexer) skipTemplate(tokens *[]tsToken) {
	lexer.advance()
	for lexer.pos < len(lexer.data) {
		if lexer.data[lexer.pos] == '\\' {
			lexer.advance()
			if lexer.pos < len(lexer.data) {
				lexer.advance()
			}
			continue
		}
		if lexer.data[lexer.pos] == '`' {
			lexer.advance()
			return
		}
		if lexer.data[lexer.pos] == '$' && lexer.pos+1 < len(lexer.data) && lexer.data[lexer.pos+1] == '{' {
			line, column := lexer.line, lexer.column
			lexer.advance()
			lexer.advance()
			*tokens = append(*tokens, tsToken{text: ";", line: line, column: column})
			lexer.lexCode(tokens, true)
			*tokens = append(*tokens, tsToken{text: ";", line: lexer.line, column: lexer.column})
			continue
		}
		lexer.advance()
	}
}

func (lexer *tsLexer) skipRegex() {
	lexer.advance()
	inClass := false
	for lexer.pos < len(lexer.data) {
		current := lexer.data[lexer.pos]
		if current == '\\' {
			lexer.advance()
			if lexer.pos < len(lexer.data) {
				lexer.advance()
			}
			continue
		}
		if current == '[' {
			inClass = true
		} else if current == ']' {
			inClass = false
		} else if current == '/' && !inClass {
			lexer.advance()
			for lexer.pos < len(lexer.data) && isIdentifierPart(lexer.data[lexer.pos]) {
				lexer.advance()
			}
			return
		}
		lexer.advance()
	}
}

func (lexer *tsLexer) regexCanStart(tokens []tsToken) bool {
	if len(tokens) == 0 {
		return true
	}
	previous := tokens[len(tokens)-1].text
	if previous == "return" || previous == "throw" || previous == "case" || previous == "yield" || previous == "await" {
		return true
	}
	if isIdentifierStart(previous[0]) || previous == ")" || previous == "]" || previous == "}" {
		return false
	}
	return true
}

func isSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\n' || value == '\r' || value == '\f'
}

func isIdentifierStart(value byte) bool {
	return value == '_' || value == '$' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || value >= '0' && value <= '9'
}
