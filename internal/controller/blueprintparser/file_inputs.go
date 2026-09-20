package blueprintparser

func (p *parsePlan) inspectEnvFiles(baseDir string, raw any) error {
	if raw == nil {
		return nil
	}
	entries, ok := anyList(raw)
	if !ok {
		return validationError("blueprint env_file is invalid")
	}
	for _, rawEntry := range entries {
		entry, ok := stringMap(rawEntry)
		if !ok {
			return validationError("blueprint env_file is invalid")
		}
		value, ok := entry["path"].(string)
		if !ok {
			return validationError("blueprint env_file path is invalid")
		}
		required := true
		if configured, exists := entry["required"]; exists {
			var valid bool
			required, valid = configured.(bool)
			if !valid {
				return validationError("blueprint env_file required flag is invalid")
			}
		}
		resolved, exists, err := p.requireReference(baseDir, value, false, !required)
		if err != nil {
			return err
		}
		if required && !exists {
			return validationError("blueprint env_file is undeclared")
		}
		if exists {
			p.runtime[resolved] = struct{}{}
		}
	}
	return nil
}

func (p *parsePlan) inspectRequiredFiles(baseDir string, raw any) error {
	if raw == nil {
		return nil
	}
	values, ok := stringList(raw)
	if !ok {
		return validationError("blueprint file reference is invalid")
	}
	for _, value := range values {
		resolved, exists, err := p.requireReference(baseDir, value, false, false)
		if err != nil {
			return err
		}
		if !exists {
			return validationError("blueprint file reference is undeclared")
		}
		p.runtime[resolved] = struct{}{}
	}
	return nil
}

func (p *parsePlan) inspectConfigs(baseDir string, raw any) error {
	configs, ok := stringMap(raw)
	if raw == nil || !ok {
		return nil
	}
	for _, rawConfig := range configs {
		config, ok := stringMap(rawConfig)
		if !ok {
			continue
		}
		if file, exists := config["file"]; exists {
			value, ok := file.(string)
			if !ok {
				return validationError("blueprint config file is invalid")
			}
			resolved, declared, err := p.requireReference(baseDir, value, false, false)
			if err != nil {
				return err
			}
			if !declared {
				return validationError("blueprint config file is undeclared")
			}
			p.runtime[resolved] = struct{}{}
		}
	}
	return nil
}

func inspectSecrets(raw any) error {
	secrets, ok := stringMap(raw)
	if raw == nil || !ok {
		return nil
	}
	for _, rawSecret := range secrets {
		secret, ok := stringMap(rawSecret)
		if !ok {
			continue
		}
		for _, source := range []string{"file", "content", "environment"} {
			if _, exists := secret[source]; exists {
				return validationError("blueprint authored secret sources are forbidden")
			}
		}
	}
	return nil
}
