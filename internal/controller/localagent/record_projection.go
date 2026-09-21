package localagent

func projectAgent(record Record) Agent {
	return Agent{
		ID:               record.ID,
		EnrollmentTaskID: record.EnrollmentTaskID,
		Image:            record.Image,
		Generation:       record.Generation,
		Phase:            record.Phase,
		Config:           cloneConfig(record.Config),
		CreatedAt:        record.CreatedAt,
		ReadyAt:          record.ReadyAt,
	}
}

func cloneRecord(record Record) Record {
	record.Config = cloneConfig(record.Config)
	record.Credential = cloneCredential(record.Credential)
	return record
}

func cloneConfig(config Config) Config {
	cloned := Config{
		PullIntervalSeconds: config.PullIntervalSeconds,
		MaxConcurrentTasks:  config.MaxConcurrentTasks,
	}
	if config.Labels != nil {
		cloned.Labels = make(map[string]string, len(config.Labels))
		for key, value := range config.Labels {
			cloned.Labels[key] = value
		}
	}
	return cloned
}

func cloneCredential(credential Credential) Credential {
	return Credential{
		EncryptedToken: append([]byte(nil), credential.EncryptedToken...),
		Digest:         credential.Digest,
	}
}
