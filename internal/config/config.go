package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Template struct {
	FromDB   string `yaml:"from_db"`
	FromPath string `yaml:"from_path"`
	Buffer   int    `yaml:"buffer"`
	Expire   int    `yaml:"expire"` // hours
}

func (t Template) validate(name string) error {
	if t.FromDB == "" && t.FromPath == "" {
		return fmt.Errorf("template %q: one of from_db or from_path must be set", name)
	}
	if t.FromDB != "" && t.FromPath != "" {
		return fmt.Errorf("template %q: from_db and from_path are mutually exclusive", name)
	}
	return nil
}

type Config struct {
	DBTemplates map[string]Template `yaml:"dbtemplates"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	for name, tmpl := range cfg.DBTemplates {
		if err := tmpl.validate(name); err != nil {
			return nil, err
		}
	}
	return &cfg, nil
}
