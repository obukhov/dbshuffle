package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Template struct {
	Template string `yaml:"template"`
	Buffer   int    `yaml:"buffer"`
	Expire   int    `yaml:"expire"` // hours
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
	return &cfg, nil
}
