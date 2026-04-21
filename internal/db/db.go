package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
}

func Connect(cfg Config) (*sql.DB, error) {
	// Connect to _dbshuffle management database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&multiStatements=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err := EnsureSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("ensure schema: %w", err)
	}
	return db, nil
}

func EnsureSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE DATABASE IF NOT EXISTS _dbshuffle;
		CREATE TABLE IF NOT EXISTS _dbshuffle.databases (
			id               CHAR(36)     NOT NULL PRIMARY KEY,
			template_name    VARCHAR(255) NOT NULL,
			db_name          VARCHAR(255) NULL DEFAULT NULL,
			created_at       DATETIME     NOT NULL,
			assigned_at      DATETIME     NULL,
			last_extended_at DATETIME     NULL,
			deleted_at       DATETIME     NULL,
			INDEX idx_template_active (template_name, db_name, deleted_at)
		) ENGINE=InnoDB
	`)
	return err
}
