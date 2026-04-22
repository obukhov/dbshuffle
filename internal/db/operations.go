package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

type Operations struct {
	db *sql.DB
}

func NewOperations(db *sql.DB) *Operations {
	return &Operations{db: db}
}

// CopyDB creates dst as a full copy of src (schema + data).
func (o *Operations) CopyDB(ctx context.Context, src, dst string) error {
	start := time.Now()
	slog.Debug("copying database", "src", src, "dst", dst)

	if _, err := o.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", dst)); err != nil {
		return fmt.Errorf("create database %s: %w", dst, err)
	}

	tables, err := o.listTables(ctx, src)
	if err != nil {
		return err
	}

	for _, tbl := range tables {
		if _, err := o.db.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`", dst, tbl, src, tbl),
		); err != nil {
			return fmt.Errorf("create table %s: %w", tbl, err)
		}
		if _, err := o.db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`", dst, tbl, src, tbl),
		); err != nil {
			return fmt.Errorf("copy table %s: %w", tbl, err)
		}
	}

	slog.Info("database copied", "src", src, "dst", dst, "tables", len(tables), "duration", time.Since(start))
	return nil
}

// RenameDB renames src to dst by moving all tables then dropping the empty src.
func (o *Operations) RenameDB(ctx context.Context, src, dst string) error {
	start := time.Now()
	slog.Debug("renaming database", "src", src, "dst", dst)

	if _, err := o.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", dst)); err != nil {
		return fmt.Errorf("create database %s: %w", dst, err)
	}

	tables, err := o.listTables(ctx, src)
	if err != nil {
		return err
	}

	for _, tbl := range tables {
		if _, err := o.db.ExecContext(ctx,
			fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`", src, tbl, dst, tbl),
		); err != nil {
			return fmt.Errorf("rename table %s: %w", tbl, err)
		}
	}

	if _, err := o.db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE `%s`", src)); err != nil {
		return fmt.Errorf("drop database %s: %w", src, err)
	}

	slog.Info("database renamed", "src", src, "dst", dst, "tables", len(tables), "duration", time.Since(start))
	return nil
}

// DropDB drops a database entirely.
func (o *Operations) DropDB(ctx context.Context, name string) error {
	slog.Debug("dropping database", "name", name)
	_, err := o.db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
	if err != nil {
		return err
	}
	slog.Info("database dropped", "name", name)
	return nil
}

func (o *Operations) listTables(ctx context.Context, dbName string) ([]string, error) {
	rows, err := o.db.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
		dbName,
	)
	if err != nil {
		return nil, fmt.Errorf("list tables for %s: %w", dbName, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}
