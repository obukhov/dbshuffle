package db

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("github.com/obukhov/dbshuffle/internal/db")

type Operations struct {
	db *sql.DB
}

func NewOperations(db *sql.DB) *Operations {
	return &Operations{db: db}
}

// CopyDB creates dst as a full copy of src: tables (schema + data), foreign keys,
// views, triggers, and stored routines. FK checks and unique checks are disabled
// for the duration of the copy so that data can be loaded before constraints are
// applied, and re-enabled before the connection is returned to the pool.
func (o *Operations) CopyDB(ctx context.Context, src, dst string) (err error) {
	ctx, span := tracer.Start(ctx, "CopyDB")
	span.SetAttributes(attribute.String("db.system", "mysql"), attribute.String("src", src), attribute.String("dst", dst))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	start := time.Now()
	slog.Debug("copying database", "src", src, "dst", dst)

	conn, err := o.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer func() {
		// Reset session state before returning the connection to the pool.
		conn.ExecContext(context.Background(), "SET SESSION FOREIGN_KEY_CHECKS=1, SESSION UNIQUE_CHECKS=1") //nolint:errcheck
		conn.Close()
	}()

	if _, err := conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0, SESSION UNIQUE_CHECKS=0"); err != nil {
		return fmt.Errorf("disable constraints: %w", err)
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", dst)); err != nil {
		return fmt.Errorf("create database %s: %w", dst, err)
	}

	tables, err := o.listTables(ctx, src)
	if err != nil {
		return err
	}

	for _, tbl := range tables {
		if _, err := conn.ExecContext(ctx,
			fmt.Sprintf("CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`", dst, tbl, src, tbl),
		); err != nil {
			return fmt.Errorf("create table %s: %w", tbl, err)
		}
		cols, err := o.listRegularColumns(ctx, src, tbl)
		if err != nil {
			return err
		}
		colList := "`" + strings.Join(cols, "`, `") + "`"
		if _, err := conn.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) SELECT %s FROM `%s`.`%s`", dst, tbl, colList, colList, src, tbl),
		); err != nil {
			return fmt.Errorf("copy table data %s: %w", tbl, err)
		}
	}

	// Add FK constraints after all data is loaded (checks are still off).
	fkCount, err := o.applyForeignKeys(ctx, conn, src, dst)
	if err != nil {
		return err
	}

	viewCount, err := o.copyViews(ctx, conn, src, dst)
	if err != nil {
		return err
	}

	triggerCount, err := o.copyTriggers(ctx, conn, src, dst)
	if err != nil {
		return err
	}

	routineCount, err := o.copyRoutines(ctx, conn, src, dst)
	if err != nil {
		return err
	}

	slog.Info("database copied",
		"src", src, "dst", dst,
		"tables", len(tables), "foreign_keys", fkCount,
		"views", viewCount, "triggers", triggerCount, "routines", routineCount,
		"duration", time.Since(start),
	)
	return nil
}

// applyForeignKeys reads all FK definitions from src via information_schema and
// issues ALTER TABLE … ADD CONSTRAINT statements on dst. Multi-column FKs are
// handled by grouping rows with the same constraint name.
// Returns the number of constraints applied.
func (o *Operations) applyForeignKeys(ctx context.Context, conn *sql.Conn, src, dst string) (int, error) {
	rows, err := o.db.QueryContext(ctx, `
		SELECT
			kcu.TABLE_NAME,
			kcu.CONSTRAINT_NAME,
			kcu.COLUMN_NAME,
			kcu.REFERENCED_TABLE_NAME,
			kcu.REFERENCED_COLUMN_NAME,
			rc.UPDATE_RULE,
			rc.DELETE_RULE
		FROM information_schema.KEY_COLUMN_USAGE kcu
		INNER JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
			ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
		WHERE kcu.TABLE_SCHEMA = ?
			AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`,
		src,
	)
	if err != nil {
		return 0, fmt.Errorf("query foreign keys: %w", err)
	}
	defer rows.Close()

	type fkKey struct{ table, constraint string }
	type fkDef struct {
		refTable, updateRule, deleteRule string
		cols, refCols                    []string
	}

	var ordered []fkKey
	byKey := map[fkKey]*fkDef{}

	for rows.Next() {
		var tableName, constraintName, colName, refTable, refCol, updateRule, deleteRule string
		if err := rows.Scan(&tableName, &constraintName, &colName, &refTable, &refCol, &updateRule, &deleteRule); err != nil {
			return 0, fmt.Errorf("scan fk row: %w", err)
		}
		k := fkKey{tableName, constraintName}
		if _, ok := byKey[k]; !ok {
			ordered = append(ordered, k)
			byKey[k] = &fkDef{refTable: refTable, updateRule: updateRule, deleteRule: deleteRule}
		}
		byKey[k].cols = append(byKey[k].cols, colName)
		byKey[k].refCols = append(byKey[k].refCols, refCol)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate fk rows: %w", err)
	}

	for _, k := range ordered {
		d := byKey[k]
		cols := "`" + strings.Join(d.cols, "`, `") + "`"
		refCols := "`" + strings.Join(d.refCols, "`, `") + "`"
		stmt := fmt.Sprintf(
			"ALTER TABLE `%s`.`%s` ADD CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s) ON UPDATE %s ON DELETE %s",
			dst, k.table, k.constraint, cols, dst, d.refTable, refCols, d.updateRule, d.deleteRule,
		)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return 0, fmt.Errorf("add foreign key %s.%s: %w", k.table, k.constraint, err)
		}
	}

	return len(ordered), nil
}

// copyViews copies all views from src to dst. Database references inside the
// view body ("`src`." → "`dst`.") are rewritten so the view resolves against
// the new database. Returns the number of views copied.
func (o *Operations) copyViews(ctx context.Context, conn *sql.Conn, src, dst string) (int, error) {
	rows, err := o.db.QueryContext(ctx,
		"SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
		src,
	)
	if err != nil {
		return 0, fmt.Errorf("list views: %w", err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return 0, err
		}
		views = append(views, v)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if len(views) == 0 {
		return 0, nil
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", dst)); err != nil {
		return 0, fmt.Errorf("use database %s: %w", dst, err)
	}

	for _, v := range views {
		// SHOW CREATE VIEW → View, Create View, character_set_client, collation_connection
		var viewName, createStmt, charset, collation string
		if err := o.db.QueryRowContext(ctx,
			fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`", src, v),
		).Scan(&viewName, &createStmt, &charset, &collation); err != nil {
			return 0, fmt.Errorf("show create view %s: %w", v, err)
		}
		createStmt = strings.ReplaceAll(createStmt, "`"+src+"`.", "`"+dst+"`.")
		if _, err := conn.ExecContext(ctx, createStmt); err != nil {
			return 0, fmt.Errorf("create view %s: %w", v, err)
		}
	}
	return len(views), nil
}

type triggerDef struct {
	name, createStmt string
}

// loadTriggers returns the CREATE TRIGGER DDL for every trigger in dbName.
func (o *Operations) loadTriggers(ctx context.Context, dbName string) ([]triggerDef, error) {
	rows, err := o.db.QueryContext(ctx,
		"SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? ORDER BY TRIGGER_NAME",
		dbName,
	)
	if err != nil {
		return nil, fmt.Errorf("list triggers: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		names = append(names, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	defs := make([]triggerDef, 0, len(names))
	for _, t := range names {
		// MySQL 8: SHOW CREATE TRIGGER → Trigger, sql_mode, SQL Original Statement,
		// character_set_client, collation_connection, Database Collation, Created
		var trigName, sqlMode, createStmt, charset, collation, dbCollation string
		var created interface{}
		if err := o.db.QueryRowContext(ctx,
			fmt.Sprintf("SHOW CREATE TRIGGER `%s`.`%s`", dbName, t),
		).Scan(&trigName, &sqlMode, &createStmt, &charset, &collation, &dbCollation, &created); err != nil {
			return nil, fmt.Errorf("show create trigger %s: %w", t, err)
		}
		defs = append(defs, triggerDef{name: t, createStmt: createStmt})
	}
	return defs, nil
}

// copyTriggers copies all triggers from src to dst. Triggers reference tables
// by short name, so executing under "USE dst" is sufficient to bind them to the
// new database. Returns the number of triggers copied.
func (o *Operations) copyTriggers(ctx context.Context, conn *sql.Conn, src, dst string) (int, error) {
	defs, err := o.loadTriggers(ctx, src)
	if err != nil {
		return 0, err
	}
	if len(defs) == 0 {
		return 0, nil
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", dst)); err != nil {
		return 0, fmt.Errorf("use database %s: %w", dst, err)
	}
	for _, d := range defs {
		if _, err := conn.ExecContext(ctx, d.createStmt); err != nil {
			return 0, fmt.Errorf("create trigger %s: %w", d.name, err)
		}
	}
	return len(defs), nil
}

// copyRoutines copies all stored procedures and functions from src to dst.
// Returns the total number of routines copied.
func (o *Operations) copyRoutines(ctx context.Context, conn *sql.Conn, src, dst string) (int, error) {
	rows, err := o.db.QueryContext(ctx,
		"SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_TYPE, ROUTINE_NAME",
		src,
	)
	if err != nil {
		return 0, fmt.Errorf("list routines: %w", err)
	}
	defer rows.Close()

	type routine struct{ name, typ string }
	var routines []routine
	for rows.Next() {
		var r routine
		if err := rows.Scan(&r.name, &r.typ); err != nil {
			return 0, err
		}
		routines = append(routines, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if len(routines) == 0 {
		return 0, nil
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", dst)); err != nil {
		return 0, fmt.Errorf("use database %s: %w", dst, err)
	}

	for _, r := range routines {
		// SHOW CREATE PROCEDURE/FUNCTION → Name, sql_mode, Create …, character_set_client,
		// collation_connection, Database Collation
		var name, sqlMode, createStmt string
		var charset, collation, dbCollation interface{}
		var row *sql.Row
		switch r.typ {
		case "PROCEDURE":
			row = o.db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE PROCEDURE `%s`.`%s`", src, r.name))
		case "FUNCTION":
			row = o.db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE FUNCTION `%s`.`%s`", src, r.name))
		default:
			continue
		}
		if err := row.Scan(&name, &sqlMode, &createStmt, &charset, &collation, &dbCollation); err != nil {
			return 0, fmt.Errorf("show create %s %s: %w", r.typ, r.name, err)
		}
		if _, err := conn.ExecContext(ctx, createStmt); err != nil {
			return 0, fmt.Errorf("create %s %s: %w", r.typ, r.name, err)
		}
	}
	return len(routines), nil
}

// RenameDB moves src to dst: tables are renamed, triggers/views/routines are
// recreated in dst, then the now-empty src schema is dropped.
//
// MySQL forbids RENAME TABLE across databases when the table has triggers
// (error 1435). Triggers are therefore dropped from src first and recreated
// in dst after the rename. Views and routines are still in src at that point
// (RENAME TABLE only moves tables) and are copied before DROP DATABASE removes
// them.
func (o *Operations) RenameDB(ctx context.Context, src, dst string) (err error) {
	ctx, span := tracer.Start(ctx, "RenameDB")
	span.SetAttributes(attribute.String("db.system", "mysql"), attribute.String("src", src), attribute.String("dst", dst))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	start := time.Now()
	slog.Debug("renaming database", "src", src, "dst", dst)

	// Save trigger DDLs before dropping — needed to recreate in dst.
	triggers, err := o.loadTriggers(ctx, src)
	if err != nil {
		return err
	}
	for _, t := range triggers {
		if _, err := o.db.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER `%s`.`%s`", src, t.name)); err != nil {
			return fmt.Errorf("drop trigger %s: %w", t.name, err)
		}
	}

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

	// Recreate triggers, views, and routines in dst.
	// Views and routines are still in src at this point; DROP DATABASE below removes them.
	conn, err := o.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE `%s`", dst)); err != nil {
		return fmt.Errorf("use database %s: %w", dst, err)
	}
	for _, t := range triggers {
		if _, err := conn.ExecContext(ctx, t.createStmt); err != nil {
			return fmt.Errorf("create trigger %s: %w", t.name, err)
		}
	}

	viewCount, err := o.copyViews(ctx, conn, src, dst)
	if err != nil {
		return err
	}
	routineCount, err := o.copyRoutines(ctx, conn, src, dst)
	if err != nil {
		return err
	}

	if _, err := o.db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE `%s`", src)); err != nil {
		return fmt.Errorf("drop database %s: %w", src, err)
	}

	slog.Info("database renamed",
		"src", src, "dst", dst,
		"tables", len(tables), "triggers", len(triggers),
		"views", viewCount, "routines", routineCount,
		"duration", time.Since(start),
	)
	return nil
}

// DropDB drops a database entirely.
func (o *Operations) DropDB(ctx context.Context, name string) (err error) {
	ctx, span := tracer.Start(ctx, "DropDB")
	span.SetAttributes(attribute.String("db.system", "mysql"), attribute.String("name", name))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	slog.Debug("dropping database", "name", name)
	_, err = o.db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
	if err != nil {
		return err
	}
	slog.Info("database dropped", "name", name)
	return nil
}

// CreateDBFromPath creates dst then streams every .sql / .sql.gz file from path (sorted)
// line by line, executing each complete statement as it is found — all inside a single
// transaction. CREATE DATABASE itself is outside the transaction because MySQL DDL causes
// an implicit commit.
func (o *Operations) CreateDBFromPath(ctx context.Context, dst, path string) (err error) {
	ctx, span := tracer.Start(ctx, "CreateDBFromPath")
	span.SetAttributes(attribute.String("db.system", "mysql"), attribute.String("dst", dst), attribute.String("path", path))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	start := time.Now()
	slog.Debug("creating database from path", "dst", dst, "path", path)

	if _, err := o.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", dst)); err != nil {
		return fmt.Errorf("create database %s: %w", dst, err)
	}
	slog.Debug("database created, scanning path for sql files", "dst", dst, "path", path)

	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", path, err)
	}

	var sqlFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasSuffix(n, ".sql") || strings.HasSuffix(n, ".sql.gz") {
			sqlFiles = append(sqlFiles, n)
		}
	}
	slog.Debug("found sql files", "path", path, "count", len(sqlFiles))

	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, fmt.Sprintf("USE `%s`", dst)); err != nil {
		return fmt.Errorf("use database %s: %w", dst, err)
	}

	totalStmts := 0
	for _, name := range sqlFiles {
		slog.Debug("streaming file", "dst", dst, "file", name)
		fileStart := time.Now()

		rc, err := openSQLFile(filepath.Join(path, name))
		if err != nil {
			return err
		}

		stmtNum, err := streamAndExec(ctx, tx, name, rc)
		rc.Close()
		if err != nil {
			return err
		}

		slog.Info("imported file", "dst", dst, "file", name, "statements", stmtNum, "duration", time.Since(fileStart))
		totalStmts += stmtNum
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	slog.Info("database created from path", "dst", dst, "path", path, "files", len(sqlFiles), "statements", totalStmts, "duration", time.Since(start))
	return nil
}

// streamAndExec reads r line by line, executing each complete statement via execer.
// Returns the number of statements executed.
func streamAndExec(ctx context.Context, execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, fileName string, r io.Reader) (int, error) {
	ss := newStmtStreamer(r)
	n := 0
	for {
		stmt, ok, err := ss.Next()
		if err != nil {
			return n, fmt.Errorf("read %s: %w", fileName, err)
		}
		if !ok {
			break
		}
		n++
		slog.Debug("executing statement", "file", fileName, "n", n, "preview", stmtPreview(stmt, 80))
		if _, err := execer.ExecContext(ctx, stmt); err != nil {
			return n, fmt.Errorf("statement %d in %s: %w", n, fileName, err)
		}
	}
	return n, nil
}

// stmtStreamer reads an io.Reader byte by byte and yields complete SQL statements
// delimited by `;`. Comments are kept intact — they may carry MySQL directives
// (e.g. /*!40101 ... */) that must be executed.
// Reading byte-by-byte (via bufio.Reader) means a statement can be returned
// mid-line without losing the characters that follow.
type stmtStreamer struct {
	r              *bufio.Reader
	cur            strings.Builder
	inStr          byte // 0 = normal, otherwise the opening quote byte
	inLineComment  bool
	inBlockComment bool
}

func newStmtStreamer(r io.Reader) *stmtStreamer {
	return &stmtStreamer{r: bufio.NewReaderSize(r, 64<<10)}
}

// Next returns the next complete SQL statement. Returns ("", false, nil) when
// exhausted; any trailing unterminated fragment is flushed as a final statement.
func (s *stmtStreamer) Next() (string, bool, error) {
	for {
		b, err := s.r.ReadByte()
		if err == io.EOF {
			if stmt := strings.TrimSpace(s.cur.String()); stmt != "" {
				s.cur.Reset()
				return stmt, true, nil
			}
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		c := b

		// ── inside block comment ──────────────────────────────────────────────
		if s.inBlockComment {
			s.cur.WriteByte(c)
			if c == '*' {
				if next, _ := s.r.Peek(1); len(next) > 0 && next[0] == '/' {
					s.r.ReadByte() //nolint:errcheck
					s.cur.WriteByte('/')
					s.inBlockComment = false
				}
			}
			continue
		}

		// ── inside line comment ───────────────────────────────────────────────
		if s.inLineComment {
			s.cur.WriteByte(c)
			if c == '\n' {
				s.inLineComment = false
			}
			continue
		}

		// ── inside quoted string / identifier ────────────────────────────────
		if s.inStr != 0 {
			s.cur.WriteByte(c)
			if c == '\\' && s.inStr != '`' {
				// backslash escape: consume next byte verbatim
				if next, _ := s.r.ReadByte(); next != 0 {
					s.cur.WriteByte(next)
				}
			} else if c == s.inStr {
				if next, _ := s.r.Peek(1); len(next) > 0 && next[0] == s.inStr {
					// doubled-quote escape ('' "" ``)
					s.r.ReadByte() //nolint:errcheck
					s.cur.WriteByte(s.inStr)
				} else {
					s.inStr = 0
				}
			}
			continue
		}

		// ── normal state ──────────────────────────────────────────────────────
		switch c {
		case '-':
			s.cur.WriteByte(c)
			if next, _ := s.r.Peek(1); len(next) > 0 && next[0] == '-' {
				s.r.ReadByte() //nolint:errcheck
				s.cur.WriteByte('-')
				s.inLineComment = true
			}
		case '#':
			s.cur.WriteByte(c)
			s.inLineComment = true
		case '/':
			s.cur.WriteByte(c)
			if next, _ := s.r.Peek(1); len(next) > 0 && next[0] == '*' {
				s.r.ReadByte() //nolint:errcheck
				s.cur.WriteByte('*')
				s.inBlockComment = true
			}
		case '\'', '"', '`':
			s.inStr = c
			s.cur.WriteByte(c)
		case ';':
			s.cur.WriteByte(c)
			if stmt := strings.TrimSpace(s.cur.String()); stmt != ";" {
				s.cur.Reset()
				return stmt, true, nil
			}
			s.cur.Reset()
		default:
			s.cur.WriteByte(c)
		}
	}
}

// stmtPreview returns a compact single-line preview of a statement for logging.
func stmtPreview(s string, maxLen int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

// openSQLFile opens a .sql or .sql.gz file and returns a ReadCloser.
func openSQLFile(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	if !strings.HasSuffix(path, ".gz") {
		return f, nil
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("gzip reader %s: %w", path, err)
	}
	return &gzipReadCloser{gr: gr, f: f}, nil
}

type gzipReadCloser struct {
	gr *gzip.Reader
	f  *os.File
}

func (g *gzipReadCloser) Read(p []byte) (int, error) { return g.gr.Read(p) }
func (g *gzipReadCloser) Close() error {
	g.gr.Close()
	return g.f.Close()
}

// listRegularColumns returns column names for a table, excluding generated columns.
// MySQL rejects explicit values for generated columns in INSERT statements.
func (o *Operations) listRegularColumns(ctx context.Context, dbName, tableName string) ([]string, error) {
	rows, err := o.db.QueryContext(ctx,
		"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT IN ('VIRTUAL GENERATED', 'STORED GENERATED') ORDER BY ORDINAL_POSITION",
		dbName, tableName,
	)
	if err != nil {
		return nil, fmt.Errorf("list columns for %s.%s: %w", dbName, tableName, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
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
