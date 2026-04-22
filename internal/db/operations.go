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

// CreateDBFromPath creates dst then streams every .sql / .sql.gz file from path (sorted)
// line by line, executing each complete statement as it is found — all inside a single
// transaction. CREATE DATABASE itself is outside the transaction because MySQL DDL causes
// an implicit commit.
func (o *Operations) CreateDBFromPath(ctx context.Context, dst, path string) error {
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
