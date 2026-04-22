package db

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stmts(t *testing.T, input string) []string {
	t.Helper()
	ss := newStmtStreamer(strings.NewReader(input))
	var out []string
	for {
		stmt, ok, err := ss.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		out = append(out, stmt)
	}
	return out
}

func TestStmtStreamer(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "simple statements",
			input: "SELECT 1; SELECT 2;",
			want:  []string{"SELECT 1;", "SELECT 2;"},
		},
		{
			name:  "unterminated last statement flushed",
			input: "SELECT 1; SELECT 2",
			want:  []string{"SELECT 1;", "SELECT 2"},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "semicolon inside single-quoted string",
			input: "INSERT INTO t VALUES ('a;b'); SELECT 1;",
			want:  []string{"INSERT INTO t VALUES ('a;b');", "SELECT 1;"},
		},
		{
			name:  "semicolon inside double-quoted string",
			input: `INSERT INTO t VALUES ("a;b"); SELECT 1;`,
			want:  []string{`INSERT INTO t VALUES ("a;b");`, "SELECT 1;"},
		},
		{
			name:  "semicolon inside backtick identifier",
			input: "SELECT `col;name` FROM t;",
			want:  []string{"SELECT `col;name` FROM t;"},
		},
		{
			name:  "backslash escape inside string",
			input: `INSERT INTO t VALUES ('it\'s fine'); SELECT 1;`,
			want:  []string{`INSERT INTO t VALUES ('it\'s fine');`, "SELECT 1;"},
		},
		{
			name:  "doubled single-quote escape",
			input: "INSERT INTO t VALUES ('it''s fine'); SELECT 1;",
			want:  []string{"INSERT INTO t VALUES ('it''s fine');", "SELECT 1;"},
		},
		{
			name:  "line comment preserved",
			input: "SELECT 1; -- this is a comment\nSELECT 2;",
			want:  []string{"SELECT 1;", "-- this is a comment\nSELECT 2;"},
		},
		{
			name:  "hash comment preserved",
			input: "SELECT 1; # mysql comment\nSELECT 2;",
			want:  []string{"SELECT 1;", "# mysql comment\nSELECT 2;"},
		},
		{
			name:  "block comment preserved",
			input: "SELECT /* inline */ 1; SELECT 2;",
			want:  []string{"SELECT /* inline */ 1;", "SELECT 2;"},
		},
		{
			name:  "mysql conditional comment preserved",
			input: "/*!40101 SET NAMES utf8mb4 */;",
			want:  []string{"/*!40101 SET NAMES utf8mb4 */;"},
		},
		{
			name:  "backtick in line comment does not open string",
			input: "-- `users`\nSELECT 1;",
			want:  []string{"-- `users`\nSELECT 1;"},
		},
		{
			name:  "semicolon inside block comment does not split",
			input: "/* do; not; split */ SELECT 1;",
			want:  []string{"/* do; not; split */ SELECT 1;"},
		},
		{
			name:  "multiline statement",
			input: "CREATE TABLE t (\n  id INT\n);\nINSERT INTO t VALUES (1);",
			want:  []string{"CREATE TABLE t (\n  id INT\n);", "INSERT INTO t VALUES (1);"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stmts(t, tc.input))
		})
	}
}
