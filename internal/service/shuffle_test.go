package service

import (
	"context"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/obukhov/dbshuffle/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockOps implements dbOperations for testing.
type mockOps struct {
	copyDB            func(ctx context.Context, src, dst string) error
	renameDB          func(ctx context.Context, src, dst string) error
	dropDB            func(ctx context.Context, name string) error
	createDBFromPath  func(ctx context.Context, dst, path string) error
}

func (m *mockOps) CopyDB(ctx context.Context, src, dst string) error {
	if m.copyDB != nil {
		return m.copyDB(ctx, src, dst)
	}
	return nil
}

func (m *mockOps) RenameDB(ctx context.Context, src, dst string) error {
	if m.renameDB != nil {
		return m.renameDB(ctx, src, dst)
	}
	return nil
}

func (m *mockOps) DropDB(ctx context.Context, name string) error {
	if m.dropDB != nil {
		return m.dropDB(ctx, name)
	}
	return nil
}

func (m *mockOps) CreateDBFromPath(ctx context.Context, dst, path string) error {
	if m.createDBFromPath != nil {
		return m.createDBFromPath(ctx, dst, path)
	}
	return nil
}

func testCfg() *config.Config {
	return &config.Config{
		DBTemplates: map[string]config.Template{
			"blog": {FromDB: "_template_blog", Buffer: 3, Expire: 24},
		},
	}
}

func newTestService(t *testing.T) (*ShuffleService, sqlmock.Sqlmock, *mockOps) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	ops := &mockOps{}
	svc := &ShuffleService{db: db, ops: ops, cfg: testCfg()}
	t.Cleanup(func() { db.Close() })
	return svc, mock, ops
}

func recordCols() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "template_name", "db_name", "created_at",
		"assigned_at", "last_extended_at", "deleted_at",
	})
}

// -- Status ------------------------------------------------------------------

func TestStatus_GroupsBufferAssignedExpired(t *testing.T) {
	svc, mock, _ := newTestService(t)

	now := time.Now()
	assignedAt := now.Add(-2 * time.Hour)
	activeExtended := now.Add(-1 * time.Hour)   // expires in 23h — not expired
	expiredExtended := now.Add(-25 * time.Hour)  // expired 1h ago

	dbNameActive := "active_db"
	dbNameExpired := "expired_db"

	rows := recordCols().
		// buffer: db_name IS NULL
		AddRow("id-buf", "blog", nil, now.Add(-10*time.Minute), nil, nil, nil).
		// assigned, not expired
		AddRow("id-act", "blog", dbNameActive, now.Add(-3*time.Hour), assignedAt, activeExtended, nil).
		// assigned, expired
		AddRow("id-exp", "blog", dbNameExpired, now.Add(-30*time.Hour), now.Add(-26*time.Hour), expiredExtended, nil)

	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE deleted_at IS NULL ORDER BY template_name, created_at",
	).WillReturnRows(rows)

	reports, err := svc.Status(context.Background())

	require.NoError(t, err)
	require.Len(t, reports, 1)

	r := reports[0]
	assert.Equal(t, "blog", r.Template)
	assert.Len(t, r.Buffer, 1)
	assert.Len(t, r.Assigned, 1)
	assert.Len(t, r.Expired, 1)

	assert.Equal(t, "id-buf", r.Buffer[0].ID)
	assert.Equal(t, "active_db", *r.Assigned[0].DBName)
	assert.Equal(t, "expired_db", *r.Expired[0].DBName)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStatus_EmptyReturnsNoReports(t *testing.T) {
	svc, mock, _ := newTestService(t)

	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE deleted_at IS NULL ORDER BY template_name, created_at",
	).WillReturnRows(recordCols())

	reports, err := svc.Status(context.Background())

	require.NoError(t, err)
	assert.Empty(t, reports)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// -- Assign ------------------------------------------------------------------

func TestAssign_Success(t *testing.T) {
	svc, mock, ops := newTestService(t)

	const (
		recID    = "550e8400-e29b-41d4-a716-446655440000"
		dbName   = "myfeature_test"
		template = "blog"
	)
	createdAt := time.Now().Add(-5 * time.Minute)
	expectedPhysical := template + "_" + strings.ReplaceAll(recID, "-", "")

	var renamedFrom, renamedTo string
	ops.renameDB = func(_ context.Context, src, dst string) error {
		renamedFrom, renamedTo = src, dst
		return nil
	}

	mock.ExpectBegin()
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs(dbName).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery(
		"SELECT id, created_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL ORDER BY created_at ASC LIMIT 1 FOR UPDATE",
	).WithArgs(template).WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(recID, createdAt))
	mock.ExpectCommit()
	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET db_name = ?, assigned_at = ?, last_extended_at = ? WHERE id = ?",
	).WithArgs(dbName, sqlmock.AnyArg(), sqlmock.AnyArg(), recID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	rec, err := svc.Assign(context.Background(), template, dbName)

	require.NoError(t, err)
	assert.Equal(t, dbName, *rec.DBName)
	assert.Equal(t, recID, rec.ID)
	assert.NotNil(t, rec.AssignedAt)
	assert.Equal(t, expectedPhysical, renamedFrom)
	assert.Equal(t, dbName, renamedTo)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAssign_UnknownTemplate(t *testing.T) {
	svc, mock, _ := newTestService(t)

	_, err := svc.Assign(context.Background(), "nonexistent", "mydb")

	assert.ErrorIs(t, err, ErrUnknownTemplate)
	assert.NoError(t, mock.ExpectationsWereMet()) // no SQL expected
}

func TestAssign_AlreadyAssigned(t *testing.T) {
	svc, mock, _ := newTestService(t)

	mock.ExpectBegin()
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs("taken_db").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectRollback()

	_, err := svc.Assign(context.Background(), "blog", "taken_db")

	assert.ErrorIs(t, err, ErrAlreadyAssigned)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestAssign_NoBuffer(t *testing.T) {
	svc, mock, _ := newTestService(t)

	mock.ExpectBegin()
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs("mydb").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery(
		"SELECT id, created_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL ORDER BY created_at ASC LIMIT 1 FOR UPDATE",
	).WithArgs("blog").WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"})) // empty
	mock.ExpectRollback()

	_, err := svc.Assign(context.Background(), "blog", "mydb")

	assert.ErrorIs(t, err, ErrNoBuffer)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// -- Reset -------------------------------------------------------------------

func TestReset_UnknownTemplate(t *testing.T) {
	svc, mock, _ := newTestService(t)

	_, err := svc.Reset(context.Background(), "nonexistent", "mydb")

	assert.ErrorIs(t, err, ErrUnknownTemplate)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReset_NoExistingAssignment(t *testing.T) {
	svc, mock, ops := newTestService(t)

	const (
		recID    = "550e8400-e29b-41d4-a716-446655440000"
		dbName   = "myfeature_test"
		template = "blog"
	)
	createdAt := time.Now().Add(-5 * time.Minute)

	var renamedFrom, renamedTo string
	ops.renameDB = func(_ context.Context, src, dst string) error {
		renamedFrom, renamedTo = src, dst
		return nil
	}

	// Reset: no existing assignment
	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs(dbName).WillReturnRows(recordCols()) // empty → no existing

	// Assign path
	mock.ExpectBegin()
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs(dbName).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery(
		"SELECT id, created_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL ORDER BY created_at ASC LIMIT 1 FOR UPDATE",
	).WithArgs(template).WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(recID, createdAt))
	mock.ExpectCommit()
	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET db_name = ?, assigned_at = ?, last_extended_at = ? WHERE id = ?",
	).WithArgs(dbName, sqlmock.AnyArg(), sqlmock.AnyArg(), recID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	rec, err := svc.Reset(context.Background(), template, dbName)

	require.NoError(t, err)
	assert.Equal(t, dbName, *rec.DBName)
	assert.Equal(t, template+"_"+strings.ReplaceAll(recID, "-", ""), renamedFrom)
	assert.Equal(t, dbName, renamedTo)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReset_DropsExistingAndAssignsFresh(t *testing.T) {
	svc, mock, ops := newTestService(t)

	const (
		oldRecID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		newRecID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		dbName   = "myfeature_test"
		template = "blog"
	)
	now := time.Now()

	var droppedDB string
	ops.dropDB = func(_ context.Context, name string) error {
		droppedDB = name
		return nil
	}
	var renamedTo string
	ops.renameDB = func(_ context.Context, _, dst string) error {
		renamedTo = dst
		return nil
	}

	// Reset: existing assignment found
	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs(dbName).WillReturnRows(
		recordCols().AddRow(oldRecID, template, dbName, now.Add(-2*time.Hour), now.Add(-1*time.Hour), now.Add(-1*time.Hour), nil),
	)
	// Drop old + soft-delete
	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET deleted_at = ? WHERE id = ?",
	).WithArgs(sqlmock.AnyArg(), oldRecID).WillReturnResult(sqlmock.NewResult(0, 1))

	// Assign path
	mock.ExpectBegin()
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs(dbName).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery(
		"SELECT id, created_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL ORDER BY created_at ASC LIMIT 1 FOR UPDATE",
	).WithArgs(template).WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(newRecID, now.Add(-10*time.Minute)))
	mock.ExpectCommit()
	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET db_name = ?, assigned_at = ?, last_extended_at = ? WHERE id = ?",
	).WithArgs(dbName, sqlmock.AnyArg(), sqlmock.AnyArg(), newRecID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	rec, err := svc.Reset(context.Background(), template, dbName)

	require.NoError(t, err)
	assert.Equal(t, dbName, *rec.DBName)
	assert.Equal(t, newRecID, rec.ID)
	assert.Equal(t, dbName, droppedDB) // physical name of assigned record = dbName
	assert.Equal(t, dbName, renamedTo)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReset_NoBuffer(t *testing.T) {
	svc, mock, ops := newTestService(t)

	const (
		oldRecID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		dbName   = "myfeature_test"
		template = "blog"
	)
	now := time.Now()

	ops.dropDB = func(_ context.Context, _ string) error { return nil }

	// Reset: existing assignment found
	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs(dbName).WillReturnRows(
		recordCols().AddRow(oldRecID, template, dbName, now.Add(-2*time.Hour), now.Add(-1*time.Hour), now.Add(-1*time.Hour), nil),
	)
	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET deleted_at = ? WHERE id = ?",
	).WithArgs(sqlmock.AnyArg(), oldRecID).WillReturnResult(sqlmock.NewResult(0, 1))

	// Assign path: buffer empty
	mock.ExpectBegin()
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
	).WithArgs(dbName).WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery(
		"SELECT id, created_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL ORDER BY created_at ASC LIMIT 1 FOR UPDATE",
	).WithArgs(template).WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"})) // empty
	mock.ExpectRollback()

	_, err := svc.Reset(context.Background(), template, dbName)

	assert.ErrorIs(t, err, ErrNoBuffer)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// -- Clean -------------------------------------------------------------------

func TestClean_DropsOnlyExpiredDatabases(t *testing.T) {
	svc, mock, ops := newTestService(t)

	now := time.Now()
	dbNameExpired := "expired_db"
	dbNameActive := "active_db"

	rows := recordCols().
		AddRow("id-exp", "blog", dbNameExpired, now.Add(-30*time.Hour), now.Add(-26*time.Hour), now.Add(-25*time.Hour), nil).
		AddRow("id-act", "blog", dbNameActive, now.Add(-2*time.Hour), now.Add(-1*time.Hour), now.Add(-1*time.Hour), nil)

	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE db_name IS NOT NULL AND deleted_at IS NULL",
	).WillReturnRows(rows)

	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET deleted_at = ? WHERE id = ?",
	).WithArgs(sqlmock.AnyArg(), "id-exp").WillReturnResult(sqlmock.NewResult(0, 1))

	var dropped []string
	ops.dropDB = func(_ context.Context, name string) error {
		dropped = append(dropped, name)
		return nil
	}

	n, err := svc.Clean(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{dbNameExpired}, dropped)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClean_NothingToClean(t *testing.T) {
	svc, mock, _ := newTestService(t)

	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE db_name IS NOT NULL AND deleted_at IS NULL",
	).WillReturnRows(recordCols())

	n, err := svc.Clean(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// -- Extend ------------------------------------------------------------------

func TestExtend_Success(t *testing.T) {
	svc, mock, _ := newTestService(t)

	now := time.Now()
	dbName := "my_db"

	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET last_extended_at = ? WHERE template_name = ? AND db_name = ? AND deleted_at IS NULL",
	).WithArgs(sqlmock.AnyArg(), "blog", dbName).WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name = ? AND deleted_at IS NULL",
	).WithArgs("blog", dbName).WillReturnRows(
		recordCols().AddRow("id-1", "blog", dbName, now.Add(-1*time.Hour), now.Add(-30*time.Minute), now, nil),
	)

	rec, err := svc.Extend(context.Background(), "blog", dbName)

	require.NoError(t, err)
	assert.Equal(t, dbName, *rec.DBName)
	assert.NotNil(t, rec.LastExtendedAt)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExtend_UnknownTemplate(t *testing.T) {
	svc, mock, _ := newTestService(t)

	_, err := svc.Extend(context.Background(), "nonexistent", "my_db")

	assert.ErrorIs(t, err, ErrUnknownTemplate)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestExtend_NotFound(t *testing.T) {
	svc, mock, _ := newTestService(t)

	mock.ExpectExec(
		"UPDATE `_dbshuffle`.`databases` SET last_extended_at = ? WHERE template_name = ? AND db_name = ? AND deleted_at IS NULL",
	).WithArgs(sqlmock.AnyArg(), "blog", "missing_db").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := svc.Extend(context.Background(), "blog", "missing_db")

	assert.ErrorIs(t, err, ErrNotFound)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// -- Refill ------------------------------------------------------------------

func TestRefill_CreatesUpToBufferSize(t *testing.T) {
	svc, mock, ops := newTestService(t)

	// 1 exists, buffer=3 → need 2 more
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL",
	).WithArgs("blog").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	// Two CopyDB + INSERT pairs
	for i := 0; i < 2; i++ {
		mock.ExpectExec(
			"INSERT INTO `_dbshuffle`.`databases` (id, template_name, created_at) VALUES (?, ?, ?)",
		).WithArgs(sqlmock.AnyArg(), "blog", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	var copySrcs []string
	ops.copyDB = func(_ context.Context, src, dst string) error {
		copySrcs = append(copySrcs, src)
		assert.True(t, strings.HasPrefix(dst, "blog_"), "buffer db name should start with template name")
		return nil
	}

	n, err := svc.Refill(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []string{"_template_blog", "_template_blog"}, copySrcs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRefill_DoesNotCountSoftDeleted(t *testing.T) {
	svc, mock, ops := newTestService(t)

	// Simulate 2 live buffer DBs; soft-deleted ones must not be counted.
	// buffer=3, so 1 new copy should be created.
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL",
	).WithArgs("blog").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(2))

	mock.ExpectExec(
		"INSERT INTO `_dbshuffle`.`databases` (id, template_name, created_at) VALUES (?, ?, ?)",
	).WithArgs(sqlmock.AnyArg(), "blog", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	var copyCount int
	ops.copyDB = func(_ context.Context, _, _ string) error {
		copyCount++
		return nil
	}

	n, err := svc.Refill(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, copyCount)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRefill_SkipsWhenBufferFull(t *testing.T) {
	svc, mock, ops := newTestService(t)

	// 3 exists, buffer=3 → nothing to do
	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL",
	).WithArgs("blog").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(3))

	var copyCount int
	ops.copyDB = func(_ context.Context, _, _ string) error {
		copyCount++
		return nil
	}

	n, err := svc.Refill(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, copyCount)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRefill_FromPath(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ops := &mockOps{}
	svc := &ShuffleService{
		db: db,
		ops: ops,
		cfg: &config.Config{
			DBTemplates: map[string]config.Template{
				"wiki": {FromPath: "/templates/wiki", Buffer: 2, Expire: 12},
			},
		},
	}

	mock.ExpectQuery(
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL",
	).WithArgs("wiki").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	mock.ExpectExec(
		"INSERT INTO `_dbshuffle`.`databases` (id, template_name, created_at) VALUES (?, ?, ?)",
	).WithArgs(sqlmock.AnyArg(), "wiki", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	var pathsUsed []string
	ops.createDBFromPath = func(_ context.Context, dst, path string) error {
		pathsUsed = append(pathsUsed, path)
		assert.True(t, strings.HasPrefix(dst, "wiki_"))
		return nil
	}

	n, err := svc.Refill(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, []string{"/templates/wiki"}, pathsUsed)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// -- DBRecord helpers --------------------------------------------------------

func TestDBRecord_IsExpired(t *testing.T) {
	past25h := time.Now().Add(-25 * time.Hour)
	past1h := time.Now().Add(-1 * time.Hour)

	expired := DBRecord{LastExtendedAt: &past25h}
	active := DBRecord{LastExtendedAt: &past1h}
	unassigned := DBRecord{}

	assert.True(t, expired.IsExpired(24))
	assert.False(t, active.IsExpired(24))
	assert.False(t, unassigned.IsExpired(24))
}

func TestDBRecord_PhysicalName(t *testing.T) {
	id := "550e8400-e29b-41d4-a716-446655440000"
	dbName := "my_db"

	buffer := DBRecord{ID: id, TemplateName: "blog"}
	assigned := DBRecord{ID: id, TemplateName: "blog", DBName: &dbName}

	assert.Equal(t, "blog_550e8400e29b41d4a716446655440000", buffer.PhysicalName())
	assert.Equal(t, "my_db", assigned.PhysicalName())
}

// helper to get sql.ErrNoRows from sqlmock
func withErrNoRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "created_at"})
}
