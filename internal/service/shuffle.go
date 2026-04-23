package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/obukhov/dbshuffle/internal/config"
	dbops "github.com/obukhov/dbshuffle/internal/db"
)

var tracer = otel.Tracer("github.com/obukhov/dbshuffle/internal/service")
var meter = otel.Meter("github.com/obukhov/dbshuffle/internal/service")

type DBRecord struct {
	ID             string     `json:"id"`
	TemplateName   string     `json:"template_name"`
	DBName         *string    `json:"db_name,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	AssignedAt     *time.Time `json:"assigned_at,omitempty"`
	LastExtendedAt *time.Time `json:"last_extended_at,omitempty"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty"`
}

func (r *DBRecord) IsAssigned() bool { return r.DBName != nil }

func (r *DBRecord) ExpiresAt(expireHours int) *time.Time {
	if r.LastExtendedAt == nil {
		return nil
	}
	t := r.LastExtendedAt.Add(time.Duration(expireHours) * time.Hour)
	return &t
}

func (r *DBRecord) IsExpired(expireHours int) bool {
	t := r.ExpiresAt(expireHours)
	return t != nil && t.Before(time.Now())
}

// PhysicalName returns the current MySQL database name for this record.
func (r *DBRecord) PhysicalName() string {
	if r.DBName != nil {
		return *r.DBName
	}
	return r.TemplateName + "_" + strings.ReplaceAll(r.ID, "-", "")
}

type StatusReport struct {
	Template    string     `json:"template"`
	ExpireHours int        `json:"expire_hours"`
	Buffer      []DBRecord `json:"buffer"`
	Assigned    []DBRecord `json:"assigned"`
	Expired     []DBRecord `json:"expired"`
}

type dbOperations interface {
	CopyDB(ctx context.Context, src, dst string) error
	RenameDB(ctx context.Context, src, dst string) error
	DropDB(ctx context.Context, name string) error
	CreateDBFromPath(ctx context.Context, dst, path string) error
}

type ShuffleService struct {
	db  *sql.DB
	ops dbOperations
	cfg *config.Config
}

func NewShuffleService(db *sql.DB, cfg *config.Config) *ShuffleService {
	return &ShuffleService{db: db, ops: dbops.NewOperations(db), cfg: cfg}
}

// RegisterMetrics registers observable gauges for buffer and assigned database counts
// per template. Both are observed in a single query per collection cycle.
func (s *ShuffleService) RegisterMetrics() error {
	bufferGauge, err := meter.Int64ObservableGauge("dbshuffle.buffer.size",
		otelmetric.WithDescription("Number of ready buffer copies per template"),
		otelmetric.WithUnit("{databases}"),
	)
	if err != nil {
		return fmt.Errorf("create buffer gauge: %w", err)
	}
	assignedGauge, err := meter.Int64ObservableGauge("dbshuffle.assigned.size",
		otelmetric.WithDescription("Number of assigned databases per template"),
		otelmetric.WithUnit("{databases}"),
	)
	if err != nil {
		return fmt.Errorf("create assigned gauge: %w", err)
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, o otelmetric.Observer) error {
		rows, err := s.db.QueryContext(ctx,
			"SELECT template_name,"+
				" SUM(CASE WHEN db_name IS NULL THEN 1 ELSE 0 END),"+
				" SUM(CASE WHEN db_name IS NOT NULL THEN 1 ELSE 0 END)"+
				" FROM `_dbshuffle`.`databases` WHERE deleted_at IS NULL GROUP BY template_name",
		)
		if err != nil {
			return fmt.Errorf("observe gauges: %w", err)
		}
		defer rows.Close()

		seen := make(map[string]struct{})
		for rows.Next() {
			var tmpl string
			var buf, assigned int64
			if err := rows.Scan(&tmpl, &buf, &assigned); err != nil {
				return fmt.Errorf("scan gauge row: %w", err)
			}
			attrs := otelmetric.WithAttributes(attribute.String("template", tmpl))
			o.ObserveInt64(bufferGauge, buf, attrs)
			o.ObserveInt64(assignedGauge, assigned, attrs)
			seen[tmpl] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("gauge rows error: %w", err)
		}
		// Emit zero for templates that have no rows yet.
		for name := range s.cfg.DBTemplates {
			if _, ok := seen[name]; !ok {
				attrs := otelmetric.WithAttributes(attribute.String("template", name))
				o.ObserveInt64(bufferGauge, 0, attrs)
				o.ObserveInt64(assignedGauge, 0, attrs)
			}
		}
		return nil
	}, bufferGauge, assignedGauge)
	if err != nil {
		return fmt.Errorf("register gauge callback: %w", err)
	}
	return nil
}

func (s *ShuffleService) ExpireHours(templateName string) int {
	return s.cfg.DBTemplates[templateName].Expire
}

func (s *ShuffleService) Status(ctx context.Context) (reports []StatusReport, err error) {
	ctx, span := tracer.Start(ctx, "Status")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	var rows *sql.Rows
	rows, err = s.db.QueryContext(ctx,
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE deleted_at IS NULL ORDER BY template_name, created_at",
	)
	if err != nil {
		return nil, fmt.Errorf("query databases: %w", err)
	}
	defer rows.Close()

	byTemplate := map[string]*StatusReport{}
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		rep, ok := byTemplate[r.TemplateName]
		if !ok {
			tmplCfg := s.cfg.DBTemplates[r.TemplateName]
			rep = &StatusReport{Template: r.TemplateName, ExpireHours: tmplCfg.Expire}
			byTemplate[r.TemplateName] = rep
		}
		tmpl := s.cfg.DBTemplates[r.TemplateName]
		switch {
		case !r.IsAssigned():
			rep.Buffer = append(rep.Buffer, r)
		case r.IsExpired(tmpl.Expire):
			rep.Expired = append(rep.Expired, r)
		default:
			rep.Assigned = append(rep.Assigned, r)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	reports = make([]StatusReport, 0, len(byTemplate))
	for _, r := range byTemplate {
		reports = append(reports, *r)
	}
	return reports, nil
}

func (s *ShuffleService) Assign(ctx context.Context, templateName, dbName string) (result *DBRecord, err error) {
	ctx, span := tracer.Start(ctx, "Assign")
	span.SetAttributes(attribute.String("template", templateName), attribute.String("db_name", dbName))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	tmpl, ok := s.cfg.DBTemplates[templateName]
	if !ok {
		return nil, ErrUnknownTemplate
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var inUse int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL", dbName,
	).Scan(&inUse); err != nil {
		return nil, fmt.Errorf("check db name: %w", err)
	}
	if inUse > 0 {
		return nil, ErrAlreadyAssigned
	}

	var rec DBRecord
	rec.TemplateName = templateName
	err = tx.QueryRowContext(ctx,
		"SELECT id, created_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL ORDER BY created_at ASC LIMIT 1 FOR UPDATE",
		templateName,
	).Scan(&rec.ID, &rec.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, ErrNoBuffer
	}
	if err != nil {
		return nil, fmt.Errorf("pick buffer db: %w", err)
	}

	// Commit before DDL — MySQL DDL causes an implicit commit anyway
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	if err := s.ops.RenameDB(ctx, rec.PhysicalName(), dbName); err != nil {
		return nil, fmt.Errorf("rename db: %w", err)
	}

	now := time.Now()
	if _, err := s.db.ExecContext(ctx,
		"UPDATE `_dbshuffle`.`databases` SET db_name = ?, assigned_at = ?, last_extended_at = ? WHERE id = ?",
		dbName, now, now, rec.ID,
	); err != nil {
		return nil, fmt.Errorf("update record: %w", err)
	}

	rec.TemplateName = templateName
	rec.DBName = &dbName
	rec.AssignedAt = &now
	rec.LastExtendedAt = &now
	_ = tmpl
	return &rec, nil
}

func (s *ShuffleService) Clean(ctx context.Context) (cleaned int, err error) {
	ctx, span := tracer.Start(ctx, "Clean")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	var rows *sql.Rows
	rows, err = s.db.QueryContext(ctx,
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE db_name IS NOT NULL AND deleted_at IS NULL",
	)
	if err != nil {
		return 0, fmt.Errorf("query assigned: %w", err)
	}
	defer rows.Close()

	var toClean []DBRecord
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return 0, err
		}
		tmpl := s.cfg.DBTemplates[r.TemplateName]
		if r.IsExpired(tmpl.Expire) {
			toClean = append(toClean, r)
		}
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}

	for _, r := range toClean {
		if err := s.ops.DropDB(ctx, r.PhysicalName()); err != nil {
			return cleaned, fmt.Errorf("drop %s: %w", r.PhysicalName(), err)
		}
		if _, err := s.db.ExecContext(ctx,
			"UPDATE `_dbshuffle`.`databases` SET deleted_at = ? WHERE id = ?", time.Now(), r.ID,
		); err != nil {
			return cleaned, fmt.Errorf("soft-delete record %s: %w", r.ID, err)
		}
		cleaned++
	}
	return cleaned, nil
}

func (s *ShuffleService) Extend(ctx context.Context, templateName, dbName string) (result *DBRecord, err error) {
	ctx, span := tracer.Start(ctx, "Extend")
	span.SetAttributes(attribute.String("template", templateName), attribute.String("db_name", dbName))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if _, ok := s.cfg.DBTemplates[templateName]; !ok {
		return nil, ErrUnknownTemplate
	}

	now := time.Now()
	res, err := s.db.ExecContext(ctx,
		"UPDATE `_dbshuffle`.`databases` SET last_extended_at = ? WHERE template_name = ? AND db_name = ? AND deleted_at IS NULL",
		now, templateName, dbName,
	)
	if err != nil {
		return nil, fmt.Errorf("extend db: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return nil, ErrNotFound
	}

	var rec DBRecord
	err = s.db.QueryRowContext(ctx,
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name = ? AND deleted_at IS NULL",
		templateName, dbName,
	).Scan(&rec.ID, &rec.TemplateName, &rec.DBName, &rec.CreatedAt, &rec.AssignedAt, &rec.LastExtendedAt, &rec.DeletedAt)
	if err != nil {
		return nil, fmt.Errorf("fetch record: %w", err)
	}
	return &rec, nil
}

func (s *ShuffleService) Reset(ctx context.Context, templateName, dbName string) (result *DBRecord, err error) {
	ctx, span := tracer.Start(ctx, "Reset")
	span.SetAttributes(attribute.String("template", templateName), attribute.String("db_name", dbName))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if _, ok := s.cfg.DBTemplates[templateName]; !ok {
		return nil, ErrUnknownTemplate
	}

	var existing DBRecord
	err = s.db.QueryRowContext(ctx,
		"SELECT id, template_name, db_name, created_at, assigned_at, last_extended_at, deleted_at FROM `_dbshuffle`.`databases` WHERE db_name = ? AND deleted_at IS NULL",
		dbName,
	).Scan(&existing.ID, &existing.TemplateName, &existing.DBName, &existing.CreatedAt, &existing.AssignedAt, &existing.LastExtendedAt, &existing.DeletedAt)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("check existing assignment: %w", err)
	}
	if err == nil {
		if err := s.ops.DropDB(ctx, existing.PhysicalName()); err != nil {
			return nil, fmt.Errorf("drop existing db: %w", err)
		}
		if _, err := s.db.ExecContext(ctx,
			"UPDATE `_dbshuffle`.`databases` SET deleted_at = ? WHERE id = ?",
			time.Now(), existing.ID,
		); err != nil {
			return nil, fmt.Errorf("soft-delete record: %w", err)
		}
	}

	return s.Assign(ctx, templateName, dbName)
}

func (s *ShuffleService) Refill(ctx context.Context) (created int, err error) {
	ctx, span := tracer.Start(ctx, "Refill")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	for name, tmpl := range s.cfg.DBTemplates {
		var current int
		if err := s.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM `_dbshuffle`.`databases` WHERE template_name = ? AND db_name IS NULL AND deleted_at IS NULL", name,
		).Scan(&current); err != nil {
			return created, fmt.Errorf("count buffer for %s: %w", name, err)
		}

		for i := current; i < tmpl.Buffer; i++ {
			id := uuid.New()
			bufName := name + "_" + strings.ReplaceAll(id.String(), "-", "")
			if tmpl.FromPath != "" {
				if err := s.ops.CreateDBFromPath(ctx, bufName, tmpl.FromPath); err != nil {
					return created, fmt.Errorf("create from path %s: %w", name, err)
				}
			} else {
				if err := s.ops.CopyDB(ctx, tmpl.FromDB, bufName); err != nil {
					return created, fmt.Errorf("copy template %s: %w", name, err)
				}
			}
			if _, err := s.db.ExecContext(ctx,
				"INSERT INTO `_dbshuffle`.`databases` (id, template_name, created_at) VALUES (?, ?, ?)",
				id.String(), name, time.Now(),
			); err != nil {
				return created, fmt.Errorf("insert record: %w", err)
			}
			created++
		}
	}
	return created, nil
}

func scanRecord(rows *sql.Rows) (DBRecord, error) {
	var r DBRecord
	err := rows.Scan(&r.ID, &r.TemplateName, &r.DBName, &r.CreatedAt, &r.AssignedAt, &r.LastExtendedAt, &r.DeletedAt)
	if err != nil {
		return r, fmt.Errorf("scan record: %w", err)
	}
	return r, nil
}
