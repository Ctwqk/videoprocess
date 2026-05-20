package store

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a pgx pool and exposes the query methods the Go API needs.
// Field/column names mirror `backend/app/models/*.py` exactly so JSON
// produced from these rows matches the Python FastAPI response shapes.
type Store struct {
	Pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() {
	if s != nil && s.Pool != nil {
		s.Pool.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.Pool == nil {
		return pgx.ErrNoRows
	}
	return s.Pool.Ping(ctx)
}

// PipelineRow mirrors backend/app/schemas/pipeline.py PipelineResponse.
type PipelineRow struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	Definition   any       `json:"definition"`
	IsTemplate   bool      `json:"is_template"`
	TemplateTags []string  `json:"template_tags"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	Version      int       `json:"version"`
}

// JobRow mirrors backend/app/schemas/job.py JobResponse.
type JobRow struct {
	ID                string     `json:"id"`
	PipelineID        string     `json:"pipeline_id"`
	Status            string     `json:"status"`
	SubmittedAt       time.Time  `json:"submitted_at"`
	StartedAt         *time.Time `json:"started_at"`
	CompletedAt       *time.Time `json:"completed_at"`
	ErrorMessage      *string    `json:"error_message"`
	SubmittedBy       string     `json:"submitted_by"`
	RetryCount        int        `json:"retry_count"`
	OrchestratorOwner string     `json:"orchestrator_owner"`
}

// AssetRow mirrors backend/app/schemas/asset.py AssetResponse.
type AssetRow struct {
	ID           string    `json:"id"`
	Filename     string    `json:"filename"`
	OriginalName string    `json:"original_name"`
	MimeType     *string   `json:"mime_type"`
	FileSize     *int64    `json:"file_size"`
	MediaInfo    any       `json:"media_info"`
	UploadedAt   time.Time `json:"uploaded_at"`
}

// PageOptions captures FastAPI `skip`/`limit` semantics with `limit` clamped
// to 100 the same way `Query(default=50, le=100)` is.
type PageOptions struct {
	Skip  int
	Limit int
}

func (p PageOptions) normalized() (int, int) {
	limit := p.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	skip := p.Skip
	if skip < 0 {
		skip = 0
	}
	return skip, limit
}

// ListPipelines paginates the pipelines table, optionally filtering by
// is_template. Order matches Python: most recently created first.
func (s *Store) ListPipelines(ctx context.Context, opts PageOptions, isTemplate *bool) ([]PipelineRow, int, error) {
	skip, limit := opts.normalized()
	args := []any{}
	where := ""
	if isTemplate != nil {
		args = append(args, *isTemplate)
		where = " WHERE is_template = $1"
	}
	var total int
	if err := s.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM pipelines"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, skip)
	limitArg := strconv.Itoa(len(args) - 1)
	offsetArg := strconv.Itoa(len(args))
	query := "SELECT id, name, description, definition, is_template, template_tags, " +
		"created_at, updated_at, version FROM pipelines" + where +
		" ORDER BY created_at DESC LIMIT $" + limitArg + " OFFSET $" + offsetArg
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]PipelineRow, 0)
	for rows.Next() {
		var row PipelineRow
		var id [16]byte
		if err := rows.Scan(&id, &row.Name, &row.Description, &row.Definition, &row.IsTemplate,
			&row.TemplateTags, &row.CreatedAt, &row.UpdatedAt, &row.Version); err != nil {
			return nil, 0, err
		}
		row.ID = uuidString(id)
		if row.TemplateTags == nil {
			row.TemplateTags = []string{}
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListJobs paginates jobs ordered by submitted_at DESC.
func (s *Store) ListJobs(ctx context.Context, opts PageOptions, pipelineID *string, status *string) ([]JobRow, int, error) {
	skip, limit := opts.normalized()
	args := []any{}
	conditions := ""
	if pipelineID != nil {
		args = append(args, *pipelineID)
		conditions += " AND pipeline_id = $" + strconv.Itoa(len(args))
	}
	if status != nil {
		args = append(args, *status)
		conditions += " AND status = $" + strconv.Itoa(len(args))
	}
	where := ""
	if conditions != "" {
		where = " WHERE 1=1" + conditions
	}
	var total int
	if err := s.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, skip)
	limitArg := strconv.Itoa(len(args) - 1)
	offsetArg := strconv.Itoa(len(args))
	query := "SELECT id, pipeline_id, status::text, submitted_at, started_at, completed_at, " +
		"error_message, submitted_by, retry_count, orchestrator_owner FROM jobs" + where +
		" ORDER BY submitted_at DESC LIMIT $" + limitArg + " OFFSET $" + offsetArg
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]JobRow, 0)
	for rows.Next() {
		var row JobRow
		var id [16]byte
		var pipelineUUID [16]byte
		if err := rows.Scan(&id, &pipelineUUID, &row.Status, &row.SubmittedAt, &row.StartedAt,
			&row.CompletedAt, &row.ErrorMessage, &row.SubmittedBy, &row.RetryCount, &row.OrchestratorOwner); err != nil {
			return nil, 0, err
		}
		row.ID = uuidString(id)
		row.PipelineID = uuidString(pipelineUUID)
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListAssets paginates assets ordered by uploaded_at DESC.
func (s *Store) ListAssets(ctx context.Context, opts PageOptions) ([]AssetRow, int, error) {
	skip, limit := opts.normalized()
	var total int
	if err := s.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM assets").Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.Pool.Query(
		ctx,
		"SELECT id, filename, original_name, mime_type, file_size, media_info, uploaded_at "+
			"FROM assets ORDER BY uploaded_at DESC LIMIT $1 OFFSET $2",
		limit, skip,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]AssetRow, 0)
	for rows.Next() {
		var row AssetRow
		var id [16]byte
		if err := rows.Scan(&id, &row.Filename, &row.OriginalName, &row.MimeType, &row.FileSize,
			&row.MediaInfo, &row.UploadedAt); err != nil {
			return nil, 0, err
		}
		row.ID = uuidString(id)
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// CountByQuery is a small helper for parity smoke checks/tests that runs an
// arbitrary scalar count query against the pool.
func (s *Store) CountByQuery(ctx context.Context, sql string, args ...any) (int, error) {
	var n int
	err := s.Pool.QueryRow(ctx, sql, args...).Scan(&n)
	if err != nil && err != pgx.ErrNoRows {
		return 0, err
	}
	return n, nil
}

// uuidString formats a Postgres uuid `[16]byte` as canonical lowercase hex.
func uuidString(b [16]byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 36)
	encode := func(off int, src []byte) int {
		for _, v := range src {
			out[off] = hex[v>>4]
			out[off+1] = hex[v&0x0f]
			off += 2
		}
		return off
	}
	off := 0
	off = encode(off, b[0:4])
	out[off] = '-'
	off++
	off = encode(off, b[4:6])
	out[off] = '-'
	off++
	off = encode(off, b[6:8])
	out[off] = '-'
	off++
	off = encode(off, b[8:10])
	out[off] = '-'
	off++
	encode(off, b[10:16])
	return string(out)
}
