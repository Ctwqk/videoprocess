package store

import (
	"context"
	"fmt"
)

type PipelineWriteInput struct {
	Name         string
	Description  string
	Definition   map[string]any
	IsTemplate   bool
	TemplateTags []string
}

func (s *Store) CreatePipeline(ctx context.Context, in PipelineWriteInput) (PipelineRow, error) {
	return s.scanPipelineRow(ctx, `
		INSERT INTO pipelines (name, description, definition, is_template, template_tags)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, name, description, definition, is_template, template_tags,
		          created_at, updated_at, version
	`, in.Name, in.Description, in.Definition, in.IsTemplate, normalizeTags(in.TemplateTags))
}

func (s *Store) UpdatePipeline(ctx context.Context, id string, in PipelineWriteInput) (PipelineRow, error) {
	row, err := s.scanPipelineRow(ctx, `
		UPDATE pipelines
		SET name = $2,
		    description = $3,
		    definition = $4,
		    is_template = $5,
		    template_tags = $6,
		    version = version + 1,
		    updated_at = NOW()
		WHERE id = $1
		RETURNING id, name, description, definition, is_template, template_tags,
		          created_at, updated_at, version
	`, id, in.Name, in.Description, in.Definition, in.IsTemplate, normalizeTags(in.TemplateTags))
	if err != nil {
		return PipelineRow{}, err
	}
	return row, nil
}

func (s *Store) DeletePipeline(ctx context.Context, id string) error {
	var isTemplate bool
	if err := s.Pool.QueryRow(ctx, "SELECT is_template FROM pipelines WHERE id = $1", id).Scan(&isTemplate); err != nil {
		return err
	}
	var referencingJobs int
	if err := s.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE pipeline_id = $1", id).Scan(&referencingJobs); err != nil {
		return err
	}
	if referencingJobs > 0 {
		if !isTemplate {
			return fmt.Errorf("%w: pipeline is referenced by existing jobs and cannot be deleted", ErrConflict)
		}
		_, err := s.Pool.Exec(ctx, `
			UPDATE pipelines
			SET is_template = false, template_tags = '{}', version = version + 1, updated_at = NOW()
			WHERE id = $1
		`, id)
		return err
	}
	tag, err := s.Pool.Exec(ctx, "DELETE FROM pipelines WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pipeline not found")
	}
	return nil
}

func (s *Store) DuplicatePipeline(ctx context.Context, id string) (PipelineRow, error) {
	return s.scanPipelineRow(ctx, `
		INSERT INTO pipelines (name, description, definition, is_template, template_tags)
		SELECT name || ' (copy)', description, definition, false, template_tags
		FROM pipelines
		WHERE id = $1
		RETURNING id, name, description, definition, is_template, template_tags,
		          created_at, updated_at, version
	`, id)
}

func (s *Store) scanPipelineRow(ctx context.Context, query string, args ...any) (PipelineRow, error) {
	var row PipelineRow
	var id [16]byte
	err := s.Pool.QueryRow(ctx, query, args...).Scan(
		&id,
		&row.Name,
		&row.Description,
		&row.Definition,
		&row.IsTemplate,
		&row.TemplateTags,
		&row.CreatedAt,
		&row.UpdatedAt,
		&row.Version,
	)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(id)
	if row.TemplateTags == nil {
		row.TemplateTags = []string{}
	}
	return row, nil
}

func normalizeTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}
