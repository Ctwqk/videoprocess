package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/pipeline"
	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

type pipelineCreateRequest struct {
	Name         string                       `json:"name"`
	Description  string                       `json:"description"`
	Definition   contracts.PipelineDefinition `json:"definition"`
	IsTemplate   bool                         `json:"is_template"`
	TemplateTags []string                     `json:"template_tags"`
}

type pipelineUpdateRequest struct {
	Name         *string                       `json:"name"`
	Description  *string                       `json:"description"`
	Definition   *contracts.PipelineDefinition `json:"definition"`
	IsTemplate   *bool                         `json:"is_template"`
	TemplateTags *[]string                     `json:"template_tags"`
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	var req pipelineCreateRequest
	if !decodeWriteJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline request"})
		return
	}
	input, ok := s.pipelineInputFromDefinition(w, req.Name, req.Description, req.Definition, req.IsTemplate, req.TemplateTags)
	if !ok {
		return
	}
	s.withWriteStore(w, func(st *store.Store) {
		row, err := st.CreatePipeline(r.Context(), input)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, row)
	})
}

func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request) {
	var req pipelineUpdateRequest
	if !decodeWriteJSON(w, r, &req) {
		return
	}
	id := chi.URLParam(r, "pipelineID")
	s.withWriteStore(w, func(st *store.Store) {
		existing, err := st.GetPipeline(r.Context(), id)
		if err != nil {
			s.writeDetailResult(w, existing, err, "Pipeline not found")
			return
		}
		name := existing.Name
		if req.Name != nil {
			name = *req.Name
		}
		if strings.TrimSpace(name) == "" {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline request"})
			return
		}
		description := existing.Description
		if req.Description != nil {
			description = *req.Description
		}
		isTemplate := existing.IsTemplate
		if req.IsTemplate != nil {
			isTemplate = *req.IsTemplate
		}
		tags := existing.TemplateTags
		if req.TemplateTags != nil {
			tags = *req.TemplateTags
		}
		definition := existing.Definition
		if req.Definition != nil {
			definition = *req.Definition
			if !s.validateWriteDefinition(w, *req.Definition) {
				return
			}
		}
		definitionMap, err := definitionToMap(definition)
		if err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
			return
		}
		row, err := st.UpdatePipeline(r.Context(), id, store.PipelineWriteInput{
			Name:         name,
			Description:  description,
			Definition:   definitionMap,
			IsTemplate:   isTemplate,
			TemplateTags: tags,
		})
		if err != nil {
			s.writeDetailResult(w, row, err, "Pipeline not found")
			return
		}
		writeJSON(w, http.StatusOK, row)
	})
}

func (s *Server) deletePipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	s.withWriteStore(w, func(st *store.Store) {
		err := st.DeletePipeline(r.Context(), id)
		if err == nil {
			writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
			return
		}
		if store.IsNotFound(err) {
			notFound(w, "Pipeline not found")
			return
		}
		if store.IsConflict(err) {
			conflict(w, strings.TrimPrefix(err.Error(), store.ErrConflict.Error()+": "))
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
	})
}

func (s *Server) duplicatePipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	s.withWriteStore(w, func(st *store.Store) {
		row, err := st.DuplicatePipeline(r.Context(), id)
		if err != nil {
			s.writeDetailResult(w, row, err, "Pipeline not found")
			return
		}
		writeJSON(w, http.StatusCreated, row)
	})
}

func (s *Server) pipelineInputFromDefinition(
	w http.ResponseWriter,
	name string,
	description string,
	def contracts.PipelineDefinition,
	isTemplate bool,
	tags []string,
) (store.PipelineWriteInput, bool) {
	if !s.validateWriteDefinition(w, def) {
		return store.PipelineWriteInput{}, false
	}
	definitionMap, err := definitionToMap(def)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
		return store.PipelineWriteInput{}, false
	}
	return store.PipelineWriteInput{
		Name:         name,
		Description:  description,
		Definition:   definitionMap,
		IsTemplate:   isTemplate,
		TemplateTags: tags,
	}, true
}

func (s *Server) validateWriteDefinition(w http.ResponseWriter, def contracts.PipelineDefinition) bool {
	if def.Nodes == nil || def.Edges == nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
		return false
	}
	result := pipeline.Validate(def)
	if result.Valid {
		return true
	}
	if validationHasUnsupportedGraph(result) {
		unsupportedWrite(w, "pipeline graph must be routed to Python because Go validation does not own this graph")
		return false
	}
	writeJSON(w, http.StatusUnprocessableEntity, result)
	return false
}

func validationHasUnsupportedGraph(result contracts.ValidationResult) bool {
	for _, err := range result.Errors {
		if err.Type == "unsupported_go_validation" {
			return true
		}
	}
	return false
}

func decodeWriteJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid request body"})
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid request body"})
		return false
	}
	return true
}

func (s *Server) withWriteStore(w http.ResponseWriter, fn func(*store.Store)) {
	if s.store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
		return
	}
	fn(s.store)
}

func definitionToMap(value any) (map[string]any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}
