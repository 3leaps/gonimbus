package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"

	apperrors "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

const (
	maxJobMetadataEntries = 32
	maxJobMetadataKeyLen  = 64
	maxJobMetadataValLen  = 512
)

type jobStore interface {
	Get(string) (*jobregistry.JobRecord, error)
	List() ([]jobregistry.JobRecord, error)
}

type jobStarter interface {
	StartIndexBuildBackground(string, string, jobregistry.BackgroundOptions) (*jobregistry.JobRecord, error)
}

type jobStopper interface {
	Stop(string, jobregistry.StopOptions) (*jobregistry.StopResult, error)
}

type JobsHandler struct {
	store   jobStore
	starter jobStarter
	stopper jobStopper
}

func NewJobsHandler(root string) *JobsHandler {
	store := jobregistry.NewStore(root)
	return &JobsHandler{
		store:   store,
		starter: jobregistry.NewExecutor(root),
		stopper: store,
	}
}

func newJobsHandlerForTest(store jobStore, starter jobStarter, stopper jobStopper) *JobsHandler {
	return &JobsHandler{store: store, starter: starter, stopper: stopper}
}

type submitJobRequest struct {
	Type         string            `json:"type"`
	ManifestPath string            `json:"manifest_path,omitempty"`
	ManifestURI  string            `json:"manifest_uri,omitempty"`
	Name         string            `json:"name,omitempty"`
	Since        string            `json:"since,omitempty"`
	Dedupe       bool              `json:"dedupe,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type jobEnvelope struct {
	Job jobregistry.JobRecord `json:"job"`
}

type jobListEnvelope struct {
	Jobs    []jobregistry.JobRecord `json:"jobs"`
	Total   int                     `json:"total"`
	HasMore bool                    `json:"has_more"`
}

type cancelJobEnvelope struct {
	JobID      string `json:"job_id"`
	Signal     string `json:"signal"`
	ForcedKill bool   `json:"forced_kill"`
	State      string `json:"state"`
}

func (h *JobsHandler) Submit(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondWithError(w, r, apperrors.WrapInvalidInput(r.Context(), err, "invalid job request JSON"))
		return
	}
	manifestPath, metadata, err := validateSubmitJobRequest(r.Context(), req)
	if err != nil {
		respondWithError(w, r, err)
		return
	}

	job, err := h.starter.StartIndexBuildBackground(manifestPath, strings.TrimSpace(req.Name), jobregistry.BackgroundOptions{
		Dedupe:   req.Dedupe,
		Since:    strings.TrimSpace(req.Since),
		JobType:  jobregistry.JobTypeIndexBuild,
		Metadata: metadata,
	})
	if err != nil {
		respondWithError(w, r, mapJobStartError(r, err))
		return
	}
	writeJSON(w, http.StatusAccepted, jobEnvelope{Job: normalizeJobRecord(*job)})
}

func (h *JobsHandler) List(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.store.List()
	if err != nil {
		respondWithError(w, r, apperrors.WrapInternal(r.Context(), err, "list jobs"))
		return
	}

	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	typeFilter := strings.TrimSpace(r.URL.Query().Get("type"))
	limit := parseJobListLimit(r.URL.Query().Get("limit"))

	filtered := make([]jobregistry.JobRecord, 0, len(jobs))
	for _, job := range jobs {
		job = normalizeJobRecord(job)
		if statusFilter != "" && string(job.State) != statusFilter {
			continue
		}
		if typeFilter != "" && job.Type != typeFilter {
			continue
		}
		filtered = append(filtered, job)
	}

	total := len(filtered)
	hasMore := false
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
		hasMore = true
	}

	writeJSON(w, http.StatusOK, jobListEnvelope{Jobs: filtered, Total: total, HasMore: hasMore})
}

func (h *JobsHandler) Status(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(chi.URLParam(r, "job_id"))
	if jobID == "" {
		respondWithError(w, r, apperrors.NewInvalidInputError("job_id is required"))
		return
	}
	job, err := h.store.Get(jobID)
	if err != nil {
		respondWithError(w, r, mapJobGetError(r, err))
		return
	}
	writeJSON(w, http.StatusOK, jobEnvelope{Job: normalizeJobRecord(*job)})
}

func (h *JobsHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(chi.URLParam(r, "job_id"))
	if jobID == "" {
		respondWithError(w, r, apperrors.NewInvalidInputError("job_id is required"))
		return
	}
	result, err := h.stopper.Stop(jobID, jobregistry.StopOptions{Signal: "term"})
	if err != nil {
		respondWithError(w, r, mapJobStopError(r, err))
		return
	}
	writeJSON(w, http.StatusOK, cancelJobEnvelope{
		JobID:      result.JobID,
		Signal:     result.Signal,
		ForcedKill: result.ForcedKill,
		State:      result.State,
	})
}

func validateSubmitJobRequest(ctx context.Context, req submitJobRequest) (string, map[string]string, error) {
	if strings.TrimSpace(req.Type) != jobregistry.JobTypeIndexBuild {
		return "", nil, apperrors.NewInvalidInputError("only type=index.build is supported")
	}
	if strings.TrimSpace(req.ManifestURI) != "" {
		return "", nil, apperrors.NewInvalidInputError("manifest_uri is not supported in this phase")
	}
	manifestPath := strings.TrimSpace(req.ManifestPath)
	if manifestPath == "" {
		return "", nil, apperrors.NewInvalidInputError("manifest_path is required")
	}
	if strings.Contains(manifestPath, "://") {
		return "", nil, apperrors.NewInvalidInputError("manifest_path must be a local absolute path")
	}
	if !filepath.IsAbs(manifestPath) {
		return "", nil, apperrors.NewInvalidInputError("manifest_path must be absolute")
	}
	cleanPath := filepath.Clean(manifestPath)
	if _, err := os.Stat(cleanPath); err != nil {
		return "", nil, apperrors.WrapValidationError(ctx, err, "manifest_path is not readable")
	}

	metadata, err := validateJobMetadata(req.Metadata)
	if err != nil {
		return "", nil, err
	}
	return cleanPath, metadata, nil
}

func validateJobMetadata(metadata map[string]string) (map[string]string, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	if len(metadata) > maxJobMetadataEntries {
		return nil, apperrors.NewInvalidInputError(fmt.Sprintf("metadata may contain at most %d entries", maxJobMetadataEntries))
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, apperrors.NewInvalidInputError("metadata keys must be non-empty")
		}
		if len(key) > maxJobMetadataKeyLen {
			return nil, apperrors.NewInvalidInputError(fmt.Sprintf("metadata keys must be <= %d bytes", maxJobMetadataKeyLen))
		}
		if len(value) > maxJobMetadataValLen {
			return nil, apperrors.NewInvalidInputError(fmt.Sprintf("metadata values must be <= %d bytes", maxJobMetadataValLen))
		}
		out[key] = value
	}
	return out, nil
}

func parseJobListLimit(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	var limit int
	if _, err := fmt.Sscanf(raw, "%d", &limit); err != nil || limit < 0 {
		return 0
	}
	return limit
}

func normalizeJobRecord(job jobregistry.JobRecord) jobregistry.JobRecord {
	if strings.TrimSpace(job.Type) == "" {
		job.Type = jobregistry.JobTypeIndexBuild
	}
	return job
}

func mapJobStartError(r *http.Request, err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "duplicate running job"):
		return apperrors.WrapConflict(r.Context(), err, "duplicate running job")
	case strings.Contains(msg, "manifest not found"), strings.Contains(msg, "manifest path is required"):
		return apperrors.WrapValidationError(r.Context(), err, "invalid manifest_path")
	default:
		return apperrors.WrapInternal(r.Context(), err, "submit job")
	}
}

func mapJobGetError(r *http.Request, err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return apperrors.WrapNotFound(r.Context(), err, "job not found")
	}
	return apperrors.WrapInternal(r.Context(), err, "read job")
}

func mapJobStopError(r *http.Request, err error) error {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return apperrors.WrapNotFound(r.Context(), err, "job not found")
	case errors.Is(err, jobregistry.ErrJobNoPID), errors.Is(err, jobregistry.ErrJobNotRunning):
		return apperrors.WrapConflict(r.Context(), err, "job is not running")
	default:
		return apperrors.WrapInternal(r.Context(), err, "cancel job")
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
