package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	apperrors "github.com/3leaps/gonimbus/internal/errors"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func TestJobsHandlerSubmitStartsIndexBuild(t *testing.T) {
	manifestPath := writeJobAPITestManifest(t)
	store := &fakeJobStore{}
	starter := &fakeJobStarter{job: &jobregistry.JobRecord{
		JobID:        "job-1",
		Type:         jobregistry.JobTypeIndexBuild,
		State:        jobregistry.JobStateRunning,
		ManifestPath: manifestPath,
		CreatedAt:    time.Now().UTC(),
	}}
	h := newJobsHandlerForTest(store, starter, &fakeJobStopper{})

	reqBody := fmt.Sprintf(`{"type":"index.build","manifest_path":%q,"name":"nightly","since":"auto"}`, manifestPath)
	rec := serveJobsRequest(h, http.MethodPost, "/api/v1/jobs", reqBody)

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, manifestPath, starter.manifestPath)
	require.Equal(t, "nightly", starter.name)
	require.Equal(t, "auto", starter.opts.Since)
	require.NotNil(t, starter.opts.Invocation)
	require.Equal(t, "auto", starter.opts.Invocation.Since)
	require.Equal(t, "nightly", starter.opts.Invocation.Name)
	require.Equal(t, jobregistry.JobTypeIndexBuild, starter.opts.JobType)

	var body jobEnvelope
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "job-1", body.Job.JobID)
}

func TestJobsHandlerSubmitRejectsInvalidPayloads(t *testing.T) {
	manifestPath := writeJobAPITestManifest(t)
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown type", body: fmt.Sprintf(`{"type":"transfer","manifest_path":%q}`, manifestPath)},
		{name: "missing manifest", body: `{"type":"index.build"}`},
		{name: "remote manifest uri", body: `{"type":"index.build","manifest_uri":"s3://bucket/job.yaml"}`},
		{name: "relative manifest path", body: `{"type":"index.build","manifest_path":"job.yaml"}`},
		{name: "metadata rejected", body: fmt.Sprintf(`{"type":"index.build","manifest_path":%q,"metadata":{"label":"value"}}`, manifestPath)},
		{name: "signed material in name", body: fmt.Sprintf(`{"type":"index.build","manifest_path":%q,"name":"https://host/key?X-Amz-Signature=sentinel"}`, manifestPath)},
		{name: "invalid since", body: fmt.Sprintf(`{"type":"index.build","manifest_path":%q,"since":"https://host/key?X-Amz-Signature=sentinel"}`, manifestPath)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newJobsHandlerForTest(&fakeJobStore{}, &fakeJobStarter{}, &fakeJobStopper{})
			rec := serveJobsRequest(h, http.MethodPost, "/api/v1/jobs", tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			var body apperrors.HTTPErrorResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
			require.NotEmpty(t, body.Error.Code)
		})
	}
}

func TestJobsHandlerListFiltersAndLimits(t *testing.T) {
	h := newJobsHandlerForTest(&fakeJobStore{jobs: []jobregistry.JobRecord{
		{JobID: "job-1", Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateRunning, CreatedAt: time.Now().UTC()},
		{JobID: "job-2", Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateSuccess, CreatedAt: time.Now().UTC()},
		{JobID: "job-3", Type: jobregistry.JobTypeIndexBuild, State: jobregistry.JobStateRunning, CreatedAt: time.Now().UTC()},
		{JobID: "job-4", Type: "other", State: jobregistry.JobStateRunning, CreatedAt: time.Now().UTC()},
	}}, &fakeJobStarter{}, &fakeJobStopper{})

	rec := serveJobsRequest(h, http.MethodGet, "/api/v1/jobs?status=running&type=index.build&limit=1", "")

	require.Equal(t, http.StatusOK, rec.Code)
	var body jobListEnvelope
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, 1, len(body.Jobs))
	require.Equal(t, 2, body.Total)
	require.True(t, body.HasMore)
	require.Equal(t, "job-1", body.Jobs[0].JobID)
}

func TestJobsHandlerStatus(t *testing.T) {
	h := newJobsHandlerForTest(&fakeJobStore{records: map[string]*jobregistry.JobRecord{
		"job-1": {JobID: "job-1", State: jobregistry.JobStateRunning, CreatedAt: time.Now().UTC()},
	}}, &fakeJobStarter{}, &fakeJobStopper{})

	rec := serveJobsRequest(h, http.MethodGet, "/api/v1/jobs/job-1", "")

	require.Equal(t, http.StatusOK, rec.Code)
	var body jobEnvelope
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "job-1", body.Job.JobID)
	require.Equal(t, jobregistry.JobTypeIndexBuild, body.Job.Type)
}

func TestJobsHandlerStatusMissingReturnsNotFound(t *testing.T) {
	h := newJobsHandlerForTest(&fakeJobStore{}, &fakeJobStarter{}, &fakeJobStopper{})

	rec := serveJobsRequest(h, http.MethodGet, "/api/v1/jobs/missing", "")

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestJobsHandlerCancel(t *testing.T) {
	stopper := &fakeJobStopper{result: &jobregistry.StopResult{
		JobID:  "job-1",
		Signal: "term",
		State:  string(jobregistry.JobStateStopped),
	}}
	h := newJobsHandlerForTest(&fakeJobStore{}, &fakeJobStarter{}, stopper)

	rec := serveJobsRequest(h, http.MethodDelete, "/api/v1/jobs/job-1", "")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "job-1", stopper.jobID)
	var body cancelJobEnvelope
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "job-1", body.JobID)
	require.Equal(t, "term", body.Signal)
	require.Equal(t, string(jobregistry.JobStateStopped), body.State)
}

func TestJobsHandlerCancelMissingReturnsNotFound(t *testing.T) {
	h := newJobsHandlerForTest(&fakeJobStore{}, &fakeJobStarter{}, &fakeJobStopper{err: os.ErrNotExist})

	rec := serveJobsRequest(h, http.MethodDelete, "/api/v1/jobs/missing", "")

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestJobsHandlerCancelNonRunningReturnsConflict(t *testing.T) {
	h := newJobsHandlerForTest(&fakeJobStore{}, &fakeJobStarter{}, &fakeJobStopper{err: jobregistry.ErrJobNotRunning})

	rec := serveJobsRequest(h, http.MethodDelete, "/api/v1/jobs/job-1", "")

	require.Equal(t, http.StatusConflict, rec.Code)
}

func serveJobsRequest(h *JobsHandler, method, target, body string) *httptest.ResponseRecorder {
	r := chi.NewRouter()
	r.Post("/api/v1/jobs", h.Submit)
	r.Get("/api/v1/jobs", h.List)
	r.Get("/api/v1/jobs/{job_id}", h.Status)
	r.Delete("/api/v1/jobs/{job_id}", h.Cancel)

	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, target, reader)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func writeJobAPITestManifest(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(path, []byte("connection:\n  base_uri: s3://bucket/prefix/\n"), 0o600))
	return path
}

type fakeJobStore struct {
	records map[string]*jobregistry.JobRecord
	jobs    []jobregistry.JobRecord
	err     error
}

func (s *fakeJobStore) Get(jobID string) (*jobregistry.JobRecord, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.records == nil {
		return nil, os.ErrNotExist
	}
	rec, ok := s.records[jobID]
	if !ok {
		return nil, os.ErrNotExist
	}
	return rec, nil
}

func (s *fakeJobStore) List() ([]jobregistry.JobRecord, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.jobs, nil
}

type fakeJobStarter struct {
	job          *jobregistry.JobRecord
	err          error
	manifestPath string
	name         string
	opts         jobregistry.BackgroundOptions
}

func (s *fakeJobStarter) StartIndexBuildBackground(manifestPath string, name string, opts jobregistry.BackgroundOptions) (*jobregistry.JobRecord, error) {
	s.manifestPath = manifestPath
	s.name = name
	s.opts = opts
	if s.err != nil {
		return nil, s.err
	}
	return s.job, nil
}

type fakeJobStopper struct {
	result *jobregistry.StopResult
	err    error
	jobID  string
	opts   jobregistry.StopOptions
}

func (s *fakeJobStopper) Stop(jobID string, opts jobregistry.StopOptions) (*jobregistry.StopResult, error) {
	s.jobID = jobID
	s.opts = opts
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}
