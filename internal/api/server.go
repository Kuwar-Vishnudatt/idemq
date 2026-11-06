// Package api exposes the queue over HTTP. Every mutating endpoint maps
// directly onto one store operation and preserves its error semantics:
// ErrNoJob -> 204 (poll again later), ErrFenced -> 409 (your lease is stale,
// stop processing), ErrNotFound -> 404. A worker is just an HTTP client
// calling claim/heartbeat/ack/nack in a loop — see pkg/client for a
// reference implementation — so any process on any machine can be a worker;
// that's the "distributed" in this project: many independent worker
// processes safely sharing one durable, lease-governed broker.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/Kuwar-Vishnudatt/idemq/internal/store"
)

type Server struct {
	db  *store.DB
	mux *http.ServeMux
}

func New(db *store.DB) *Server {
	s := &Server{db: db, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("POST /v1/queues/{queue}/enqueue", s.handleEnqueue)
	s.mux.HandleFunc("POST /v1/queues/{queue}/claim", s.handleClaim)
	s.mux.HandleFunc("GET /v1/queues/{queue}/stats", s.handleStats)
	s.mux.HandleFunc("GET /v1/queues/{queue}/dlq", s.handleListDead)
	s.mux.HandleFunc("POST /v1/jobs/{id}/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("POST /v1/jobs/{id}/ack", s.handleAck)
	s.mux.HandleFunc("POST /v1/jobs/{id}/nack", s.handleNack)
	s.mux.HandleFunc("POST /v1/jobs/{id}/requeue", s.handleRequeue)
	s.mux.HandleFunc("GET /v1/jobs/{id}", s.handleGetJob)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

type enqueueRequest struct {
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	DelayMs        int64           `json:"delay_ms,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	Priority       int             `json:"priority,omitempty"`
}

type enqueueResponse struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id, dup, err := s.db.Enqueue(queue, req.Payload, store.EnqueueOpts{
		IdempotencyKey: req.IdempotencyKey,
		Delay:          time.Duration(req.DelayMs) * time.Millisecond,
		MaxAttempts:    req.MaxAttempts,
		Priority:       req.Priority,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, enqueueResponse{ID: id, Duplicate: dup})
}

type claimRequest struct {
	LeaseMs int64 `json:"lease_ms"`
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	var req claimRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.LeaseMs <= 0 {
		req.LeaseMs = 30_000
	}
	job, err := s.db.Claim(queue, time.Duration(req.LeaseMs)*time.Millisecond)
	if errors.Is(err, store.ErrNoJob) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, jobView(job))
}

type leaseRequest struct {
	Token    int64  `json:"token"`
	ExtendMs int64  `json:"extend_ms,omitempty"`
	Error    string `json:"error,omitempty"`
	DelayMs  int64  `json:"delay_ms,omitempty"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req leaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.ExtendMs <= 0 {
		req.ExtendMs = 30_000
	}
	err := s.db.Heartbeat(id, req.Token, time.Duration(req.ExtendMs)*time.Millisecond)
	respondLeaseResult(w, err)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req leaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	err := s.db.Ack(id, req.Token)
	respondLeaseResult(w, err)
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req leaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	err := s.db.Nack(id, req.Token, req.Error, time.Duration(req.DelayMs)*time.Millisecond)
	respondLeaseResult(w, err)
}

func respondLeaseResult(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, store.ErrFenced):
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (s *Server) handleRequeue(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.db.RequeueDead(id)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := s.db.Get(id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, jobView(job))
}

func (s *Server) handleListDead(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	dead, err := s.db.ListDead(queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	views := make([]jobResponse, len(dead))
	for i := range dead {
		views[i] = jobView(&dead[i])
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	stats, err := s.db.Stats(queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

type jobResponse struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Priority       int             `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LeaseToken     int64           `json:"lease_token"`
	LastError      string          `json:"last_error,omitempty"`
}

func jobView(j *store.Job) jobResponse {
	return jobResponse{
		ID: j.ID, Queue: j.Queue, IdempotencyKey: j.IdempotencyKey,
		Payload: json.RawMessage(j.Payload), Status: j.Status, Priority: j.Priority,
		Attempts: j.Attempts, MaxAttempts: j.MaxAttempts, LeaseToken: j.LeaseToken,
		LastError: j.LastError,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
