package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/obukhov/dbshuffle/internal/service"
)

type ShuffleHandler struct {
	svc *service.ShuffleService
}

func NewShuffleHandler(svc *service.ShuffleService) *ShuffleHandler {
	return &ShuffleHandler{svc: svc}
}

func (h *ShuffleHandler) Routes(r chi.Router) {
	r.Get("/status", h.status)
	r.Post("/assign", h.assign)
	r.Post("/reset", h.reset)
	r.Post("/extend", h.extend)
	r.Post("/clean", h.clean)
	r.Post("/refill", h.refill)
}

func (h *ShuffleHandler) status(w http.ResponseWriter, r *http.Request) {
	report, err := h.svc.Status(r.Context())
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, report)
}

func (h *ShuffleHandler) assign(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Template string `json:"template"`
		DBName   string `json:"db_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid body", http.StatusBadRequest)
		return
	}
	if body.Template == "" || body.DBName == "" {
		jsonError(w, "template and db_name are required", http.StatusBadRequest)
		return
	}

	rec, err := h.svc.Assign(r.Context(), body.Template, body.DBName)
	switch {
	case errors.Is(err, service.ErrUnknownTemplate):
		jsonError(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, service.ErrNoBuffer):
		jsonError(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, service.ErrAlreadyAssigned):
		jsonError(w, err.Error(), http.StatusConflict)
	case err != nil:
		jsonError(w, err.Error(), http.StatusInternalServerError)
	default:
		jsonOK(w, rec)
	}
}

func (h *ShuffleHandler) reset(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Template string `json:"template"`
		DBName   string `json:"db_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid body", http.StatusBadRequest)
		return
	}
	if body.Template == "" || body.DBName == "" {
		jsonError(w, "template and db_name are required", http.StatusBadRequest)
		return
	}

	rec, err := h.svc.Reset(r.Context(), body.Template, body.DBName)
	switch {
	case errors.Is(err, service.ErrUnknownTemplate):
		jsonError(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, service.ErrNoBuffer):
		jsonError(w, err.Error(), http.StatusServiceUnavailable)
	case err != nil:
		jsonError(w, err.Error(), http.StatusInternalServerError)
	default:
		jsonOK(w, rec)
	}
}

func (h *ShuffleHandler) extend(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Template string `json:"template"`
		DBName   string `json:"db_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid body", http.StatusBadRequest)
		return
	}
	if body.Template == "" || body.DBName == "" {
		jsonError(w, "template and db_name are required", http.StatusBadRequest)
		return
	}

	rec, err := h.svc.Extend(r.Context(), body.Template, body.DBName)
	switch {
	case errors.Is(err, service.ErrUnknownTemplate):
		jsonError(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, service.ErrNotFound):
		jsonError(w, err.Error(), http.StatusNotFound)
	case err != nil:
		jsonError(w, err.Error(), http.StatusInternalServerError)
	default:
		jsonOK(w, rec)
	}
}

func (h *ShuffleHandler) clean(w http.ResponseWriter, r *http.Request) {
	n, err := h.svc.Clean(r.Context())
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, map[string]int{"cleaned": n})
}

func (h *ShuffleHandler) refill(w http.ResponseWriter, r *http.Request) {
	n, err := h.svc.Refill(r.Context())
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, map[string]int{"created": n})
}

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
