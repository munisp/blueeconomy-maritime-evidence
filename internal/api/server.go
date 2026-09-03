// Package api is the /v1/evidence REST boundary: idempotent package creation
// with a presigned upload descriptor, package reads with a presigned download
// descriptor, append-only validation decisions with enforced state
// transitions, and capped listing. Every read enforces the clearance floor
// (caller clearance >= package classification). Roles: evidence-reader,
// evidence-writer, evidence-validator, evidence-admin.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/auth"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/evidence"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/objstore"
)

// Store is the persistence boundary the API requires.
type Store interface {
	Create(ctx context.Context, request evidence.CreateRequest) (evidence.Package, bool, error)
	Get(ctx context.Context, packageID string) (evidence.Package, error)
	RecordValidation(ctx context.Context, packageID string, request evidence.ValidationRequest) error
	List(ctx context.Context, limit, offset int) ([]evidence.Package, error)
}

// Server wires the REST handlers.
type Server struct {
	Store      Store
	Objects    objstore.Store
	Bucket     string
	ListLimits ListLimits
}

// ListLimits caps listing pagination.
type ListLimits struct {
	Default int
	Max     int
}

// NewServer validates the wiring fail-closed.
func NewServer(store Store, objects objstore.Store, bucket string, limits ListLimits) (*Server, error) {
	if store == nil {
		return nil, errors.New("api store is required")
	}
	if objects == nil {
		return nil, errors.New("api object store is required")
	}
	if strings.TrimSpace(bucket) == "" {
		return nil, errors.New("api evidence bucket is required")
	}
	if limits.Default < 1 || limits.Max < limits.Default {
		return nil, errors.New("api list limits are invalid")
	}
	return &Server{Store: store, Objects: objects, Bucket: bucket, ListLimits: limits}, nil
}

// Handler builds the authenticated route tree.
func (server *Server) Handler(authenticator auth.Authenticator) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/evidence/packages",
		auth.RequireRoles(http.HandlerFunc(server.createPackage), "evidence-writer", "evidence-admin"))
	mux.Handle("GET /v1/evidence/packages",
		auth.RequireRoles(http.HandlerFunc(server.listPackages), "evidence-reader", "evidence-validator", "evidence-admin"))
	mux.Handle("GET /v1/evidence/packages/{id}",
		auth.RequireRoles(http.HandlerFunc(server.getPackage), "evidence-reader", "evidence-validator", "evidence-admin"))
	mux.Handle("POST /v1/evidence/packages/{id}/validations",
		auth.RequireRoles(http.HandlerFunc(server.recordValidation), "evidence-validator", "evidence-admin"))
	mux.Handle("POST /v1/evidence/packages/{id}/upload-confirmation",
		auth.RequireRoles(http.HandlerFunc(server.confirmUpload), "evidence-writer", "evidence-admin"))
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	})
	return auth.Middleware(authenticator, mux)
}

// createBody is the caller-provided creation metadata. content_location is
// service-derived (the approved bucket); callers never supply it.
type createBody struct {
	IdempotencyKey    string `json:"idempotency_key"`
	ExternalReference string `json:"external_reference"`
	EvidenceType      string `json:"evidence_type"`
	ContentSHA256     string `json:"content_sha256"`
	ReceivedAt        string `json:"received_at"`
	Classification    string `json:"classification"`
	CorrelationID     string `json:"correlation_id"`
}

// POST /v1/evidence/packages — idempotent create; returns the retained
// package plus a presigned upload descriptor for the raw evidence bytes.
func (server *Server) createPackage(writer http.ResponseWriter, request *http.Request) {
	if _, ok := principalOrFail(writer, request); !ok {
		return
	}
	var body createBody
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	receivedAt, err := parseTime(body.ReceivedAt)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "received_at must be RFC 3339")
		return
	}
	create := evidence.CreateRequest{
		IdempotencyKey:    body.IdempotencyKey,
		ExternalReference: body.ExternalReference,
		EvidenceType:      body.EvidenceType,
		ContentSHA256:     body.ContentSHA256,
		ContentLocation:   server.objectLocation(body.IdempotencyKey),
		ReceivedAt:        receivedAt,
		Classification:    body.Classification,
		CorrelationID:     body.CorrelationID,
	}
	if err := create.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	record, created, err := server.Store.Create(request.Context(), create)
	if errors.Is(err, evidence.ErrIdempotencyConflict) {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence package creation failed")
		return
	}
	key, err := server.objectKey(record.ContentLocation)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence object location is invalid")
		return
	}
	upload, err := server.Objects.PresignedUpload(request.Context(), key, record.ContentSHA256)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "evidence upload authorization is unavailable")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, map[string]any{"package": record, "upload": upload})
}

// GET /v1/evidence/packages/{id} — returns the package and, once the record
// exists and the caller's clearance covers its classification, a presigned
// download descriptor.
func (server *Server) getPackage(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	record, ok := server.loadPackage(writer, request, principal)
	if !ok {
		return
	}
	key, err := server.objectKey(record.ContentLocation)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence object location is invalid")
		return
	}
	download, err := server.Objects.PresignedDownload(request.Context(), key)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "evidence download authorization is unavailable")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"package": record, "download": download})
}

// POST /v1/evidence/packages/{id}/validations — appends a terminal
// validation decision; illegal transitions are 409.
func (server *Server) recordValidation(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	if _, ok := server.loadPackage(writer, request, principal); !ok {
		return
	}
	var body struct {
		ValidationStatus string `json:"validation_status"`
		ReasonCode       string `json:"reason_code"`
		OccurredAt       string `json:"occurred_at"`
		CorrelationID    string `json:"correlation_id"`
	}
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	occurredAt, err := parseTime(body.OccurredAt)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "occurred_at must be RFC 3339")
		return
	}
	validation := evidence.ValidationRequest{
		ValidationStatus:      body.ValidationStatus,
		ReasonCode:            body.ReasonCode,
		ActorSubjectReference: principal.Subject,
		OccurredAt:            occurredAt,
		CorrelationID:         body.CorrelationID,
	}
	if err := validation.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	packageID := request.PathValue("id")
	err = server.Store.RecordValidation(request.Context(), packageID, validation)
	switch {
	case errors.Is(err, evidence.ErrNotFound):
		writeError(writer, http.StatusNotFound, "evidence package not found")
	case errors.Is(err, evidence.ErrTerminalValidation):
		writeError(writer, http.StatusConflict, err.Error())
	case err != nil:
		writeError(writer, http.StatusInternalServerError, "evidence validation failed")
	default:
		record, err := server.Store.Get(request.Context(), packageID)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "evidence package reload failed")
			return
		}
		writeJSON(writer, http.StatusCreated, map[string]any{
			"package":           record,
			"validation_status": validation.ValidationStatus,
		})
	}
}

// POST /v1/evidence/packages/{id}/upload-confirmation — the digest
// verification hook: the object store HEADs the retained object and confirms
// it matches the recorded SHA-256 before the upload is acknowledged.
func (server *Server) confirmUpload(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	record, ok := server.loadPackage(writer, request, principal)
	if !ok {
		return
	}
	key, err := server.objectKey(record.ContentLocation)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence object location is invalid")
		return
	}
	if err := server.Objects.VerifyDigest(request.Context(), key, record.ContentSHA256); err != nil {
		writeError(writer, http.StatusConflict, "evidence object digest is not yet verified: "+err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"evidence_package_id": record.EvidencePackageID,
		"content_sha256":      record.ContentSHA256,
		"digest_verified":     true,
	})
}

// GET /v1/evidence/packages?limit=&offset= — listing with pagination caps.
// Rows above the caller's clearance floor are omitted.
func (server *Server) listPackages(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	limit := server.parseLimit(request)
	offset := parseOffset(request)
	records, err := server.Store.List(request.Context(), limit, offset)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence package listing failed")
		return
	}
	visible := make([]evidence.Package, 0, len(records))
	for _, record := range records {
		if auth.ClearanceCovers(principal.Clearance, record.Classification) {
			visible = append(visible, record)
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"packages": visible,
		"limit":    limit,
		"offset":   offset,
	})
}

// loadPackage loads the package and enforces the clearance floor.
func (server *Server) loadPackage(writer http.ResponseWriter, request *http.Request, principal auth.Principal) (evidence.Package, bool) {
	packageID := request.PathValue("id")
	record, err := server.Store.Get(request.Context(), packageID)
	if errors.Is(err, evidence.ErrNotFound) {
		writeError(writer, http.StatusNotFound, "evidence package not found")
		return evidence.Package{}, false
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence package query failed")
		return evidence.Package{}, false
	}
	if !auth.ClearanceCovers(principal.Clearance, record.Classification) {
		writeError(writer, http.StatusForbidden, "clearance does not cover the package classification")
		return evidence.Package{}, false
	}
	return record, true
}

// objectLocation derives the credential-free location of the raw object for
// an idempotency-scoped evidence upload.
func (server *Server) objectLocation(idempotencyKey string) string {
	return "s3://" + server.Bucket + "/evidence/" + idempotencyKey
}

func (server *Server) objectKey(location string) (string, error) {
	prefix := "s3://" + server.Bucket + "/"
	if !strings.HasPrefix(location, prefix) {
		return "", errors.New("content location is not in the approved evidence bucket")
	}
	return strings.TrimPrefix(location, prefix), nil
}

func (server *Server) parseLimit(request *http.Request) int {
	raw := strings.TrimSpace(request.URL.Query().Get("limit"))
	if raw == "" {
		return server.ListLimits.Default
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return server.ListLimits.Default
	}
	if value > server.ListLimits.Max {
		return server.ListLimits.Max
	}
	return value
}

func parseOffset(request *http.Request) int {
	raw := strings.TrimSpace(request.URL.Query().Get("offset"))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func principalOrFail(writer http.ResponseWriter, request *http.Request) (auth.Principal, bool) {
	principal, ok := auth.PrincipalFrom(request.Context())
	if !ok {
		writeError(writer, http.StatusForbidden, "principal unavailable")
		return auth.Principal{}, false
	}
	return principal, true
}

func decodeJSON(request *http.Request, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("request body is not valid JSON")
	}
	return nil
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, errors.New("invalid timestamp")
	}
	return parsed, nil
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": message})
}
