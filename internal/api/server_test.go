package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/auth"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/evidence"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/objstore"
)

type fakeStore struct {
	packages      map[string]evidence.Package
	byIdempotency map[string]evidence.Package
	validations   []evidence.ValidationRequest
	errOnTerminal error
}

func newFakeStore() *fakeStore {
	return &fakeStore{packages: map[string]evidence.Package{}, byIdempotency: map[string]evidence.Package{}}
}

func (s *fakeStore) Create(_ context.Context, request evidence.CreateRequest) (evidence.Package, bool, error) {
	key := request.TenantID + "|" + request.IdempotencyKey
	if existing, found := s.byIdempotency[key]; found {
		if existing.ExternalReference != request.ExternalReference || existing.ContentSHA256 != request.ContentSHA256 {
			return evidence.Package{}, false, evidence.ErrIdempotencyConflict
		}
		return existing, false, nil
	}
	record := evidence.Package{
		EvidencePackageID: request.IdempotencyKey,
		IdempotencyKey:    request.IdempotencyKey,
		TenantID:          request.TenantID,
		ExternalReference: request.ExternalReference,
		EvidenceType:      request.EvidenceType,
		ContentSHA256:     request.ContentSHA256,
		ContentLocation:   request.ContentLocation,
		ReceivedAt:        request.ReceivedAt.UTC(),
		Classification:    request.Classification,
		CorrelationID:     request.CorrelationID,
		CreatedAt:         time.Now().UTC(),
		ValidationStatus:  evidence.StatusReceived,
	}
	s.packages[record.EvidencePackageID] = record
	s.byIdempotency[record.TenantID+"|"+record.IdempotencyKey] = record
	return record, true, nil
}

func (s *fakeStore) Get(_ context.Context, packageID string) (evidence.Package, error) {
	record, found := s.packages[packageID]
	if !found {
		return evidence.Package{}, evidence.ErrNotFound
	}
	return record, nil
}

func (s *fakeStore) RecordValidation(_ context.Context, packageID string, request evidence.ValidationRequest) error {
	if _, found := s.packages[packageID]; !found {
		return evidence.ErrNotFound
	}
	if len(s.validations) > 0 {
		if s.errOnTerminal != nil {
			return s.errOnTerminal
		}
		return evidence.ErrTerminalValidation
	}
	s.validations = append(s.validations, request)
	return nil
}

func (s *fakeStore) List(_ context.Context, tenantID string, limit, offset int) ([]evidence.Package, error) {
	if tenantID == "" {
		return nil, errors.New("tenant scope is required")
	}
	out := make([]evidence.Package, 0, len(s.packages))
	for _, record := range s.packages {
		if record.TenantID == tenantID {
			out = append(out, record)
		}
	}
	if offset >= len(out) {
		return []evidence.Package{}, nil
	}
	out = out[offset:]
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type fakeObjects struct {
	verifyErr error
}

func (fakeObjects) PresignedUpload(_ context.Context, key, _ string) (objstore.PresignedUpload, error) {
	return objstore.PresignedUpload{URL: "https://objects.example.invalid/upload/" + key, Method: "PUT", Headers: map[string]string{"x-checksum-sha256": "required"}, ExpiresAt: time.Now().Add(time.Hour).UTC()}, nil
}

func (fakeObjects) PresignedDownload(_ context.Context, key string) (objstore.PresignedDownload, error) {
	return objstore.PresignedDownload{URL: "https://objects.example.invalid/download/" + key, ExpiresAt: time.Now().Add(time.Hour).UTC()}, nil
}

func (objects fakeObjects) VerifyDigest(_ context.Context, _, _ string) error { return objects.verifyErr }

type staticAuthenticator struct {
	principal auth.Principal
	err       error
}

func (authenticator staticAuthenticator) Authenticate(_ context.Context, _ string) (auth.Principal, error) {
	return authenticator.principal, authenticator.err
}

const createBodyJSON = `{
  "idempotency_key":"11111111-1111-4111-8111-111111111111",
  "external_reference":"approved-reference-1",
  "evidence_type":"position.report",
  "content_sha256":"277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
  "received_at":"2026-08-12T12:00:00Z",
  "classification":"internal",
  "correlation_id":"22222222-2222-4222-8222-222222222222"
}`

func testServer(t *testing.T, store *fakeStore, objects fakeObjects) (*Server, http.Handler) {
	t.Helper()
	server, err := NewServer(store, objects, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	principal := auth.Principal{
		Subject:   "service:test",
		Roles:     map[string]struct{}{"evidence-reader": {}, "evidence-writer": {}, "evidence-validator": {}},
		Clearance: "highly_restricted",
		TenantID:  "tenant-test",
	}
	return server, server.Handler(staticAuthenticator{principal: principal})
}

func TestCreatePackageIdempotentReplay(t *testing.T) {
	store := newFakeStore()
	_, handler := testServer(t, store, fakeObjects{})
	for index, want := range []int{http.StatusCreated, http.StatusOK} {
		request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("request %d: expected %d, got %d (%s)", index, want, response.Code, response.Body.String())
		}
		var body struct {
			Package evidence.Package        `json:"package"`
			Upload  objstore.PresignedUpload `json:"upload"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode create response: %v", err)
		}
		if body.Upload.URL == "" || !strings.Contains(body.Upload.URL, body.Package.EvidencePackageID) {
			t.Fatalf("request %d: upload descriptor does not address the retained object", index)
		}
	}
}

func TestCreatePackageRejectsCallerSuppliedLocation(t *testing.T) {
	store := newFakeStore()
	_, handler := testServer(t, store, fakeObjects{})
	body := strings.Replace(createBodyJSON, `"correlation_id":"22222222-2222-4222-8222-222222222222"`,
		`"correlation_id":"22222222-2222-4222-8222-222222222222","content_location":"https://access:secret@evil.example/object"`, 1)
	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for caller-supplied content_location, got %d", response.Code)
	}
}

func TestGetPackageClearanceFloor(t *testing.T) {
	store := newFakeStore()
	server, err := NewServer(store, fakeObjects{}, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	if _, _, err := server.Store.Create(context.Background(), evidence.CreateRequest{
		IdempotencyKey:    "11111111-1111-4111-8111-111111111111",
		TenantID:          "tenant-test",
		ExternalReference: "ref",
		EvidenceType:      "test",
		ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
		ContentLocation:   "s3://evidence-bucket/evidence/11111111-1111-4111-8111-111111111111",
		ReceivedAt:        time.Now().UTC(),
		Classification:    "restricted",
		CorrelationID:     "22222222-2222-4222-8222-222222222222",
	}); err != nil {
		t.Fatalf("seed package: %v", err)
	}
	lowClearance := auth.Principal{Subject: "reader", Roles: map[string]struct{}{"evidence-reader": {}}, Clearance: "internal", TenantID: "tenant-test"}
	handler := server.Handler(staticAuthenticator{principal: lowClearance})
	request := httptest.NewRequest(http.MethodGet, "/v1/evidence/packages/11111111-1111-4111-8111-111111111111", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected 403 below the clearance floor, got %d", response.Code)
	}
	highClearance := auth.Principal{Subject: "reader", Roles: map[string]struct{}{"evidence-reader": {}}, Clearance: "restricted", TenantID: "tenant-test"}
	handler = server.Handler(staticAuthenticator{principal: highClearance})
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/evidence/packages/11111111-1111-4111-8111-111111111111", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200 at the clearance floor, got %d", response.Code)
	}
}

func TestValidationTransitionsAreTerminal(t *testing.T) {
	store := newFakeStore()
	_, handler := testServer(t, store, fakeObjects{})
	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("seed: expected 201, got %d", response.Code)
	}
	validationBody := `{"validation_status":"validated","reason_code":"integrity_confirmed","occurred_at":"2026-08-12T13:00:00Z","correlation_id":"33333333-3333-4333-8333-333333333333"}`
	request = httptest.NewRequest(http.MethodPost, "/v1/evidence/packages/11111111-1111-4111-8111-111111111111/validations", strings.NewReader(validationBody))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("first validation: expected 201, got %d (%s)", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/evidence/packages/11111111-1111-4111-8111-111111111111/validations", strings.NewReader(validationBody))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("second terminal validation: expected 409, got %d", response.Code)
	}
}

func TestUploadConfirmationVerifiesDigest(t *testing.T) {
	store := newFakeStore()
	_, handler := testServer(t, store, fakeObjects{verifyErr: errors.New("digest mismatch")})
	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("seed: expected 201, got %d", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/evidence/packages/11111111-1111-4111-8111-111111111111/upload-confirmation", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("expected 409 when the digest is unverified, got %d", response.Code)
	}
}

func TestListPaginationCapsAndClearanceFilter(t *testing.T) {
	store := newFakeStore()
	server, err := NewServer(store, fakeObjects{}, "evidence-bucket", ListLimits{Default: 50, Max: 2})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	for _, classification := range []string{"public", "restricted", "highly_restricted"} {
		if _, _, err := store.Create(context.Background(), evidence.CreateRequest{
			IdempotencyKey:    "11111111-1111-4111-8111-11111111111" + classification[:1],
			TenantID:          "tenant-test",
			ExternalReference: "ref-" + classification,
			EvidenceType:      "test",
			ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
			ContentLocation:   "s3://evidence-bucket/evidence/" + classification,
			ReceivedAt:        time.Now().UTC(),
			Classification:    classification,
			CorrelationID:     "22222222-2222-4222-8222-222222222222",
		}); err != nil {
			t.Fatalf("seed %s package: %v", classification, err)
		}
	}
	principal := auth.Principal{Subject: "reader", Roles: map[string]struct{}{"evidence-reader": {}}, Clearance: "public", TenantID: "tenant-test"}
	handler := server.Handler(staticAuthenticator{principal: principal})
	request := httptest.NewRequest(http.MethodGet, "/v1/evidence/packages?limit=500", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var body struct {
		Packages []evidence.Package `json:"packages"`
		Limit    int                `json:"limit"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if body.Limit != 2 {
		t.Fatalf("limit was not capped at the configured maximum")
	}
	for _, record := range body.Packages {
		if record.Classification != "public" {
			t.Fatalf("list leaked a %s package below the clearance floor", record.Classification)
		}
	}
}

// TestCrossTenantPackageIsInvisible is the H2 regression: a caller from
// another tenant cannot read, validate, or confirm upload on a package it
// does not own; every route reports not-found so the UUID is no oracle.
func TestCrossTenantPackageIsInvisible(t *testing.T) {
	store := newFakeStore()
	server, err := NewServer(store, fakeObjects{}, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	if _, _, err := server.Store.Create(context.Background(), evidence.CreateRequest{
		IdempotencyKey:    "11111111-1111-4111-8111-111111111111",
		TenantID:          "tenant-a",
		ExternalReference: "ref",
		EvidenceType:      "test",
		ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
		ContentLocation:   "s3://evidence-bucket/evidence/11111111-1111-4111-8111-111111111111",
		ReceivedAt:        time.Now().UTC(),
		Classification:    "public",
		CorrelationID:     "22222222-2222-4222-8222-222222222222",
	}); err != nil {
		t.Fatalf("seed package: %v", err)
	}
	outsider := auth.Principal{
		Subject:   "reader-b",
		Roles:     map[string]struct{}{"evidence-reader": {}, "evidence-validator": {}, "evidence-writer": {}},
		Clearance: "highly_restricted",
		TenantID:  "tenant-b",
	}
	handler := server.Handler(staticAuthenticator{principal: outsider})
	packageID := "11111111-1111-4111-8111-111111111111"
	for _, target := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/evidence/packages/" + packageID, ""},
		{http.MethodPost, "/v1/evidence/packages/" + packageID + "/validations",
			`{"validation_status":"validated","reason_code":"integrity_confirmed","occurred_at":"2026-08-12T13:00:00Z","correlation_id":"33333333-3333-4333-8333-333333333333"}`},
		{http.MethodPost, "/v1/evidence/packages/" + packageID + "/upload-confirmation", ""},
	} {
		request := httptest.NewRequest(target.method, target.path, strings.NewReader(target.body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s %s: expected 404 for cross-tenant access, got %d", target.method, target.path, response.Code)
		}
	}
	// The outsider's listing is scoped to its own (empty) tenant.
	request := httptest.NewRequest(http.MethodGet, "/v1/evidence/packages", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var body map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if packages := body["packages"].([]any); len(packages) != 0 {
		t.Fatalf("cross-tenant list leaked %d packages", len(packages))
	}
	// The owner tenant still sees the package.
	owner := auth.Principal{
		Subject:   "reader-a",
		Roles:     map[string]struct{}{"evidence-reader": {}},
		Clearance: "highly_restricted",
		TenantID:  "tenant-a",
	}
	ownerHandler := server.Handler(staticAuthenticator{principal: owner})
	request = httptest.NewRequest(http.MethodGet, "/v1/evidence/packages/"+packageID, nil)
	response = httptest.NewRecorder()
	ownerHandler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("owner tenant read: expected 200, got %d", response.Code)
	}
}

// TestCreateRequiresTenantBinding fails closed when the authenticated
// principal carries no tenant claim.
func TestCreateRequiresTenantBinding(t *testing.T) {
	server, err := NewServer(newFakeStore(), fakeObjects{}, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	principal := auth.Principal{
		Subject:   "writer",
		Roles:     map[string]struct{}{"evidence-writer": {}},
		Clearance: "highly_restricted",
	}
	handler := server.Handler(staticAuthenticator{principal: principal})
	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("tenantless principal: expected 403, got %d", response.Code)
	}
}

// TestCreateStampsPrincipalTenant proves the stored record carries the
// caller's tenant, not any caller-supplied value.
func TestCreateStampsPrincipalTenant(t *testing.T) {
	store := newFakeStore()
	_, handler := testServer(t, store, fakeObjects{})
	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d", response.Code)
	}
	record := store.packages["11111111-1111-4111-8111-111111111111"]
	if record.TenantID != "tenant-test" {
		t.Fatalf("package tenant = %q, want tenant-test", record.TenantID)
	}
}
