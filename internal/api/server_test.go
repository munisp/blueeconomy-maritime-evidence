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

type staticAuthenticator struct {
	principal auth.Principal
	err       error
}

func (a staticAuthenticator) Authenticate(*http.Request) (auth.Principal, error) {
	return a.principal, a.err
}

type fakeStore struct {
	packages        map[string]evidence.Package
	byIdempotency   map[string]evidence.Package
	validations     []evidence.ValidationRequest
	terminalReached map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		packages:        map[string]evidence.Package{},
		byIdempotency:   map[string]evidence.Package{},
		terminalReached: map[string]bool{},
	}
}

func (s *fakeStore) Create(_ context.Context, request evidence.CreateRequest) (evidence.Package, bool, error) {
	if existing, found := s.byIdempotency[request.IdempotencyKey]; found {
		if existing.ExternalReference != request.ExternalReference || existing.ContentSHA256 != request.ContentSHA256 {
			return evidence.Package{}, false, evidence.ErrIdempotencyConflict
		}
		return existing, false, nil
	}
	record := evidence.Package{
		EvidencePackageID: request.IdempotencyKey,
		IdempotencyKey:    request.IdempotencyKey,
		ExternalReference: request.ExternalReference,
		EvidenceType:      request.EvidenceType,
		ContentSHA256:     request.ContentSHA256,
		ContentLocation:   request.ContentLocation,
		ReceivedAt:        request.ReceivedAt,
		Classification:    request.Classification,
		CorrelationID:     request.CorrelationID,
		CreatedAt:         time.Now().UTC(),
		ValidationStatus:  evidence.StatusReceived,
	}
	s.packages[record.EvidencePackageID] = record
	s.byIdempotency[record.IdempotencyKey] = record
	return record, true, nil
}

func (s *fakeStore) Get(_ context.Context, packageID string) (evidence.Package, error) {
	if record, found := s.packages[packageID]; found {
		return record, nil
	}
	return evidence.Package{}, evidence.ErrNotFound
}

func (s *fakeStore) RecordValidation(_ context.Context, packageID string, request evidence.ValidationRequest) error {
	if _, found := s.packages[packageID]; !found {
		return evidence.ErrNotFound
	}
	if s.terminalReached[packageID] {
		return evidence.ErrTerminalValidation
	}
	s.terminalReached[packageID] = true
	s.validations = append(s.validations, request)
	return nil
}

func (s *fakeStore) List(_ context.Context, limit, offset int) ([]evidence.Package, error) {
	out := make([]evidence.Package, 0, len(s.packages))
	for _, record := range s.packages {
		out = append(out, record)
	}
	if offset >= len(out) {
		return nil, nil
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

func (f fakeObjects) PresignedUpload(_ context.Context, key, digest string) (objstore.UploadDescriptor, error) {
	if key == "" || digest == "" {
		return objstore.UploadDescriptor{}, errors.New("key and digest are required")
	}
	return objstore.UploadDescriptor{
		Method: "PUT", URL: "https://objects.example/signed-put",
		Headers: map[string]string{"x-amz-checksum-sha256": "checksum"}, ExpiresAt: time.Now().Add(time.Hour).UTC(),
	}, nil
}

func (f fakeObjects) PresignedDownload(_ context.Context, key string) (objstore.DownloadDescriptor, error) {
	return objstore.DownloadDescriptor{Method: "GET", URL: "https://objects.example/signed-get", ExpiresAt: time.Now().Add(time.Hour).UTC()}, nil
}

func (f fakeObjects) VerifyDigest(_ context.Context, key, digest string) error {
	return f.verifyErr
}

func testServer(t *testing.T, store *fakeStore, objects objstore.Store) (*Server, http.Handler) {
	t.Helper()
	server, err := NewServer(store, objects, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	principal := auth.Principal{
		Subject:   "service:test",
		Roles:     map[string]struct{}{"evidence-reader": {}, "evidence-writer": {}, "evidence-validator": {}},
		Clearance: "highly_restricted",
	}
	return server, server.Handler(staticAuthenticator{principal: principal})
}

const createBodyJSON = `{
	"idempotency_key": "11111111-1111-4111-8111-111111111111",
	"external_reference": "approved-test-reference",
	"evidence_type": "test.conformance",
	"content_sha256": "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
	"received_at": "2026-08-12T12:00:00Z",
	"classification": "internal",
	"correlation_id": "22222222-2222-4222-8222-222222222222"
}`

func TestCreatePackageIdempotentWithUploadDescriptor(t *testing.T) {
	_, handler := testServer(t, newFakeStore(), fakeObjects{})

	create := func() (int, map[string]any) {
		request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode response: %v (%s)", err, response.Body.String())
		}
		return response.Code, body
	}

	status, body := create()
	if status != http.StatusCreated {
		t.Fatalf("first create: expected 201, got %d (%v)", status, body)
	}
	upload, ok := body["upload"].(map[string]any)
	if !ok || upload["url"] == "" || upload["method"] != "PUT" {
		t.Fatalf("create must return an upload descriptor: %v", body)
	}
	record := body["package"].(map[string]any)
	if !strings.HasPrefix(record["content_location"].(string), "s3://evidence-bucket/evidence/") {
		t.Fatalf("content location must be the derived approved-bucket location: %v", record["content_location"])
	}

	status, body = create()
	if status != http.StatusOK {
		t.Fatalf("repeat create: expected idempotent 200, got %d (%v)", status, body)
	}
}

func TestCreatePackageRejectsInvalidBody(t *testing.T) {
	_, handler := testServer(t, newFakeStore(), fakeObjects{})
	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(`{"idempotency_key":"not-a-uuid"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid body: expected 400, got %d", response.Code)
	}
}

func TestValidationTransitions(t *testing.T) {
	store := newFakeStore()
	_, handler := testServer(t, store, fakeObjects{})

	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var created map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &created)
	packageID := created["package"].(map[string]any)["evidence_package_id"].(string)

	validation := `{"validation_status":"validated","reason_code":"integrity_confirmed","occurred_at":"2026-08-12T13:00:00Z","correlation_id":"33333333-3333-4333-8333-333333333333"}`
	call := func() int {
		request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages/"+packageID+"/validations", strings.NewReader(validation))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}
	if status := call(); status != http.StatusCreated {
		t.Fatalf("first validation: expected 201, got %d", status)
	}
	if status := call(); status != http.StatusConflict {
		t.Fatalf("second terminal validation: expected 409, got %d", status)
	}
	if store.validations[0].ActorSubjectReference != "service:test" {
		t.Fatal("actor subject must come from the authenticated principal")
	}

	// Unknown package → 404.
	request = httptest.NewRequest(http.MethodPost, "/v1/evidence/packages/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb/validations", strings.NewReader(validation))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown package: expected 404, got %d", response.Code)
	}
}

func TestGetPackageClearanceFloor(t *testing.T) {
	server, err := NewServer(newFakeStore(), fakeObjects{}, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	if _, _, err := server.Store.Create(context.Background(), evidence.CreateRequest{
		IdempotencyKey:    "11111111-1111-4111-8111-111111111111",
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

	lowClearance := auth.Principal{Subject: "reader", Roles: map[string]struct{}{"evidence-reader": {}}, Clearance: "internal"}
	handler := server.Handler(staticAuthenticator{principal: lowClearance})
	request := httptest.NewRequest(http.MethodGet, "/v1/evidence/packages/11111111-1111-4111-8111-111111111111", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("below-floor clearance: expected 403, got %d", response.Code)
	}

	highClearance := auth.Principal{Subject: "reader", Roles: map[string]struct{}{"evidence-reader": {}}, Clearance: "restricted"}
	handler = server.Handler(staticAuthenticator{principal: highClearance})
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("at-floor clearance: expected 200, got %d", response.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if download, ok := body["download"].(map[string]any); !ok || download["url"] == "" {
		t.Fatalf("read must return a download descriptor: %v", body)
	}
}

func TestListPaginationCapsAndClearanceFilter(t *testing.T) {
	store := newFakeStore()
	for _, classification := range []string{"public", "highly_restricted"} {
		if _, _, err := store.Create(context.Background(), evidence.CreateRequest{
			IdempotencyKey:    "11111111-1111-4111-8111-11111111111" + classification[:1],
			ExternalReference: "ref-" + classification,
			EvidenceType:      "test",
			ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
			ContentLocation:   "s3://evidence-bucket/evidence/" + classification,
			ReceivedAt:        time.Now().UTC(),
			Classification:    classification,
			CorrelationID:     "22222222-2222-4222-8222-222222222222",
		}); err != nil {
			t.Fatalf("seed package: %v", err)
		}
	}
	server, err := NewServer(store, fakeObjects{}, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	principal := auth.Principal{Subject: "reader", Roles: map[string]struct{}{"evidence-reader": {}}, Clearance: "public"}
	handler := server.Handler(staticAuthenticator{principal: principal})

	request := httptest.NewRequest(http.MethodGet, "/v1/evidence/packages?limit=99999", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var body map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if body["limit"].(float64) != 200 {
		t.Fatalf("limit must be capped at 200, got %v", body["limit"])
	}
	packages := body["packages"].([]any)
	if len(packages) != 1 {
		t.Fatalf("public clearance must only see the public package, got %d", len(packages))
	}
	if packages[0].(map[string]any)["classification"] != "public" {
		t.Fatal("clearance filter leaked an above-floor package")
	}
}

func TestUnauthenticatedFailsClosed(t *testing.T) {
	server, err := NewServer(newFakeStore(), fakeObjects{}, "evidence-bucket", ListLimits{Default: 50, Max: 200})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	handler := server.Handler(staticAuthenticator{err: errors.New("no bearer token")})
	request := httptest.NewRequest(http.MethodGet, "/v1/evidence/packages", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated request: expected 401, got %d", response.Code)
	}
}

func TestUploadConfirmationHook(t *testing.T) {
	store := newFakeStore()
	_, handler := testServer(t, store, fakeObjects{})
	request := httptest.NewRequest(http.MethodPost, "/v1/evidence/packages", strings.NewReader(createBodyJSON))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var created map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &created)
	packageID := created["package"].(map[string]any)["evidence_package_id"].(string)

	request = httptest.NewRequest(http.MethodPost, "/v1/evidence/packages/"+packageID+"/upload-confirmation", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("verified upload: expected 200, got %d", response.Code)
	}

	// A digest mismatch is a conflict, never a silent pass.
	_, conflictHandler := testServer(t, store, fakeObjects{verifyErr: errors.New("digest mismatch")})
	request = httptest.NewRequest(http.MethodPost, "/v1/evidence/packages/"+packageID+"/upload-confirmation", nil)
	response = httptest.NewRecorder()
	conflictHandler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("mismatched upload: expected 409, got %d", response.Code)
	}
}
