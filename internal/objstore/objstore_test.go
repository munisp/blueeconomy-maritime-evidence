package objstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 is an in-process S3 test double: it honors the signed
// x-amz-checksum-sha256 upload contract (rejecting mismatched payloads like
// real S3) and answers checksum-mode HEAD requests. The production path
// remains fail-closed; this double only exercises the client logic.
type fakeS3 struct {
	mu        sync.Mutex
	checksums map[string]string // key -> base64 sha256
	bodies    map[string][]byte
}

func newFakeS3(t *testing.T) (*fakeS3, *httptest.Server) {
	t.Helper()
	fake := &fakeS3{checksums: map[string]string{}, bodies: map[string][]byte{}}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		key := strings.TrimPrefix(request.URL.Path, "/bucket/")
		fake.mu.Lock()
		defer fake.mu.Unlock()
		switch request.Method {
		case http.MethodPut:
			body, err := io.ReadAll(request.Body)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			checksum := base64.StdEncoding.EncodeToString(func() []byte { sum := sha256.Sum256(body); return sum[:] }())
			if expected := request.Header.Get("X-Amz-Checksum-Sha256"); expected != checksum {
				writer.WriteHeader(http.StatusBadRequest)
				_, _ = writer.Write([]byte(`<Error><Code>BadDigest</Code></Error>`))
				return
			}
			fake.checksums[key] = checksum
			fake.bodies[key] = body
			writer.Header().Set("ETag", `"test"`)
			writer.WriteHeader(http.StatusOK)
		case http.MethodHead:
			checksum, found := fake.checksums[key]
			if !found {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.Header().Set("X-Amz-Checksum-Sha256", checksum)
			writer.WriteHeader(http.StatusOK)
		case http.MethodGet:
			body, found := fake.bodies[key]
			if !found {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = writer.Write(body)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	return fake, server
}

func testConfig(endpoint string) Config {
	return Config{
		Endpoint:   endpoint,
		Region:     "us-east-1",
		Bucket:     "bucket",
		AccessKey:  "test-access",
		SecretKey:  "test-secret",
		PathStyle:  true,
		PresignTTL: 15 * time.Minute,
	}
}

const testDigest = "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9"

func TestConfigValidateFailsClosed(t *testing.T) {
	config := testConfig("")
	if err := config.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Region = "" },
		func(c *Config) { c.Bucket = "" },
		func(c *Config) { c.AccessKey = "" },
		func(c *Config) { c.SecretKey = "" },
		func(c *Config) { c.PresignTTL = 0 },
		func(c *Config) { c.PresignTTL = 2 * time.Hour },
		func(c *Config) { c.Endpoint = "https://user:secret@objects.example" },
		func(c *Config) { c.Endpoint = "not-a-url" },
	} {
		mutated := testConfig("")
		mutate(&mutated)
		if err := mutated.Validate(); err == nil {
			t.Fatalf("incomplete config must fail closed: %+v", mutated)
		}
	}
}

func TestPresignedUploadAndDigestVerification(t *testing.T) {
	_, server := newFakeS3(t)
	client, err := NewClient(testConfig(server.URL))
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	ctx := context.Background()

	// The payload bytes matching the recorded digest.
	payload := []byte("approved non-production evidence bytes")
	sum := sha256.Sum256(payload)
	digest := hexEncode(sum[:])

	upload, err := client.PresignedUpload(ctx, "evidence/pkg-1", digest)
	if err != nil {
		t.Fatalf("presign upload: %v", err)
	}
	if upload.Method != "PUT" || upload.URL == "" || upload.Headers["x-amz-checksum-sha256"] == "" {
		t.Fatalf("upload descriptor is incomplete: %+v", upload)
	}

	request, err := http.NewRequest(http.MethodPut, upload.URL, strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("build upload request: %v", err)
	}
	for key, value := range upload.Headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload to fake s3: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("fake s3 rejected a digest-matching upload: %d", response.StatusCode)
	}

	if err := client.VerifyDigest(ctx, "evidence/pkg-1", digest); err != nil {
		t.Fatalf("digest verification hook failed: %v", err)
	}

	// Wrong digest must fail closed.
	if err := client.VerifyDigest(ctx, "evidence/pkg-1", testDigest); err == nil {
		t.Fatal("a digest mismatch must fail closed")
	}
	// Missing object must fail closed.
	if err := client.VerifyDigest(ctx, "evidence/absent", digest); err == nil {
		t.Fatal("a missing object must fail closed")
	}

	// Download descriptor round-trips the bytes.
	download, err := client.PresignedDownload(ctx, "evidence/pkg-1")
	if err != nil {
		t.Fatalf("presign download: %v", err)
	}
	getResponse, err := http.Get(download.URL)
	if err != nil {
		t.Fatalf("download from fake s3: %v", err)
	}
	body, _ := io.ReadAll(getResponse.Body)
	getResponse.Body.Close()
	if string(body) != string(payload) {
		t.Fatal("downloaded bytes do not match the uploaded evidence")
	}
}

func TestPresignedUploadServerRejectsMismatchedPayload(t *testing.T) {
	_, server := newFakeS3(t)
	client, err := NewClient(testConfig(server.URL))
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	upload, err := client.PresignedUpload(context.Background(), "evidence/pkg-2", testDigest)
	if err != nil {
		t.Fatalf("presign upload: %v", err)
	}
	request, _ := http.NewRequest(http.MethodPut, upload.URL, strings.NewReader("tampered bytes"))
	for key, value := range upload.Headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("upload to fake s3: %v", err)
	}
	response.Body.Close()
	if response.StatusCode == http.StatusOK {
		t.Fatal("the server-side checksum contract must reject a mismatched payload")
	}
}

func TestParseLocationFailsClosed(t *testing.T) {
	client, err := NewClient(testConfig(""))
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	key, err := client.ParseLocation("s3://bucket/evidence/pkg-1")
	if err != nil || key != "evidence/pkg-1" {
		t.Fatalf("parse approved location: %v %q", err, key)
	}
	for _, location := range []string{
		"s3://other-bucket/evidence/pkg-1",
		"https://bucket.example/evidence/pkg-1",
		"s3://bucket/",
		"s3://bucket/../escape",
	} {
		if _, err := client.ParseLocation(location); err == nil {
			t.Fatalf("foreign or malformed location must fail closed: %s", location)
		}
	}
}

func hexEncode(raw []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(raw)*2)
	for index, value := range raw {
		out[index*2] = digits[value>>4]
		out[index*2+1] = digits[value&0x0f]
	}
	return string(out)
}
