package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testTargetPrefix = "abfs://evidence@agencyevidence01.dfs.core.usgovcloudapi.net/legacy-s3"

func TestPlanLegacyS3RelocationMapsBucketAndKey(t *testing.T) {
	target, err := PlanLegacyS3Relocation("s3://agency-evidence/case/2026/object.bin", testTargetPrefix)
	if err != nil {
		t.Fatalf("plan relocation: %v", err)
	}
	expected := testTargetPrefix + "/agency-evidence/case/2026/object.bin"
	if target != expected {
		t.Fatalf("planned target %q, expected %q", target, expected)
	}
	// The planned target must satisfy the default location policy on its own.
	if err := validateContentLocation(target, LocationPolicy{}); err != nil {
		t.Fatalf("planned target violates the location policy: %v", err)
	}
}

func TestPlanLegacyS3RelocationPreservesDottedBucket(t *testing.T) {
	target, err := PlanLegacyS3Relocation("s3://agency.evidence.archive/object", testTargetPrefix)
	if err != nil {
		t.Fatalf("plan relocation: %v", err)
	}
	if !strings.Contains(target, "/agency.evidence.archive/object") {
		t.Fatalf("dotted bucket name lost in target %q", target)
	}
}

func TestPlanLegacyS3RelocationFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		source string
		prefix string
	}{
		{"non s3 source", "https://objects.example.gov/object", testTargetPrefix},
		{"s3 source without key", "s3://agency-evidence", testTargetPrefix},
		{"s3 source with root key only", "s3://agency-evidence/", testTargetPrefix},
		{"invalid bucket", "s3://Bad_Bucket/object", testTargetPrefix},
		{"s3 target prefix", "s3://agency-evidence/object", "s3://other-bucket/base"},
		{"commercial cloud abfs prefix", "s3://agency-evidence/object", "abfs://evidence@account.dfs.core.windows.net/base"},
		{"abfs prefix with credentials", "s3://agency-evidence/object", "abfs://evidence:secret@agencyevidence01.dfs.core.usgovcloudapi.net/base"},
		{"abfs prefix without base path", "s3://agency-evidence/object", "abfs://evidence@agencyevidence01.dfs.core.usgovcloudapi.net"},
		{"non abfs prefix", "s3://agency-evidence/object", "https://objects.example.gov/base"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if target, err := PlanLegacyS3Relocation(testCase.source, testCase.prefix); err == nil {
				t.Fatalf("relocation of %q onto %q unexpectedly planned %q", testCase.source, testCase.prefix, target)
			}
		})
	}
}

func TestVerifyStagedContentSHA256(t *testing.T) {
	directory := t.TempDir()
	content := []byte("approved legacy evidence object bytes")
	digest := hex.EncodeToString(sha256Sum(content))

	staged := filepath.Join(directory, "object")
	if err := os.WriteFile(staged, content, 0o600); err != nil {
		t.Fatalf("stage object: %v", err)
	}
	if err := VerifyStagedContentSHA256(staged, digest); err != nil {
		t.Fatalf("matching staged content must verify: %v", err)
	}

	// A digest mismatch must refuse verification: this is the guard that
	// blocks supersession when the copied bytes differ from the registered
	// evidence.
	corrupted := filepath.Join(directory, "corrupted")
	if err := os.WriteFile(corrupted, append(content, 0x00), 0o600); err != nil {
		t.Fatalf("stage corrupted object: %v", err)
	}
	if err := VerifyStagedContentSHA256(corrupted, digest); err == nil {
		t.Fatal("digest mismatch must refuse verification")
	}

	if err := VerifyStagedContentSHA256(staged, "NOT-A-DIGEST"); err == nil {
		t.Fatal("malformed registered digest must fail closed")
	}
	if err := VerifyStagedContentSHA256(filepath.Join(directory, "missing"), digest); err == nil {
		t.Fatal("missing staged object must fail closed")
	}
}

func TestCopyAndVerifyRejectsMismatchedPlan(t *testing.T) {
	copier := StagedCommandCopier{AWSCLI: "aws", AzCopy: "azcopy", WorkDir: t.TempDir()}
	legacy := Package{
		EvidencePackageID: "11111111-1111-4111-8111-111111111111",
		ContentLocation:   "s3://agency-evidence/object",
		ContentSHA256:     hex.EncodeToString(sha256Sum([]byte("bytes"))),
		ReceivedAt:        time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
	plan := LegacyS3Plan{
		LegacyPackageID: "22222222-2222-4222-8222-222222222222",
		TargetLocation:  testTargetPrefix + "/agency-evidence/object",
	}
	if err := copier.CopyAndVerify(t.Context(), legacy, plan); err == nil {
		t.Fatal("a plan for a different package must be refused before any copy")
	}
}

func TestCopyAndVerifyRejectsUnsafePackageID(t *testing.T) {
	workDir := t.TempDir()
	copier := StagedCommandCopier{AWSCLI: "aws", AzCopy: "azcopy", WorkDir: workDir}
	base := Package{
		ContentLocation: "s3://agency-evidence/object",
		ContentSHA256:   hex.EncodeToString(sha256Sum([]byte("bytes"))),
		ReceivedAt:      time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
	// (the empty id is already refused by the plan-match guard above)
	for _, id := range []string{"../etc", "..", "case/pkg", "pkg.evidence", "pkg evidence", "pkg;rm"} {
		legacy := base
		legacy.EvidencePackageID = id
		plan := LegacyS3Plan{
			LegacyPackageID: id,
			TargetLocation:  testTargetPrefix + "/agency-evidence/object",
		}
		err := copier.CopyAndVerify(t.Context(), legacy, plan)
		if err == nil {
			t.Fatalf("package id %q must be rejected before any copy", id)
		}
		if !strings.Contains(err.Error(), "not safe to use as a staging file name") {
			t.Fatalf("package id %q rejection must name the cause, got: %v", id, err)
		}
	}
	// No staging file may have been created, inside or outside the work dir.
	entries, readErr := os.ReadDir(workDir)
	if readErr != nil {
		t.Fatalf("list staging directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected package ids must not leave staging files, found %d", len(entries))
	}
	if _, statErr := os.Stat(filepath.Join(workDir, "..", "etc.staged")); statErr == nil {
		t.Fatal("a traversal package id must never create a file outside the staging directory")
	}
}

func TestStagedPathAcceptsUUIDAndDashTokens(t *testing.T) {
	copier := StagedCommandCopier{WorkDir: t.TempDir()}
	for _, id := range []string{
		"11111111-1111-4111-8111-111111111111",
		"PKG-2026-0001-a",
		"018f3c2a7b9e4f1d9c2b8a6e5d4f3a21",
	} {
		staged, err := copier.stagedPath(id, ".staged")
		if err != nil {
			t.Fatalf("package id %q must stay usable: %v", id, err)
		}
		if filepath.Dir(staged) != filepath.Clean(copier.WorkDir) {
			t.Fatalf("staging path %q must stay directly inside the staging directory", staged)
		}
		if filepath.Base(staged) != id+".staged" {
			t.Fatalf("staging file name %q must derive from the package id", staged)
		}
	}
}

func TestNewMigrationCorrelationID(t *testing.T) {
	first, err := NewMigrationCorrelationID()
	if err != nil {
		t.Fatalf("generate correlation id: %v", err)
	}
	if !isUUID(first) {
		t.Fatalf("correlation id %q is not a UUID", first)
	}
	second, err := NewMigrationCorrelationID()
	if err != nil {
		t.Fatalf("generate correlation id: %v", err)
	}
	if first == second {
		t.Fatal("correlation ids must be unique per run")
	}
}

func sha256Sum(content []byte) []byte {
	digest := sha256.Sum256(content)
	return digest[:]
}
