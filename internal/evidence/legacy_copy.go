package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// StagedCommandCopier copies one evidence object from its legacy s3 location
// to the planned abfs location through local staging files, using the approved
// operator CLIs (the AWS CLI for s3 reads and azcopy for ADLS Gen2 traffic).
// Credentials stay with the operator environment; the tool never accepts or
// logs them. Every step fails closed: a non-zero CLI exit, a missing binary
// or a digest mismatch aborts the package before any database mutation.
//
// Verification is two-sided. The staged source object must match the
// package's retained content_sha256 before upload (the registered digest is
// the evidence), and the re-downloaded abfs object must match it again after
// upload (what is registered is what was stored). Supersession is refused on
// any mismatch.
type StagedCommandCopier struct {
	// AWSCLI is the aws binary used as `aws s3 cp <s3-uri> <staged-file>`.
	AWSCLI string
	// AzCopy is the azcopy binary used as
	// `azcopy copy <staged-file> <abfs-uri>` and `azcopy copy <abfs-uri> <file>`.
	AzCopy string
	// WorkDir is an operator-provisioned directory for staged objects. Staged
	// files are removed after each package.
	WorkDir string
}

// NewStagedCommandCopierFromEnv resolves the copier configuration. All three
// values are required; a missing binary or directory fails closed at startup.
//
//	EVIDENCE_MIGRATE_AWS_CLI   path to the aws binary (default: aws on PATH)
//	EVIDENCE_MIGRATE_AZCOPY    path to the azcopy binary (default: azcopy on PATH)
//	EVIDENCE_MIGRATE_WORK_DIR  existing staging directory (required)
func NewStagedCommandCopierFromEnv() (StagedCommandCopier, error) {
	awsCLI, err := requireExecutable("EVIDENCE_MIGRATE_AWS_CLI", "aws")
	if err != nil {
		return StagedCommandCopier{}, err
	}
	azCopy, err := requireExecutable("EVIDENCE_MIGRATE_AZCOPY", "azcopy")
	if err != nil {
		return StagedCommandCopier{}, err
	}
	workDir := os.Getenv("EVIDENCE_MIGRATE_WORK_DIR")
	if workDir == "" {
		return StagedCommandCopier{}, errors.New("EVIDENCE_MIGRATE_WORK_DIR must name an existing staging directory")
	}
	stat, err := os.Stat(workDir)
	if err != nil || !stat.IsDir() {
		return StagedCommandCopier{}, fmt.Errorf("EVIDENCE_MIGRATE_WORK_DIR is not a readable directory: %s", workDir)
	}
	return StagedCommandCopier{AWSCLI: awsCLI, AzCopy: azCopy, WorkDir: workDir}, nil
}

func requireExecutable(envName, defaultName string) (string, error) {
	name := os.Getenv(envName)
	if name == "" {
		name = defaultName
	}
	resolved, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("%s (%s) is required for the legacy s3 migration and was not found: %w", envName, name, err)
	}
	return resolved, nil
}

// CopyAndVerify copies one planned package and verifies content integrity on
// both sides of the copy. It returns an error — and performs no supersession
// — unless the abfs object provably holds the registered digest.
func (c StagedCommandCopier) CopyAndVerify(ctx context.Context, legacy Package, plan LegacyS3Plan) error {
	if plan.LegacyPackageID == "" || plan.LegacyPackageID != legacy.EvidencePackageID {
		return errors.New("re-registration plan does not match the legacy package")
	}
	staged, err := c.stagedPath(legacy.EvidencePackageID, ".staged")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(staged) }()

	if err := c.run(ctx, c.AWSCLI, "s3", "cp", legacy.ContentLocation, staged); err != nil {
		return fmt.Errorf("fetch legacy object %s: %w", legacy.ContentLocation, err)
	}
	if err := VerifyStagedContentSHA256(staged, legacy.ContentSHA256); err != nil {
		return fmt.Errorf("legacy object %s fails content verification; refusing re-registration: %w",
			legacy.ContentLocation, err)
	}
	if err := c.run(ctx, c.AzCopy, "copy", staged, plan.TargetLocation); err != nil {
		return fmt.Errorf("upload replacement object %s: %w", plan.TargetLocation, err)
	}

	verify, err := c.stagedPath(legacy.EvidencePackageID, ".verify")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(verify) }()
	if err := c.run(ctx, c.AzCopy, "copy", plan.TargetLocation, verify); err != nil {
		return fmt.Errorf("re-download replacement object %s for verification: %w", plan.TargetLocation, err)
	}
	if err := VerifyStagedContentSHA256(verify, legacy.ContentSHA256); err != nil {
		return fmt.Errorf("replacement object %s fails content verification; refusing supersession: %w",
			plan.TargetLocation, err)
	}
	return nil
}

// stagedPath builds the staging file path for one DB-supplied package ID.
// The ID is read from the packages database, so it is validated before it
// touches the filesystem: it must be a UUID or a strict alphanumeric-dash
// token (no path separators, dots, or whitespace). As defense-in-depth the
// joined path must also resolve to a file directly inside WorkDir. Anything
// else fails closed before any operator CLI runs.
func (c StagedCommandCopier) stagedPath(packageID, suffix string) (string, error) {
	if !validStagingPackageID(packageID) {
		return "", fmt.Errorf("evidence package id %q is not safe to use as a staging file name", packageID)
	}
	joined := filepath.Join(c.WorkDir, packageID+suffix)
	if filepath.Dir(joined) != filepath.Clean(c.WorkDir) {
		return "", fmt.Errorf("staging path for evidence package id %q escapes the staging directory", packageID)
	}
	return joined, nil
}

func validStagingPackageID(value string) bool {
	if isUUID(value) {
		return true
	}
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') ||
			(character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			character == '-') {
			return false
		}
	}
	return true
}

func (c StagedCommandCopier) run(ctx context.Context, binary string, arguments ...string) error {
	command := exec.CommandContext(ctx, binary, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s failed: %w: %s", binary, err, string(output))
	}
	return nil
}

// VerifyStagedContentSHA256 streams a staged object and compares its SHA-256
// digest with the registered lower-case hexadecimal digest. A mismatch is a
// hard error; the caller must refuse supersession.
func VerifyStagedContentSHA256(path, expectedSHA256Hex string) error {
	if !validSHA256(expectedSHA256Hex) {
		return fmt.Errorf("registered digest %q is not a lower-case SHA-256 hexadecimal digest", expectedSHA256Hex)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open staged object: %w", err)
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return fmt.Errorf("hash staged object: %w", err)
	}
	actual := hex.EncodeToString(digest.Sum(nil))
	if actual != expectedSHA256Hex {
		return fmt.Errorf("staged object digest %s does not match registered digest %s", actual, expectedSHA256Hex)
	}
	return nil
}
