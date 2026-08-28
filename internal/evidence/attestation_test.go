package evidence

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/provenance"
)

func attestationSigner(t *testing.T) *provenance.Signer {
	t.Helper()
	_, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := provenance.NewSigner(SigningKeyID, private)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

func attestationDirectory(t *testing.T, signer *provenance.Signer) *provenance.Directory {
	t.Helper()
	directory, err := provenance.ParseDirectory([]byte(fmt.Sprintf(`{%q:%q}`, signer.KeyID(), signer.PublicKey())))
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func TestReregistrationAttestationRoundTrip(t *testing.T) {
	signer := attestationSigner(t)
	attestation, err := SignReregistrationAttestation(signer, "legacy-1", "replacement-1",
		"abfs://evidence@account.dfs.core.usgovcloudapi.net/base/legacy-1", "operator-1",
		"corr-1", time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if attestation.Provenance.Signature == "" {
		t.Fatal("attestation signature is empty")
	}
	encoded, err := json.Marshal(attestation)
	if err != nil {
		t.Fatal(err)
	}
	if err := attestationDirectory(t, signer).VerifyEnvelope(encoded); err != nil {
		t.Fatalf("attestation signature does not verify: %v", err)
	}
}

func TestReregistrationAttestationTamperDetection(t *testing.T) {
	signer := attestationSigner(t)
	attestation, err := SignReregistrationAttestation(signer, "legacy-1", "replacement-1",
		"abfs://evidence@account.dfs.core.usgovcloudapi.net/base/legacy-1", "operator-1",
		"corr-1", time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(attestation)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(encoded), "replacement-1", "replacement-2", 1)
	if err := attestationDirectory(t, signer).VerifyEnvelope([]byte(tampered)); err == nil {
		t.Fatal("tampered attestation verified")
	}
}

func TestReregistrationAttestationFailsClosed(t *testing.T) {
	signer := attestationSigner(t)
	if _, err := SignReregistrationAttestation(nil, "a", "b", "c", "d", "e", time.Now()); err == nil {
		t.Fatal("missing signer accepted")
	}
	if _, err := SignReregistrationAttestation(signer, "", "b", "c", "d", "e", time.Now()); err == nil {
		t.Fatal("missing legacy package id accepted")
	}
	if _, err := SignReregistrationAttestation(signer, "a", "b", "c", "d", "e", time.Time{}); err == nil {
		t.Fatal("zero timestamp accepted")
	}
}
