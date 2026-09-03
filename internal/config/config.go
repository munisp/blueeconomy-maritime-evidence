// Package config resolves the evidence-api configuration from the
// environment and fails closed on any incomplete subsystem. Secrets come
// from the environment only.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/objstore"
)

// Config is the resolved evidence-api configuration.
type Config struct {
	DatabaseURL string
	APIAddr     string

	// PBAC: Keycloak RS256 JWTs validated against the realm JWKS.
	OIDCIssuer   string
	OIDCAudience string
	OIDCJWKSURL  string
	OIDCCAFile   string

	// Producer principal asserted in outbox envelope provenance (Keycloak
	// service-account subject; never a credential).
	PrincipalID   string
	PrincipalRole string

	// List pagination caps.
	ListDefaultLimit int
	ListMaxLimit     int

	ObjectStore objstore.Config
}

// FromEnv loads and validates the configuration, failing closed on any
// missing or invalid value.
func FromEnv() (Config, error) {
	config := Config{
		DatabaseURL:   strings.TrimSpace(os.Getenv("DATABASE_URL")),
		APIAddr:       strings.TrimSpace(os.Getenv("EVIDENCE_API_ADDR")),
		OIDCIssuer:    strings.TrimSpace(os.Getenv("EVIDENCE_OIDC_ISSUER")),
		OIDCAudience:  strings.TrimSpace(os.Getenv("EVIDENCE_OIDC_AUDIENCE")),
		OIDCJWKSURL:   strings.TrimSpace(os.Getenv("EVIDENCE_OIDC_JWKS_URL")),
		OIDCCAFile:    strings.TrimSpace(os.Getenv("EVIDENCE_OIDC_CA_FILE")),
		PrincipalID:   strings.TrimSpace(os.Getenv("EVIDENCE_PRODUCER_PRINCIPAL_ID")),
		PrincipalRole: strings.TrimSpace(os.Getenv("EVIDENCE_PRODUCER_PRINCIPAL_ROLE")),
		ObjectStore: objstore.Config{
			Endpoint:  strings.TrimSpace(os.Getenv("EVIDENCE_S3_ENDPOINT")),
			Region:    strings.TrimSpace(os.Getenv("EVIDENCE_S3_REGION")),
			Bucket:    strings.TrimSpace(os.Getenv("EVIDENCE_S3_BUCKET")),
			AccessKey: strings.TrimSpace(os.Getenv("EVIDENCE_S3_ACCESS_KEY")),
			SecretKey: strings.TrimSpace(os.Getenv("EVIDENCE_S3_SECRET_KEY")),
			PathStyle: parseBool(os.Getenv("EVIDENCE_S3_PATH_STYLE")),
		},
	}
	if config.DatabaseURL == "" {
		return config, errors.New("DATABASE_URL must be injected by the approved secret-management path")
	}
	if config.APIAddr == "" {
		return config, errors.New("EVIDENCE_API_ADDR must be set")
	}
	if config.OIDCIssuer == "" || config.OIDCAudience == "" || config.OIDCJWKSURL == "" {
		return config, errors.New("EVIDENCE_OIDC_ISSUER, EVIDENCE_OIDC_AUDIENCE and EVIDENCE_OIDC_JWKS_URL must be set")
	}
	if config.PrincipalID == "" || config.PrincipalRole == "" {
		return config, errors.New("EVIDENCE_PRODUCER_PRINCIPAL_ID and EVIDENCE_PRODUCER_PRINCIPAL_ROLE must be set")
	}
	defaultLimit, err := strconv.Atoi(defaultEnv("EVIDENCE_LIST_DEFAULT_LIMIT", "50"))
	if err != nil || defaultLimit < 1 {
		return config, errors.New("EVIDENCE_LIST_DEFAULT_LIMIT must be a positive integer")
	}
	maxLimit, err := strconv.Atoi(defaultEnv("EVIDENCE_LIST_MAX_LIMIT", "200"))
	if err != nil || maxLimit < defaultLimit || maxLimit > 1000 {
		return config, errors.New("EVIDENCE_LIST_MAX_LIMIT must be >= the default limit and at most 1000")
	}
	config.ListDefaultLimit = defaultLimit
	config.ListMaxLimit = maxLimit
	presignTTL, err := time.ParseDuration(defaultEnv("EVIDENCE_S3_PRESIGN_TTL", "15m"))
	if err != nil {
		return config, fmt.Errorf("EVIDENCE_S3_PRESIGN_TTL: %w", err)
	}
	config.ObjectStore.PresignTTL = presignTTL
	if err := config.ObjectStore.Validate(); err != nil {
		return config, fmt.Errorf("object store: %w", err)
	}
	return config, nil
}

func defaultEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func parseBool(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "true")
}
