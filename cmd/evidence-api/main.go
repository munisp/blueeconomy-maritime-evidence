// evidence-api is the /v1/evidence REST boundary. Configuration is
// fail-closed from the environment: without a database, a Keycloak OIDC
// configuration, an object-store configuration and an envelope signing key
// the service refuses to start.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/api"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/auth"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/config"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/events"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/evidence"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/objstore"
)

func main() {
	if err := run(); err != nil {
		log.Printf("evidence-api: %v", err)
		os.Exit(1)
	}
}

func run() error {
	serviceConfig, err := config.FromEnv()
	if err != nil {
		return err
	}
	signer, err := events.SignerFromEnv()
	if err != nil {
		return fmt.Errorf("envelope signing key: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(serviceConfig.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	evidence.ConfigurePool(poolConfig)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}

	store, err := evidence.NewStore(pool).WithEvents(signer, serviceConfig.PrincipalID, serviceConfig.PrincipalRole)
	if err != nil {
		return err
	}
	objects, err := objstore.NewClient(serviceConfig.ObjectStore)
	if err != nil {
		return fmt.Errorf("object store: %w", err)
	}
	server, err := api.NewServer(store, objects, serviceConfig.ObjectStore.Bucket, api.ListLimits{
		Default: serviceConfig.ListDefaultLimit,
		Max:     serviceConfig.ListMaxLimit,
	})
	if err != nil {
		return err
	}
	jwksURL, err := parseURL(serviceConfig.OIDCJWKSURL)
	if err != nil {
		return fmt.Errorf("EVIDENCE_OIDC_JWKS_URL: %w", err)
	}
	authenticator, err := auth.NewOIDCAuthenticator(serviceConfig.OIDCIssuer, serviceConfig.OIDCAudience, jwksURL, serviceConfig.OIDCCAFile)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:              serviceConfig.APIAddr,
		Handler:           server.Handler(authenticator),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	log.Printf("evidence-api listening on %s", serviceConfig.APIAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func parseURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" && parsed.Scheme != "http" || parsed.Host == "" {
		return nil, errors.New("must be an absolute http(s) URL")
	}
	return parsed, nil
}
