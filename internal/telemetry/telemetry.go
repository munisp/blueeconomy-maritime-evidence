// Package telemetry wires OpenTelemetry tracing for the maritime-evidence
// components (evidence-migrate CLI and the evidence store library).
//
// Contract (shared with every BlueEconomy service):
//   - OTEL_EXPORTER_OTLP_ENDPOINT unset means tracing is DISABLED; a disabled
//     service runs an explicit no-op tracer and neither boot nor operations
//     change behavior (the one sanctioned fail-open).
//   - When set, export is async/batched and non-blocking; a collector outage
//     means spans are dropped and counted (telemetry_dropped_total), never a
//     failed operation.
//   - Malformed or contradictory configuration is a startup error
//     (fail-closed posture, consistent with the rest of the service).
//   - W3C tracecontext+baggage propagation is always installed so distributed
//     context survives even when export is off.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otlptracegrpc "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Config is the validated telemetry configuration. Enabled is false when no
// OTLP endpoint is configured; every other field is then ignored.
type Config struct {
	Enabled     bool
	Endpoint    string
	Insecure    bool
	ServiceName string
}

// LoadConfig reads OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE,
// OTEL_SERVICE_NAME and OTEL_SDK_DISABLED. An absent endpoint means tracing is
// disabled; a present but malformed endpoint, an unknown boolean value, or a
// contradictory OTEL_SDK_DISABLED=true fails closed.
func LoadConfig(serviceName string) (Config, error) {
	if strings.TrimSpace(serviceName) == "" {
		return Config{}, errors.New("telemetry service name is required")
	}
	config := Config{ServiceName: serviceName}
	if override := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); override != "" {
		if len(override) > 128 {
			return Config{}, errors.New("OTEL_SERVICE_NAME must be at most 128 characters")
		}
		config.ServiceName = override
	}
	disabled, err := parseBoolean("OTEL_SDK_DISABLED")
	if err != nil {
		return Config{}, err
	}
	insecure, err := parseBoolean("OTEL_EXPORTER_OTLP_INSECURE")
	if err != nil {
		return Config{}, err
	}
	config.Insecure = insecure
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if disabled {
		if endpoint != "" {
			return Config{}, errors.New("OTEL_SDK_DISABLED=true conflicts with OTEL_EXPORTER_OTLP_ENDPOINT; remove one (fail-closed)")
		}
		return config, nil
	}
	if endpoint == "" {
		return config, nil
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return Config{}, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must be a host:port pair without scheme, credentials or path: %q", endpoint)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return Config{}, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT has an invalid port: %q", endpoint)
	}
	config.Enabled = true
	config.Endpoint = endpoint
	return config, nil
}

// parseBoolean accepts only empty, "true" or "false"; anything else fails
// closed rather than being silently interpreted.
func parseBoolean(name string) (bool, error) {
	switch value := strings.TrimSpace(os.Getenv(name)); value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be true or false when set", name)
	}
}

// Telemetry carries the tracer and the Prometheus meter pipeline. It is safe
// to use a zero-capacity instance only through Setup.
type Telemetry struct {
	config         Config
	tracer         trace.Tracer
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	metricsHandler http.Handler
	dropped        metric.Int64Counter
}

// Setup builds the meter and tracer pipelines. The Prometheus exporter is
// always local-only (no egress) and backs telemetry_dropped_total; the OTLP
// gRPC trace exporter is created only when enabled.
func Setup(ctx context.Context, config Config) (*Telemetry, error) {
	if strings.TrimSpace(config.ServiceName) == "" {
		return nil, errors.New("telemetry service name is required")
	}
	serviceResource := resource.NewSchemaless(attribute.String("service.name", config.ServiceName))
	meterProvider, metricsHandler, err := newMeterPipeline(serviceResource)
	if err != nil {
		return nil, err
	}
	meter := meterProvider.Meter(config.ServiceName)
	dropped, err := meter.Int64Counter("telemetry_dropped_total", metric.WithDescription("Spans dropped because the OTLP collector was unreachable; telemetry never fails the business path"))
	if err != nil {
		return nil, fmt.Errorf("create dropped-telemetry counter: %w", err)
	}
	telemetry := &Telemetry{
		config:         config,
		meterProvider:  meterProvider,
		metricsHandler: metricsHandler,
		dropped:        dropped,
	}
	// Propagation is installed even when export is disabled: incoming
	// traceparent/baggage must still be honoured so distributed context
	// survives services running with telemetry off.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if !config.Enabled {
		telemetry.tracer = noop.NewTracerProvider().Tracer(config.ServiceName)
		return telemetry, nil
	}
	exporterOptions := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(config.Endpoint)}
	if config.Insecure {
		exporterOptions = append(exporterOptions, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, exporterOptions...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP gRPC trace exporter: %w", err)
	}
	telemetry.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(&countingExporter{next: exporter, dropped: dropped}),
		sdktrace.WithResource(serviceResource),
	)
	otel.SetTracerProvider(telemetry.tracerProvider)
	telemetry.tracer = telemetry.tracerProvider.Tracer(config.ServiceName)
	return telemetry, nil
}

// countingExporter wraps the OTLP exporter so every failed export increments
// telemetry_dropped_total. The batch processor already isolates the business
// path from collector outages (async, bounded queue, drop-on-full); this only
// makes the drops observable.
type countingExporter struct {
	next    sdktrace.SpanExporter
	dropped metric.Int64Counter
}

func (exporter *countingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := exporter.next.ExportSpans(ctx, spans)
	if err != nil {
		exporter.dropped.Add(ctx, int64(len(spans)))
	}
	return err
}

func (exporter *countingExporter) Shutdown(ctx context.Context) error {
	return exporter.next.Shutdown(ctx)
}

// newMeterPipeline installs a Prometheus reader on a private registry so
// repeated Setup calls (tests, in-process binaries) never collide on the
// global Prometheus registry.
func newMeterPipeline(serviceResource *resource.Resource) (*sdkmetric.MeterProvider, http.Handler, error) {
	registry := prometheus.NewRegistry()
	reader, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, nil, fmt.Errorf("create Prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(serviceResource))
	return provider, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), nil
}

// Enabled reports whether OTLP trace export is active.
func (telemetry *Telemetry) Enabled() bool {
	return telemetry.config.Enabled
}

// Tracer returns the service tracer: the OTLP-backed SDK tracer when enabled,
// otherwise the explicit no-op tracer.
func (telemetry *Telemetry) Tracer() trace.Tracer {
	return telemetry.tracer
}

// MetricsHandler serves the Prometheus scrape endpoint. The evidence-migrate
// CLI is short-lived and does not serve it; the handler exists so a future
// long-running evidence service reuses the same pipeline unchanged.
func (telemetry *Telemetry) MetricsHandler() http.Handler {
	return telemetry.metricsHandler
}

// Shutdown flushes and stops both providers. Callers should bound the flush
// with a context timeout (the platform contract is <= 5s).
func (telemetry *Telemetry) Shutdown(ctx context.Context) error {
	var shutdownErr error
	if telemetry.tracerProvider != nil {
		shutdownErr = telemetry.tracerProvider.Shutdown(ctx)
	}
	if telemetry.meterProvider != nil {
		if err := telemetry.meterProvider.Shutdown(ctx); shutdownErr == nil {
			shutdownErr = err
		}
	}
	return shutdownErr
}
