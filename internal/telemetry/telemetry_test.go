package telemetry

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestLoadConfigDefaultsToDisabled(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	config, err := LoadConfig("blueeconomy-maritime-evidence")
	if err != nil {
		t.Fatalf("default config must load: %v", err)
	}
	if config.Enabled {
		t.Fatal("tracing must default to disabled when no endpoint is configured")
	}
}

func TestLoadConfigFailsClosedOnMalformedValues(t *testing.T) {
	cases := []struct {
		name string
		envs map[string]string
	}{
		{"endpoint with scheme", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.example.invalid:4317"}},
		{"endpoint without port", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "collector.example.invalid"}},
		{"endpoint with bad port", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "collector.example.invalid:not-a-port"}},
		{"endpoint with credentials", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "user:secret@collector:4317"}},
		{"disabled flag garbage", map[string]string{"OTEL_SDK_DISABLED": "yes"}},
		{"insecure flag garbage", map[string]string{"OTEL_EXPORTER_OTLP_INSECURE": "1"}},
		{"conflicting disabled and endpoint", map[string]string{"OTEL_SDK_DISABLED": "true", "OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			t.Setenv("OTEL_SDK_DISABLED", "")
			t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
			for name, value := range testCase.envs {
				t.Setenv(name, value)
			}
			if _, err := LoadConfig("blueeconomy-maritime-evidence"); err == nil {
				t.Fatalf("case %q must fail closed", testCase.name)
			}
		})
	}
}

// TestPropagationRoundTrip proves W3C tracecontext+baggage injected on one
// side of a carrier re-emerges on the other, so evidence operations continue
// the caller's trace and keep tenant attribution.
func TestPropagationRoundTrip(t *testing.T) {
	config, err := LoadConfig("blueeconomy-maritime-evidence")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if _, err := Setup(context.Background(), config); err != nil {
		t.Fatalf("setup: %v", err)
	}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("round-trip-test").Start(context.Background(), "evidence.package.create")
	defer span.End()
	bag, err := baggage.Parse("tenant.id=tenant-niwa,agency=NIWA")
	if err != nil {
		t.Fatalf("parse baggage: %v", err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if carrier.Get("traceparent") == "" {
		t.Fatal("injection must write a traceparent")
	}
	extracted := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	remote := trace.SpanContextFromContext(extracted)
	if !remote.IsValid() || remote.TraceID() != span.SpanContext().TraceID() || remote.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("round trip must preserve trace/span identity, got %v want %v", remote, span.SpanContext())
	}
	if value := baggage.FromContext(extracted).Member("tenant.id").Value(); value != "tenant-niwa" {
		t.Fatalf("tenant.id baggage must survive the round trip, got %q", value)
	}
	if value := baggage.FromContext(extracted).Member("agency").Value(); value != "NIWA" {
		t.Fatalf("agency baggage must survive the round trip, got %q", value)
	}
}

// TestDisabledSetupOperationsUnchanged is the telemetry-off contract: setup
// succeeds with no OTLP endpoint, operations run on an explicit no-op
// tracer, and shutdown is clean (the one sanctioned fail-open).
func TestDisabledSetupOperationsUnchanged(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
	config, err := LoadConfig("blueeconomy-maritime-evidence")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	telemetry, err := Setup(context.Background(), config)
	if err != nil {
		t.Fatalf("disabled setup must succeed: %v", err)
	}
	if telemetry.Enabled() {
		t.Fatal("telemetry must report disabled")
	}
	_, span := telemetry.Tracer().Start(context.Background(), "evidence.package.get")
	if span.IsRecording() {
		t.Fatal("disabled mode must use a no-op, non-recording span")
	}
	span.End()
	response := httptest.NewRecorder()
	telemetry.MetricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatal("metrics pipeline must be available even when tracing is disabled")
	}
	if err := telemetry.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// failingExporter always fails, simulating a collector outage.
type failingExporter struct{}

func (failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return errors.New("collector unreachable")
}

func (failingExporter) Shutdown(context.Context) error { return nil }

// TestCountingExporterCountsDrops proves collector-down drops are observed
// via telemetry_dropped_total instead of failing the business path.
func TestCountingExporterCountsDrops(t *testing.T) {
	config, err := LoadConfig("blueeconomy-maritime-evidence")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	telemetry, err := Setup(context.Background(), config)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer func() { _ = telemetry.Shutdown(context.Background()) }()
	exporter := &countingExporter{next: failingExporter{}, dropped: telemetry.dropped}
	if err := exporter.ExportSpans(context.Background(), make([]sdktrace.ReadOnlySpan, 3)); err == nil {
		t.Fatal("the wrapped exporter error must propagate to the batch processor")
	}
	response := httptest.NewRecorder()
	telemetry.MetricsHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "telemetry_dropped_total{") || !strings.Contains(text, "} 3") {
		t.Fatalf("telemetry_dropped_total must count the 3 dropped spans, got:\n%s", body)
	}
}
