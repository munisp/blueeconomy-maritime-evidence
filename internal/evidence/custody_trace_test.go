package evidence

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestCustodyTracerNoopWhenDisabled proves the custody tracer is inert
// without an SDK provider, so telemetry-off deployments keep their exact
// pre-instrumentation behavior.
func TestCustodyTracerNoopWhenDisabled(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())
	_, span := custodyTracer().Start(context.Background(), "evidence.package.get")
	if span.IsRecording() {
		t.Fatal("custody spans must be no-op when no SDK provider is installed")
	}
	endCustodySpan(span, nil)
}
// TestCustodySpansRecordOperations proves chain-of-custody spans end cleanly
// on success, treat sentinel domain outcomes as control flow (not errors)
// and flag genuine failures — with no database required.
func TestCustodySpansRecordOperations(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	otel.SetTracerProvider(provider)
	ctx := context.Background()

	_, successSpan := custodyTracer().Start(ctx, "evidence.package.create")
	endCustodySpan(successSpan, nil)

	_, notFoundSpan := custodyTracer().Start(ctx, "evidence.package.get")
	endCustodySpan(notFoundSpan, ErrNotFound)

	_, conflictSpan := custodyTracer().Start(ctx, "evidence.package.create")
	endCustodySpan(conflictSpan, ErrIdempotencyConflict)

	_, failureSpan := custodyTracer().Start(ctx, "evidence.validation.record")
	endCustodySpan(failureSpan, errors.New("connection reset"))

	spans := recorder.Ended()
	if len(spans) != 4 {
		t.Fatalf("four spans expected, got %d", len(spans))
	}
	for index, span := range spans {
		wantError := index == 3
		if wantError && span.Status().Code != codes.Error {
			t.Fatalf("genuine failure must mark the span as error, got %v", span.Status())
		}
		if !wantError && span.Status().Code == codes.Error {
			t.Fatalf("span %q must not be marked as error (sentinel/success outcome)", span.Name())
		}
	}
}

