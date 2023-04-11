package tracing_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex/internal/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestExtract(t *testing.T) {
	t.Run("Ensure embedded span is loaded into context returned", func(t *testing.T) {
		setup()

		ctx := context.Background()
		ctx, span := otel.Tracer("reflex").Start(ctx, "span")
		span.End()

		actualSpanCtx, hasTrace := tracing.Extract(ctx)
		require.True(t, hasTrace)
		require.NotEmpty(t, actualSpanCtx.TraceID().String())
		require.NotEmpty(t, actualSpanCtx.SpanID().String())
		require.Equal(t, span.SpanContext(), actualSpanCtx)
	})
}

func TestInject(t *testing.T) {
	setup()

	traceID, err := trace.TraceIDFromHex("00000000000000000000000000000009")
	jtest.RequireNil(t, err)

	spanID, err := trace.SpanIDFromHex("0000000000000002")
	jtest.RequireNil(t, err)

	spanCtx := trace.NewSpanContext(
		trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
		},
	)

	ctx := context.Background()
	data, err := tracing.Marshal(spanCtx)
	jtest.RequireNil(t, err)

	ctx = tracing.Inject(ctx, data)
	expected := "context.Background.WithValue(type trace.traceContextKeyType, val <not Stringer>).WithValue(type internal.contextKey, val <not Stringer>)"
	require.Equal(t, expected, fmt.Sprint(ctx))
}

func setup() {
	tp := tracesdk.NewTracerProvider()
	otel.SetTracerProvider(tp)
}
