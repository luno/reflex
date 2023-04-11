package tracing_test

import (
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/luno/reflex/internal/tracing"
)

func TestUnmarshal(t *testing.T) {
	t.Run("Ensure that marshal and unmarshal are cohesive", func(t *testing.T) {
		setup()

		traceID, err := trace.TraceIDFromHex("00000000000000000000000000000009")
		jtest.RequireNil(t, err)

		spanID, err := trace.SpanIDFromHex("0000000000000002")
		jtest.RequireNil(t, err)

		expectedSpanCtx := trace.NewSpanContext(
			trace.SpanContextConfig{
				TraceID: traceID,
				SpanID:  spanID,
			},
		)

		traceData, err := tracing.Marshal(expectedSpanCtx)
		jtest.RequireNil(t, err)

		actualSpanCtx, err := tracing.Unmarshal(traceData)
		jtest.RequireNil(t, err)

		require.Equal(t, expectedSpanCtx, actualSpanCtx)
	})
}
