package tracing_test

import (
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/luno/reflex/internal/tracing"
	"github.com/luno/reflex/reflexpb"
)

func TestMarshal(t *testing.T) {
	t.Run("Ensure the trace data is marshal correctly", func(t *testing.T) {
		setup()

		traceID, err := trace.TraceIDFromHex("00000000000000000000000000000009")
		jtest.RequireNil(t, err)

		spanID, err := trace.SpanIDFromHex("0000000000000002")
		jtest.RequireNil(t, err)

		traceState, err := trace.ParseTraceState("k2=v2,k1=v1")
		jtest.RequireNil(t, err)

		spanCtx := trace.NewSpanContext(
			trace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     spanID,
				TraceState: traceState,
				Remote:     true,
			},
		)

		expected := reflexpb.Trace{
			SpanId:  "0000000000000002",
			TraceId: "00000000000000000000000000000009",
		}

		actual, err := tracing.Marshal(spanCtx)
		jtest.RequireNil(t, err)

		expBody, err := proto.Marshal(&expected)
		jtest.RequireNil(t, err)

		require.Equal(t, string(expBody), string(actual))
	})
}
