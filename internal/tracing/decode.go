package tracing

import (
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/luno/reflex/reflexpb"
)

// Unmarshal decodes the protobuf byte slice and reconstructs it into an opentelemetry SpanContext
func Unmarshal(data []byte) (trace.SpanContext, error) {
	var tr reflexpb.Trace
	err := proto.Unmarshal(data, &tr)
	if err != nil {
		return trace.SpanContext{}, err
	}

	traceID, err := trace.TraceIDFromHex(tr.TraceId)
	if err != nil {
		return trace.SpanContext{}, err
	}

	spanID, err := trace.SpanIDFromHex(tr.SpanId)
	if err != nil {
		return trace.SpanContext{}, err
	}

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}), nil
}
