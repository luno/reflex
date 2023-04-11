package tracing

import (
	"context"

	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"go.opentelemetry.io/otel/trace"
)

// Extract is a wrapper of opentelemetry's SpanFromContext that also wraps traceID validation.
func Extract(ctx context.Context) (trace.SpanContext, bool) {
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	valid := spanCtx.HasTraceID() && spanCtx.HasSpanID()
	return spanCtx, valid
}

// Inject is an best effort to load the protobuf encoded reflexpb.Trace into the context.
func Inject(ctx context.Context, data []byte) context.Context {
	// Don't attempt to unmarshal if there is no data
	if len(data) == 0 {
		return ctx
	}

	spanCtx, err := Unmarshal(data)
	if err != nil {
		// Return context unchanged as its only best effort
		return ctx
	}

	// Update the context to contain the loaded parent span context
	ctx = trace.ContextWithRemoteSpanContext(ctx, spanCtx)

	// Add trace id for logging
	traceID := spanCtx.TraceID().String()
	ctx = log.ContextWith(ctx, j.KV("trace_id", traceID))
	return ctx
}
