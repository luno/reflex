package tracing

import (
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/luno/reflex/reflexpb"
)

// Marshal encodes the opentelemetry SpanContext into a reflex proto and encodes it using a proto encoder for storage
// in the database.
func Marshal(span trace.SpanContext) ([]byte, error) {
	tr := reflexpb.Trace{
		TraceId: span.TraceID().String(),
		SpanId:  span.SpanID().String(),
	}

	return proto.Marshal(&tr)
}
