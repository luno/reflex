package rsql

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/luno/reflex/internal/tracing"
)

//go:generate go test . -run Test_makeInsertManyQuery -update -clean

func Test_makeInsertManyQuery(t *testing.T) {
	ctx := context.Background()

	defaultSchema := eTableSchema{
		name:           "events",
		idField:        "id",
		timeField:      "timestamp",
		typeField:      "type",
		foreignIDField: "foreign_id",
	}

	assert := func(t *testing.T, q string, args []any) {
		buf := new(bytes.Buffer)
		buf.WriteString(q)
		buf.WriteString("\n")
		for _, arg := range args {
			buf.WriteString("\n")
			buf.WriteString(fmt.Sprint(arg))
		}
		goldie.New(t).Assert(t, t.Name(), buf.Bytes())
	}

	t.Run("empty", func(t *testing.T) {
		q, args, err := makeInsertManyQuery(ctx, defaultSchema, nil)
		jtest.RequireNil(t, err)
		assert(t, q, args)
	})

	t.Run("one", func(t *testing.T) {
		q, args, err := makeInsertManyQuery(ctx, defaultSchema, []EventToInsert{
			{"fid", testEventType(1), nil},
		})
		jtest.RequireNil(t, err)
		assert(t, q, args)
	})

	t.Run("two", func(t *testing.T) {
		q, args, err := makeInsertManyQuery(ctx, defaultSchema, []EventToInsert{
			{"fid1", testEventType(1), nil},
			{"fid2", testEventType(2), nil},
		})
		jtest.RequireNil(t, err)
		assert(t, q, args)
	})

	t.Run("more", func(t *testing.T) {
		var events []EventToInsert
		for i := range 5 {
			events = append(events, EventToInsert{
				ForeignID: fmt.Sprintf("fid%d", i+1),
				Type:      testEventType(i),
			})
		}
		q, args, err := makeInsertManyQuery(ctx, defaultSchema, events)
		jtest.RequireNil(t, err)
		assert(t, q, args)
	})

	t.Run("metadata_error", func(t *testing.T) {
		_, _, err := makeInsertManyQuery(ctx, defaultSchema, []EventToInsert{
			{"fid", testEventType(1), []byte("metadata")},
		})
		require.ErrorContains(t, err, "metadata not enabled")
	})

	t.Run("with_metadata", func(t *testing.T) {
		schemaWithMetadata := defaultSchema
		schemaWithMetadata.metadataField = "metadata"
		q, args, err := makeInsertManyQuery(ctx, schemaWithMetadata, []EventToInsert{
			{"fid", testEventType(1), []byte("metadata")},
		})
		jtest.RequireNil(t, err)
		assert(t, q, args)
	})

	t.Run("with_trace", func(t *testing.T) {
		schemaWithTrace := defaultSchema
		schemaWithTrace.traceField = "trace"
		traceID, err := trace.TraceIDFromHex("00000000000000000000000000000009")
		jtest.RequireNil(t, err)
		spanID, err := trace.SpanIDFromHex("0000000000000002")
		jtest.RequireNil(t, err)
		data, err := tracing.Marshal(trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID,
			SpanID:  spanID,
		}))
		jtest.RequireNil(t, err)
		ctx := tracing.Inject(ctx, data)
		q, args, err := makeInsertManyQuery(ctx, schemaWithTrace, []EventToInsert{
			{"fid", testEventType(1), nil},
		})
		jtest.RequireNil(t, err)
		assert(t, q, args)
	})
}

type testEventType int

func (t testEventType) ReflexType() int { return int(t) }