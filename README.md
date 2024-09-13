# Reflex
[![Go](https://github.com/luno/reflex/actions/workflows/test.yml/badge.svg)](https://github.com/luno/reflex/actions/workflows/test.yml)[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=luno_reflex&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=luno_reflex)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=luno_reflex&metric=coverage)](https://sonarcloud.io/summary/new_code?id=luno_reflex)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=luno_reflex&metric=bugs)](https://sonarcloud.io/summary/new_code?id=luno_reflex)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=luno_reflex&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=luno_reflex)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=luno_reflex&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=luno_reflex)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=luno_reflex&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=luno_reflex)
[![Go Report Card](https://goreportcard.com/badge/github.com/luno/reflex)](https://goreportcard.com/report/github.com/luno/reflex)
[![GoDoc](https://godoc.org/github.com/luno/reflex?status.png)](https://godoc.org/github.com/luno/reflex)

> reflex /ˈriːflɛks/
>
>  1. an action that is performed without conscious thought as a response to a stimulus.
>  *"the system is equipped with ninja-like reflexes"*
>  2. a thing which is determined by and reproduces the essential features or qualities of something else.
>  *"business logic is no more than a reflex of user actions"*

Reflex provides an API for building distributed event notification streams.

> Note reflex is used in production at Luno, but still undergoing active development, **breaking changes are on the way**.

## Overview

```go
logic := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
	log.Printf("Consumed event: %#v", e)
	return f.Tempt()
}
consumer := reflex.NewConsumer("logging-consumer", logic)

spec := reflex.NewSpec(streamFunc, cursorStore, consumer)

for {
	err := reflex.Run(context.Background(), spec)
	log.Printf("Consume error: %v", err)
}
```

`Consumer` encapsulates the business logic triggered on each event. It has of a `name` 
used for cursor persistence and metrics.

`StreamFunc` is the event source providing event streams from an offset (cursor).

`CursorStore` provides cursor persistence; `GetCursor` on start and `SetCursor` on successfully consumed events. 

`Spec` combines all three above elements required to stream and consume reflex events. 
It is passed to `reflex.Run` which streams events from the source to the consumer updating the cursor on success.

See `StreamFunc` and `CursorStore` implementations [below](#sources_and_implementations).

## Characteristics

**Events are primarily state change notifications**

- `Event.ID` is the unique event identifier. It is used as the cursor.
- `Event.Type` is an enum indicating the type of state change; eg. `UserCreated`, `TradeCompleted`,`EmailUpdated`.
- `Event.ForeignID` points to the associated (mutable) entity.
- `Event.Timestamp` provides the time the event occurred.
- `Event.Metadata` provides custom unstructured event metadata.
- Event persistence implementations must provide exactly-once or at-least-once semantics.
- It is not designed as "Event Sourcing" but rather "Event Notification" see this [video](https://youtu.be/STKCRSUsyP0).
  
**The event store providing the `StreamFunc` API is inspired by Kafka partitions**

- It stores events as immutable ordered log.
- It must be queryable by offset (cursor, event ID).
- It must support independent concurrent stream queries with different offsets. 
- It must retain events for reasonable amount of time (days).
- It should be responsive, new events should stream within milliseconds. 
  
**Errors always result in the consumer getting stuck (failing fast)**

- On any error, the cursor will not be updated and the `reflex.Run` function will return.
- In the case of transient errors (network, io, shutdown, etc), merely re-calling `Run` will succeed (at some point).
- In the case of logic or data errors, it is up to the user to either fix the bug or catch and ignore the error.  
  
**It is designed for micro-services**

- gRPC implementations are provided for `StreamFunc`.
- This allows peer-to-peer event streaming without a central event bus.
- It allows encapsulating events behind a API; #microservices_own_their_own_data 

**It is composable**

- `CursorStore` and `StreamFunc` are decoupled and data source/store agnostic.
- This results in multiple types of `Specs`, including:
  - rsql `CursorStore` with gRPC `StreamFunc` (remove service events and local mysql cursors) 
  - rsql `CursorStore` and rsql `StreamFunc` (local mysql events and cursors) 
  - remote `CursorStore` with gRPC `StreamFunc` (remove service events and remote cursors) 

### Sources and implementations

The `github.com/luno/reflex` package provides the main framework API with types and interfaces. 

The `github.com/luno/reflex/rpatterns` package provides patterns for common reflex use-cases.

The following packages provide `reflex.StreamFunc` event stream source implementations:
 - [github.com/luno/reflex/rsql](https://pkg.go.dev/github.com/luno/reflex/rsql): mysql backed events with `rsql.EventsTable`.
 - [github.com/luno/reflex/rblob](https://pkg.go.dev/github.com/luno/reflex/rblob): [gocloud](https://gocloud.dev/howto/blob/) blob store (S3,GCS) backend events with `rblob.Bucket`. 
 - [experimental] [github.com/corverroos/rscylla](https://github.com/corverroos/rscylla): [scyllaDB CDC log](https://docs.scylladb.com/using-scylla/cdc/) backed events.
 - [experimental] [github.com/corverroos/rlift](https://github.com/corverroos/rlift): [liftbridge](https://github.com/liftbridge-io/liftbridge) backed events.
 
The following packages provide `reflex.CursorStore` cursor store implementations:
 - [github.com/luno/reflex/rsql](github.com/luno/reflex/rsql]): mysql table cursors with `rsql.CursorsTable`.
 - [experimental] [github.com/corverroos/rlift](https://github.com/corverroos/rlift): [liftbridge](https://github.com/liftbridge-io/liftbridge) table cursors with `rlift.CursorStore`.
