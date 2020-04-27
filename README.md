# Reflex
[![Build](https://github.com/luno/reflex/workflows/Go/badge.svg?branch=master)](https://github.com/luno/reflex/actions?query=workflow%3AGo)
[![Go Report Card](https://goreportcard.com/badge/github.com/luno/reflex?style=flat-square)](https://goreportcard.com/report/github.com/luno/reflex)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/github.com/luno/reflex)

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

**It is composable**

- `CursorStore` and `StreamFunc` are decoupled and data store agnostic.
- This results in multiple types of `Specs`, including:
  - local `CursorStore` with gRPC `StreamFunc` (remove events) 
  - local `CursorStore` and local `StreamFunc` (local events and local cursors) 
  - remote `CursorStore` with gRPC `StreamFunc` (remove events and remote cursors) 
