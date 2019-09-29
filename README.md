# Reflex
[![Go Report Card](https://goreportcard.com/badge/github.com/luno/reflex?style=flat-square)](https://goreportcard.com/report/github.com/luno/reflex)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/github.com/luno/reflex)

> reflex /ˈriːflɛks/
>
>  1. an action that is performed without conscious thought as a response to a stimulus.
>  *"the system is equipped with ninja-like reflexes"*
>  2. a thing which is determined by and reproduces the essential features or qualities of something else.
>  *"business logic is no more than a reflex of user actions"*

Reflex provides an API for building distributed event notification streams.

## Overview

The main interface is a `Consumable` that will stream events to the a `Consumer` with at-least-once data consistency until an error in encountered.

```go
logic := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
	log.Printf("Processed a new event: %#v", e)
	return f.Tempt()
}

consumer := reflex.NewConsumer("my-demo-consumer", logic)
consumble := reflex.NewConsumable(StreamFunc, cursorStore)

for {
	err := consumable.Consume(context.Background(), consumer)
	log.Printf("Consume error: %v", err)
}
```

`Consumer` encapsulates your business logic:
- `Name`: Unique name used for cursor persistence.
- `ConsumeFunc`: Function called with new events.

`Consumable` is composed of 2 parts:
- `StreamFunc`: Function interface providing event streams from an offset (cursor).
- `CursorStore`: Interface providing cursor persistence; `GetCursor` on start and `SetCursor` on successfully consumed events. 

## Characteristics

**Events are primarily state change notifications**

- `Event.Type` is an enum indicating the type of state change; eg. `UserCreated`, `TradeCompleted`,`EmailUpdated`.
- `Event.ForeignID` points to the associated (mutable) entity.
- `Event.Timestamp` provides the time the event occurred.
- It must have a exactly-once consistency.
- It is not designed as "Event Sourcing" but rather "Event Notification" see this [video](https://youtu.be/STKCRSUsyP0).
  
**The event store providing the `StreamFunc` API is inspired by Kafka partitions**

- It stores events as immutable log.
- It must be queryable by offset (cursor/eventID).
- It must support independent concurrent stream queries with different offsets. 
- It must retain events for reasonable amount of time (days).
- It should be responsive, new events should stream within microseconds. 
  
**Errors always result in the consumer getting stuck (failing fast)**

- On any error, the cursor will not be updated and the `Consumable.Consume` function will return.
- In the case of transient errors (network, io, shutdown, etc), merely re-calling `Consume` will succeed (at some point).
- In the case of logic or data errors, it is up to you to either fix the bug or catch and ignore the error.  
  
**It is designed for microservices**

- gRPC implementations are provided for both `StreamFunc` and `Consumable`.
- This allows peer-to-peer event streaming without a central event bus.

**It is composable**

- `CursorStore` and `StreamFunc` are decoupled and data store agnostic.
- This results in the following types of `Consumable`:
  - local `CursorStore` with gRPC `StreamFunc` (remove events) 
  - local `CursorStore` and local `StreamFunc` (local events and local cursors) 