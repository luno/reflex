// Package reflex provides events (state change notifications) for mutable sql
// tables. It also supports streaming consumption of these events both directly
// locally or via gRPC. It therefore aims to provide streams of sql events
// similar to other database streaming features, like DynamoDB Streams, except
// that these events do not contain any data, but are merely pointers.
//
// Reflex events are pointers to state changes of mutable data.
//    event_id int16         // unique id of the event
//    type enum              // state change type
//    foreign_id string      // id of the mutable datum
//    timestamp timestamp    // timestamp of the event
//
// Events are inserted as part of sql transactions that modify the data. This
// ensures strong data consistency; exactly one event per state change.
//
// The EventsTable wraps a event DB table allowing insertion of events.
// The canonical pattern is to define an EventsTable per mutable
// data table. Ex. users and user_events, payments and payment_events. Note that
// event insertion is generally defined on the data layer, not business logic layer.
//
// The datum (data entity) referred to and state change performed on it
// are implied by the event source and type. Ex. An event produced
// by the "user service" with type "UserCreated", or an event by the "auth service"
// with type "APIKeyVerified".
//
// The EventsTable also provides event streams from a arbitrary point in the past.
// Reflex also supports exposing these event streams via gRPC.
//
// Consumers consume the event stream keeping a cursor of the last event. This
// ensures at-least-once data consistency. Consumers therefore need to persist
// their cursor.
//
// The CursorsTable wraps a DB table allowing getting and setting of cursors.
// The canonical pattern is to define one cursor table per service.
//
// There are two ways to consume event streams:
//   1. EventTable + CursorsTable: The consumer logic has local access to both
//   the events and consumers tables. Ex. User service sends an email on UserCreated.
//
//   2. gRPC Stream + CursorsTable: The consumer logic has access to a local
//   CursorsTable, but requests the event stream from a remote service via gRPC.
//   Ex. The Fraud service consumes PaymentCreated events from the payments
//   service. It has its own DB and CursorsTable.
package reflex
