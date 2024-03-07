package reflex

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/luno/reflex/internal/metrics"
	"github.com/luno/reflex/internal/tracing"
)

const (
	defaultLagAlert    = 30 * time.Minute
	defaultActivityTTL = 24 * time.Hour
)

type ConsumerFunc func(context.Context, *Event) error

type consumer struct {
	fn          ConsumerFunc
	name        string
	lagAlert    time.Duration
	activityTTL time.Duration

	ageHist            prometheus.Observer
	lagGauge           prometheus.Gauge
	lagAlertGauge      prometheus.Gauge
	errorCounter       prometheus.Counter
	latencyHist        prometheus.Observer
	filterIncludeTypes []EventType
	activityKey        string
	filterEvent        EventFilter
	rfn                RecoveryFunc
}

// ConsumerOption will change the behaviour of the consumer
type ConsumerOption func(*consumer)

// WithConsumerLagAlert provides an option to set the consumer lag alert
// threshold.
func WithConsumerLagAlert(d time.Duration) ConsumerOption {
	return func(c *consumer) {
		c.lagAlert = d
	}
}

// WithoutConsumerLag provides an option to disable the consumer lag alert.
func WithoutConsumerLag() ConsumerOption {
	return func(c *consumer) {
		c.lagAlert = -1
	}
}

// WithConsumerLagAlertGauge provides an option to set the consumer lag alert
// gauge. Handy for custom alert metadata as labels.
func WithConsumerLagAlertGauge(g prometheus.Gauge) ConsumerOption {
	return func(c *consumer) {
		c.lagAlertGauge = g
	}
}

// WithConsumerActivityTTL provides an option to set the consumer activity
// metric ttl; ie. if no events is consumed in `tll` duration the consumer
// is considered inactive.
func WithConsumerActivityTTL(ttl time.Duration) ConsumerOption {
	return func(c *consumer) {
		c.activityTTL = ttl
	}
}

// WithConsumerActivityTTLFunc is similar to WithConsumerActivityTTL but accepts
// a function that returns the TTL.
func WithConsumerActivityTTLFunc(ttl func() time.Duration) ConsumerOption {
	return func(c *consumer) {
		c.activityTTL = ttl()
	}
}

// WithoutConsumerActivityTTL provides an option to disable the consumer activity metric ttl.
func WithoutConsumerActivityTTL() ConsumerOption {
	return func(c *consumer) {
		c.activityTTL = -1
	}
}

// WithFilterIncludeTypes provides an option to specify which EventTypes a consumer is interested in.
// For uninteresting events Consume is never called, and a skipped metric is incremented.
func WithFilterIncludeTypes(evts ...EventType) ConsumerOption {
	return func(c *consumer) {
		c.filterIncludeTypes = evts
	}
}

// WithEventFilter provides an option to specify which Event values a consumer is interested in.
// For uninteresting events Consume is never called, and a skipped metric is incremented.
func WithEventFilter(flt EventFilter) ConsumerOption {
	return func(c *consumer) {
		c.filterEvent = flt
	}
}

// WithRecoverFunction provides an option to specify a recovery function to be called when a consuming an event returns
// an error and potentially changing the error returned or even eliminate it by return nil. It can also be used for
// notification and/or recording of events that failed to process successfully.
func WithRecoverFunction(rfn RecoveryFunc) ConsumerOption {
	return func(c *consumer) {
		c.rfn = rfn
	}
}

var defaultRecoveryFunc = func(_ context.Context, _ *Event, _ Consumer, err error) error {
	return err
}

// NewConsumer returns a new instrumented consumer of events.
func NewConsumer(name string, fn ConsumerFunc,
	opts ...ConsumerOption,
) Consumer {
	ls := metrics.Labels(name)

	c := &consumer{
		fn:            fn,
		name:          name,
		lagAlert:      defaultLagAlert,
		activityTTL:   defaultActivityTTL,
		ageHist:       metrics.ConsumerAge.With(ls),
		lagAlertGauge: metrics.ConsumerLagAlert.With(ls),
		errorCounter:  metrics.ConsumerErrors.With(ls),
		latencyHist:   metrics.ConsumerLatency.With(ls),
	}

	for _, o := range opts {
		o(c)
	}
	c.activityKey = metrics.ConsumerActivityGauge.Register(ls, c.activityTTL)
	if c.rfn == nil {
		c.rfn = defaultRecoveryFunc
	}
	_ = c.Reset()
	return c
}

func (c *consumer) Name() string {
	return c.name
}

func (c *consumer) Consume(ctx context.Context, event *Event) error {
	t0 := time.Now()

	metrics.ConsumerActivityGauge.SetActive(c.activityKey)

	lag := t0.Sub(event.Timestamp)
	c.lagGauge.Set(lag.Seconds())
	c.ageHist.Observe(lag.Seconds())

	alert := 0.0
	if lag > c.lagAlert && c.lagAlert > 0 {
		alert = 1
	}
	c.lagAlertGauge.Set(alert)

	hasTraceData := len(event.Trace) > 0
	if hasTraceData {
		// Load any trace information into the context to allow logging with trace id and manually
		// configuring a trace from within the consumer.
		ctx = tracing.Inject(ctx, event.Trace)
	}

	// NOTE: The behaviour below
	// Try to filter out the event
	//  a) If the filtering returns an error then we wrap it and don't process the event
	//  b) if the filtering excludes the event then we update the skipped metrics and don't process it
	//  c) Only if no filtering error is returned, and filter indicates that the event should be processed, then we try to process it
	process, err := c.filter(event)
	if err != nil {
		err = asFilterErr(err)
	} else if !process {
		metrics.ConsumerSkippedEvents.WithLabelValues(c.name).Inc()
	} else {
		err = c.fn(ctx, event)
	}

	// We can in theory recover from either a filtering and a processing error here
	// In either case we will increment the error count metric if we cannot recover from the error
	if err != nil && !IsExpected(err) {
		err = c.tryErrorRecovery(ctx, event, err)
	}

	latency := time.Since(t0)
	c.latencyHist.Observe(latency.Seconds())

	return err
}

func (c *consumer) tryErrorRecovery(ctx context.Context, event *Event, err error) error {
	err = c.rfn(ctx, event, c, err)
	if err != nil && !IsExpected(err) {
		c.errorCounter.Inc()
	}
	return err
}

// filter returns true if the event should be processed and false if it shouldn't and if
// the filtering fails it will return a non nil error as well
func (c *consumer) filter(event *Event) (bool, error) {
	ok := len(c.filterIncludeTypes) == 0 || IsAnyType(event.Type, c.filterIncludeTypes...)
	if !ok {
		return false, nil
	}
	if c.filterEvent != nil {
		return c.filterEvent(event)
	}
	return true, nil
}

// Reset the consumer, create metrics ready for Consume
func (c *consumer) Reset() error {
	c.lagGauge = metrics.ConsumerLag.With(metrics.Labels(c.name))
	return nil
}

// Stop the consumer, discard metrics
func (c *consumer) Stop() error {
	metrics.ConsumerLag.Delete(metrics.Labels(c.name))
	return nil
}
