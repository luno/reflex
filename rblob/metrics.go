package rblob

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	readCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "rblob",
		Name:      "read_total",
		Help:      "Number of blobs read per bucket",
	}, []string{"bucket"})

	listSkipCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "rblob",
		Name:      "list_skip_total",
		Help: "Number of list results skipped per bucket. " +
			"This should be zero, otherwise fix makeStartAfter",
	}, []string{"bucket"})
)

func init() {
	prometheus.MustRegister(readCounter)
	prometheus.MustRegister(listSkipCounter)
}
