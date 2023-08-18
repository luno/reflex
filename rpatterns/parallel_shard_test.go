package rpatterns

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
)

type EvType int

func (i EvType) ReflexType() int { return int(i) }

func TestParallelConsumerNames(t *testing.T) {
	testCases := []struct {
		name      string
		consumers []ConsumerShard
		expNames  []string
	}{
		{
			name:      "default formatter",
			consumers: ConsumerShards("test", 10),
			expNames: []string{
				"test_1_of_10",
				"test_2_of_10",
				"test_3_of_10",
				"test_4_of_10",
				"test_5_of_10",
				"test_6_of_10",
				"test_7_of_10",
				"test_8_of_10",
				"test_9_of_10",
				"test_10_of_10",
			},
		},
		{
			name: "custom format",
			consumers: ConsumerShards("test", 2, WithNameFormatter(func(base string, m, n int) string {
				return fmt.Sprintf("%s/%d/%d", base, m+1, n)
			})),
			expNames: []string{
				"test/1/2",
				"test/2/2",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, len(tc.consumers), len(tc.expNames), "All consumers should have one expected name")
			for idx, c := range tc.consumers {
				assert.Equal(t, tc.expNames[idx], c.Name)
			}
		})
	}
}

func TestParallelConsumerFilterOnEventID(t *testing.T) {
	consumers := ConsumerShards("test", 10)

	hits := make(map[string]int)
	for evID := 1; evID <= 1000; evID++ {
		ev := &reflex.Event{ID: strconv.Itoa(evID)}
		c := findShardForEvent(t, ev, consumers)
		hits[c.Name]++
	}
	assert.Len(t, hits, 10)

	// Assert all consumers get about equal share of events
	target := 1000 / 10
	margin := target / 10
	for _, h := range hits {
		diff := target - h
		if diff < 0 {
			diff = -diff
		}
		assert.Less(t, diff, margin)
	}
}

func TestParallelConsumerFilter(t *testing.T) {
	testCases := []struct {
		name      string
		consumers []ConsumerShard
		events    []*reflex.Event

		expEventDistribution []string
	}{
		{
			name:      "by event id",
			consumers: ConsumerShards("test", 3, WithHashOption(HashOptionEventID)),
			events: []*reflex.Event{
				{ID: "1"}, {ID: "2"}, {ID: "3"}, {ID: "4"}, {ID: "5"},
			},
			expEventDistribution: []string{
				"test_1_of_3", "test_3_of_3", "test_2_of_3", "test_1_of_3", "test_3_of_3",
			},
		},
		{
			name:      "by type",
			consumers: ConsumerShards("test", 3, WithHashOption(HashOptionEventType)),
			events: []*reflex.Event{
				{ID: "1", Type: EvType(1)},
				{ID: "2", Type: EvType(2)},
				{ID: "3", Type: EvType(3)},
				{ID: "4", Type: EvType(1)},
				{ID: "5", Type: EvType(1)},
			},
			expEventDistribution: []string{
				"test_1_of_3", "test_3_of_3", "test_2_of_3", "test_1_of_3", "test_1_of_3",
			},
		},
		{
			name:      "by foreign id",
			consumers: ConsumerShards("test", 3, WithHashOption(HashOptionEventForeignID)),
			events: []*reflex.Event{
				{ID: "1", ForeignID: "100"},
				{ID: "2", ForeignID: "200"},
				{ID: "3", ForeignID: "300"},
				{ID: "4", ForeignID: "100"},
				{ID: "5", ForeignID: "100"},
			},
			expEventDistribution: []string{
				"test_1_of_3", "test_2_of_3", "test_2_of_3", "test_1_of_3", "test_1_of_3",
			},
		},
		{
			name: "custom hash function can put all events on one consumer",
			consumers: ConsumerShards("test", 3, WithHashFn(func(event *reflex.Event) ([]byte, error) {
				return []byte("Hello, World"), nil
			})),
			events: []*reflex.Event{
				{ID: "1"}, {ID: "2"}, {ID: "3"}, {ID: "4"}, {ID: "5"},
			},
			expEventDistribution: []string{
				"test_2_of_3", "test_2_of_3", "test_2_of_3", "test_2_of_3", "test_2_of_3",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, len(tc.events), len(tc.expEventDistribution), "All events should have one expected consumer")

			for idx, ev := range tc.events {
				c := findShardForEvent(t, ev, tc.consumers)
				assert.Equal(t, tc.expEventDistribution[idx], c.Name,
					"expected consumer %s for event %v, got consumer %s", tc.expEventDistribution[idx], ev, c.Name,
				)
			}
		})
	}
}

func findShardForEvent(t *testing.T, ev *reflex.Event, cons []ConsumerShard) ConsumerShard {
	var evConsumers []ConsumerShard
	for _, c := range cons {
		want, err := c.filter(ev)
		jtest.RequireNil(t, err)
		if want {
			evConsumers = append(evConsumers, c)
		}
	}
	assert.Len(t, evConsumers, 1)
	if len(evConsumers) > 1 {
		panic(fmt.Sprintf("multiple consumers for event %v", ev))
	}
	if len(evConsumers) == 0 {
		panic(fmt.Sprintf("no consumer for event %v", ev))
	}
	return evConsumers[0]
}
