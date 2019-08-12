package rpatterns_test

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/stretchr/testify/assert"
)

type testEventType int

func (t testEventType) ReflexType() int {
	return int(t)
}

func TestAwait(t *testing.T) {
	tests := []struct {
		name   string
		s      *streamer
		p      *poller
		find   int
		cancel bool
		outErr error
	}{
		{
			name: "event 6",
			s: &streamer{
				events: []int{1, 2, 3, 4, 5, 6},
				err:    context.DeadlineExceeded,
			},
			p: &poller{
				results: []bool{false},
				err:     sql.ErrNoRows,
			},
			find: 6,
		}, {
			name: "event 3",
			s: &streamer{
				events: []int{1, 2, 3, 4, 5, 6},
				err:    context.DeadlineExceeded,
			},
			p: &poller{
				results: []bool{false},
				err:     sql.ErrNoRows,
			},
			find: 3,
		}, {
			name: "event 7",
			s: &streamer{
				events: []int{1, 2, 3, 4, 5, 6},
				err:    context.DeadlineExceeded,
			},
			p: &poller{
				results: []bool{false},
				err:     sql.ErrNoRows,
			},
			find:   7,
			outErr: context.DeadlineExceeded,
		}, {
			name: "poll true",
			s: &streamer{
				block: true,
				err:   context.DeadlineExceeded,
			},
			p: &poller{
				results: []bool{true},
				err:     sql.ErrNoRows,
			},
			outErr: nil,
		}, {
			name: "poll error",
			s: &streamer{
				block: true,
				err:   context.DeadlineExceeded,
			},
			p: &poller{
				err: sql.ErrNoRows,
			},
			outErr: sql.ErrNoRows,
		}, {
			name:   "cancel context",
			s:      &streamer{},
			p:      &poller{},
			cancel: true,
			outErr: context.Canceled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if test.cancel {
				cancel()
			} else {
				defer cancel()
			}
			err := rpatterns.Await(ctx, test.s.Stream, test.p.poll,
				strconv.Itoa(test.find), testEventType(test.find))
			assert.Equal(t, test.outErr, err)
		})
	}
}

type poller struct {
	results []bool
	i       int
	err     error
}

func (p *poller) poll() (bool, error) {
	if len(p.results) <= p.i {
		return false, p.err
	}
	b := p.results[p.i]
	p.i++
	return b, nil
}

type streamer struct {
	block  bool
	events []int
	i      int
	err    error
}

func (s *streamer) Recv() (*reflex.Event, error) {
	if s.block {
		time.Sleep(time.Hour)
	}
	if len(s.events) <= s.i {
		return nil, s.err
	}
	ei := s.events[s.i]
	e := &reflex.Event{
		ID:        strconv.Itoa(ei),
		Type:      testEventType(ei),
		ForeignID: strconv.Itoa(ei),
	}
	s.i++
	return e, nil
}

func (s *streamer) Stream(ctx context.Context, after string,
	options ...reflex.StreamOption) (reflex.StreamClient, error) {
	return s, nil
}

func (s *streamer) Stop() {
	panic("implement me")
}
