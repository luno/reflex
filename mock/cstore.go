// Package mock contains mock reflex implementations for testing.
package mock

import (
	"context"

	"github.com/luno/reflex"
)

type MockCStore struct {
	flushed int
	cursors map[string]string
}

func (m *MockCStore) GetCursor(ctx context.Context, consumerName string) (string, error) {
	if m.cursors == nil {
		m.cursors = make(map[string]string)
	}
	return m.cursors[consumerName], nil
}

func (m *MockCStore) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	if m.cursors == nil {

	}
	m.cursors[consumerName] = cursor
	return nil
}

func (m *MockCStore) Flush(ctx context.Context) error {
	m.flushed++
	return nil
}

func (m *MockCStore) GetFlushCount() int {
	return m.flushed
}

func NewMockCStore() *MockCStore {
	return &MockCStore{cursors: make(map[string]string)}
}

var _ reflex.CursorStore = NewMockCStore()
