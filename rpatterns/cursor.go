package rpatterns

import (
	"context"
	"strconv"

	"github.com/luno/reflex"
)

// ReadThroughCursorStore provides a cursor store that queries the fallback
// cursor store if the cursor is not found in the primary. It always writes
// to the primary.
//
// Use cases:
//  - Migrating cursor stores: Use the new cursor store as the primary
//    and the old cursor store as the fallback. Revert to just the new
//    cursor store after the migration.
//  - Programmatic seeding of a cursor: Use a MemCursorStore with the cursor
//    seeded by WithMemCursor as the fallback and the target cursor store as the primary.
//    Revert to just the target cursor store afterwards.
func ReadThroughCursorStore(primary, fallback reflex.CursorStore) reflex.CursorStore {
	return &readThroughCursorStore{CursorStore: primary, fallback: fallback}
}

type readThroughCursorStore struct {
	reflex.CursorStore // Primary
	fallback           reflex.CursorStore
}

func (c *readThroughCursorStore) GetCursor(ctx context.Context, consumerName string,
) (string, error) {

	cursor, err := c.CursorStore.GetCursor(ctx, consumerName)
	if err != nil {
		return "", err
	}

	if cursor != "" {
		return cursor, nil
	}

	return c.fallback.GetCursor(ctx, consumerName)
}

// MemCursorStore returns an in-memory cursor store. Note that it obviously
// does not provide any persistence guarantees.
//
// Use cases:
//  - Testing
//  - Programmatic seeding of a cursor: See ReadThroughCursorStore above.
func MemCursorStore(opts ...memOpt) reflex.CursorStore {
	res := &memCursorStore{cursors: make(map[string]string)}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

type memCursorStore struct {
	cursors map[string]string
}

func (m *memCursorStore) GetCursor(_ context.Context, consumerName string) (string, error) {
	return m.cursors[consumerName], nil
}

func (m *memCursorStore) SetCursor(_ context.Context, consumerName string, cursor string) error {
	if m.cursors == nil {
		m.cursors = make(map[string]string)
	}
	m.cursors[consumerName] = cursor
	return nil
}

func (m *memCursorStore) Flush(_ context.Context) error { return nil }

type memOpt func(*memCursorStore)

// WithMemCursor returns a option that stores the cursor in the
// MemCursorStore.
func WithMemCursor(name, cursor string) memOpt {
	return func(m *memCursorStore) {
		_ = m.SetCursor(nil, name, cursor)
	}
}

// WithMemCursorInt returns a option that stores the int cursor in the
// MemCursorStore.
func WithMemCursorInt(name string, cursor int64) memOpt {
	return func(m *memCursorStore) {
		_ = m.SetCursor(nil, name, strconv.FormatInt(cursor, 10))
	}
}
