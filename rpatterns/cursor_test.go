package rpatterns_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/luno/reflex/testmock"
)

func TestReadthroughCursorStore_GetCursor(t *testing.T) {
	ctx := context.Background()
	consumerName := fmt.Sprintf("consumer_%d", rand.Int())
	cursor := fmt.Sprintf("cursor_%d", rand.Int())
	testErr := fmt.Errorf("error_%d", rand.Int())

	cases := []struct {
		name string

		oldCursorStoreCalls []*mock.Call
		newCursorStoreCalls []*mock.Call

		expected string
		err      error
	}{
		{
			name: "neither store has cursor",
			oldCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return("", nil),
			},
			newCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return("", nil),
			},
		},
		{
			name: "new store has cursor",
			newCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return(cursor, nil),
			},
			expected: cursor,
		},
		{
			name: "old store has cursor",
			oldCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return(cursor, nil),
			},
			newCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return("", nil),
				new(testmock.CursorStore).On("SetCursor", ctx, consumerName, cursor).Return(nil),
			},
			expected: cursor,
		},
		{
			name: "new store returns error",
			newCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return("", testErr),
			},
			err: testErr,
		},
		{
			name: "old store returns error",
			oldCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return("", testErr),
			},
			newCursorStoreCalls: []*mock.Call{
				new(testmock.CursorStore).On("GetCursor", ctx, consumerName).Return("", nil),
			},
			err: testErr,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			oldCStore := &testmock.CursorStore{Mock: mock.Mock{ExpectedCalls: test.oldCursorStoreCalls}}
			defer oldCStore.AssertExpectations(t)
			newCStore := &testmock.CursorStore{Mock: mock.Mock{ExpectedCalls: test.newCursorStoreCalls}}
			defer newCStore.AssertExpectations(t)

			cStore := rpatterns.ReadThroughCursorStore(newCStore, oldCStore)

			actual, err := cStore.GetCursor(ctx, consumerName)
			jtest.Require(t, test.err, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestReadthroughCursorStore_Flush(t *testing.T) {
	ctx := context.Background()
	testErr := fmt.Errorf("error_%d", rand.Int())

	// Not initialised because no calls are expected
	var oldCStore reflex.CursorStore
	newCStore := new(testmock.CursorStore)
	newCStore.On("Flush", ctx).Return(testErr)
	defer newCStore.AssertExpectations(t)

	cStore := rpatterns.ReadThroughCursorStore(newCStore, oldCStore)

	err := cStore.Flush(ctx)
	jtest.Require(t, testErr, err)
}

func TestMigrateCursorStore_SetCursor(t *testing.T) {
	ctx := context.Background()
	consumerName := fmt.Sprintf("consumer_%d", rand.Int())
	cursor := fmt.Sprintf("cursor_%d", rand.Int())
	testErr := fmt.Errorf("error_%d", rand.Int())

	// Not initialised because no calls are expected
	var oldCStore reflex.CursorStore
	newCStore := new(testmock.CursorStore)
	newCStore.On("SetCursor", ctx, consumerName, cursor).Return(testErr)
	defer newCStore.AssertExpectations(t)

	cStore := rpatterns.ReadThroughCursorStore(newCStore, oldCStore)

	err := cStore.SetCursor(context.Background(), consumerName, cursor)
	jtest.Require(t, testErr, err)
}

func TestMemoryCStore(t *testing.T) {
	ctx := context.Background()
	cName0 := "initialised_cursor"
	c0 := fmt.Sprintf("cursor_%d", rand.Int())

	cs := rpatterns.MemCursorStore(rpatterns.WithMemCursor(cName0, c0))

	actual, err := cs.GetCursor(ctx, "unset_cursor")
	require.NoError(t, err)
	require.Empty(t, actual)

	actual, err = cs.GetCursor(ctx, cName0)
	require.NoError(t, err)
	require.Equal(t, c0, actual)

	cName1 := "set_cursor"
	c1 := fmt.Sprintf("cursor_%d", rand.Int())

	err = cs.SetCursor(ctx, cName1, c1)
	require.NoError(t, err)

	actual, err = cs.GetCursor(ctx, cName1)
	require.NoError(t, err)
	require.Equal(t, c1, actual)

	c2 := fmt.Sprintf("cursor_%d", rand.Int())

	err = cs.SetCursor(ctx, cName1, c2)
	require.NoError(t, err)

	actual, err = cs.GetCursor(ctx, cName1)
	require.NoError(t, err)
	require.Equal(t, c2, actual)
}
