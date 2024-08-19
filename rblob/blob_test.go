package rblob_test

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"

	"github.com/luno/reflex/rblob"
)

type TestDTO struct {
	ID    int64  `json:"id"`
	Field string `json:"field"`
}

func TestStreamAll(t *testing.T) {
	tests := []struct {
		Name     string
		Path     string
		Prefix   string
		After    string
		Expect   int
		IDOffset int
	}{
		{
			Name:   "all",
			Path:   "",
			Expect: 7,
		},
		{
			Name:   "2019",
			Path:   "2019",
			Expect: 3,
		},
		{
			Name:     "2020",
			Path:     "2020",
			Expect:   4,
			IDOffset: 3,
		},
		{
			Name:     "all after 2019",
			Path:     "",
			After:    "2019/12/31/Test-2019-12-31-17-56-01-1to3|eof",
			Expect:   4,
			IDOffset: 3,
		},
		{
			Name:     "all after mid jan 1",
			Path:     "",
			After:    "2020/01/01/Test-2020-01-01-05-15-56-4to6|0",
			Expect:   3,
			IDOffset: 4,
		},
		{
			Name:     "prefix with slash",
			Path:     "2020",
			Prefix:   "01/",
			Expect:   3,
			IDOffset: 3,
		},
		{
			Name:   "prefix without slash",
			Path:   "2021",
			Prefix: "Test-2021-01-01",
			Expect: 3,
		},
		{
			Name:     "prefix without slash and offset",
			Path:     "2021",
			Prefix:   "Test-2021-01-02",
			Expect:   2,
			IDOffset: 3,
		},
	}

	dir, err := os.Getwd()
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			url := "file:///" + path.Join(dir, "testdata", test.Path)
			if test.Prefix != "" {
				url += "?prefix=" + test.Prefix
			}

			bucket, err := rblob.OpenBucket(context.Background(), "", url)
			require.NoError(t, err)
			defer bucket.Close()

			sc, err := bucket.Stream(context.Background(), test.After)
			require.NoError(t, err)

			for i := 0; i < test.Expect; i++ {
				e, err := sc.Recv()
				jtest.Require(t, nil, err)
				require.Equal(t, 0, e.Type.ReflexType())
				require.Equal(t, "", e.ForeignID)

				var dto TestDTO
				err = json.Unmarshal(e.MetaData, &dto)
				require.NoError(t, err)
				require.Equal(t, int64(test.IDOffset+i+1), dto.ID)
			}
		})
	}
}

func TestWaitForMore(t *testing.T) {
	dir, err := os.Getwd()
	require.NoError(t, err)

	url := "file:///" + path.Join(dir, "testdata")

	s, err := rblob.OpenBucket(context.Background(), "", url,
		rblob.WithBackoff(time.Millisecond))
	require.NoError(t, err)
	defer s.Close()

	sc, err := s.Stream(context.Background(), "")
	require.NoError(t, err)

	for i := 0; i < 7; i++ {
		_, err := sc.Recv()
		jtest.Require(t, nil, err)
	}

	newfile := "testdata/2020/02/10/Test-2020-02-10-23-59-59-9999"

	exp := TestDTO{ID: 9999}
	data, err := json.Marshal(exp)
	require.NoError(t, err)

	err = os.WriteFile(newfile, data, 0o644)
	require.NoError(t, err)
	defer os.RemoveAll(newfile)

	e, err := sc.Recv()
	jtest.Require(t, nil, err)

	var res TestDTO
	err = json.Unmarshal(e.MetaData, &res)
	require.NoError(t, err)
	require.Equal(t, exp, res)
}

func TestCancelWait(t *testing.T) {
	dir, err := os.Getwd()
	require.NoError(t, err)

	url := "file:///" + path.Join(dir, "testdata")

	// Wait forever if no files
	s, err := rblob.OpenBucket(context.Background(), "", url,
		rblob.WithBackoff(time.Hour))
	require.NoError(t, err)
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	sc, err := s.Stream(ctx, "2099|eof")
	require.NoError(t, err)

	_, err = sc.Recv()
	jtest.Require(t, context.DeadlineExceeded, err)
}
