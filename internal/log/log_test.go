package log_test

import (
	"io/ioutil"
	"os"
	"testing"

	log_v1 "github.com/jarviliam/commitlog/api/v1"
	"github.com/jarviliam/commitlog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *log.Log){
		"appends and reads a record succeds": testAppendRead,
		"offset out of range":                testOutOfRangeErr,
		"init with existing segments":        testInitExisting,
		"reader":                             testReader,
		"truncate":                           testTruncate,
	} {

		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := log.Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := log.NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *log.Log) {
	append := &log_v1.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *log.Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(log_v1.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, o *log.Log) {
	append := &log_v1.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := o.Append(append)
		require.NoError(t, err)
	}

	require.NoError(t, o.Close())

	off, err := o.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, off, uint64(0))

	off, err = o.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := log.NewLog(o.Dir, o.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *log.Log) {
	append := &log_v1.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &log_v1.Record{}
	err = proto.Unmarshal(b[8:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testTruncate(t *testing.T, log *log.Log) {

	append := &log_v1.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 2; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)

}
