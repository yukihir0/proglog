package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/yukihir0/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			defer os.RemoveAll(dir)
			require.NoError(t, err)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	// ログにレコードを追加する検証
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	// ログからレコードを読み込む検証
	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
	require.NoError(t, log.Close())
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	// ログに保存されているオフセットの範囲外を読み取ろうした場合にエラーになる検証
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
	require.NoError(t, log.Close())
}

func testInitExisting(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	// ログにレコードを追加する
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	// オフセットの最小値を検証する
	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	// オフセットの最大値を検証する
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// ログを再構築する
	require.NoError(t, log.Close())
	n, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	// オフセットの最小値を検証する
	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	// オフセットの最大値を検証する
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	require.NoError(t, n.Close())
}

func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	// ログにレコードを追加する
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	// リーダーからレコードを読み込む
	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	// 読み込んだレコードを検証する
	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)

	require.NoError(t, log.Close())
}

func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	// ログにレコードを書き込む
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	// 指定したオフセットまでを含むログを切り詰める
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)

	require.NoError(t, log.Close())
}
