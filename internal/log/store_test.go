package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// テストレコード
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	// テスト用の一時ファイルを作成する
	f, err := os.CreateTemp("", "store_append_read_test")
	defer os.Remove(f.Name())
	require.NoError(t, err)

	// ストアを作成する
	s, err := newStore(f)
	require.NoError(t, err)

	// ストアの書き込みと読み込みをテストする
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// サービスが再起動後に状態が回復することをテストする
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
	testReadAt(t, s)
}
func TestStoreClose(t *testing.T) {
	// テスト用の一時ファイルを作成する
	f, err := os.CreateTemp("", "store_close_test")
	defer os.Remove(f.Name())
	require.NoError(t, err)

	// ストアを作成する
	s, err := newStore(f)
	require.NoError(t, err)

	// ストアへレコードを書き込む
	_, _, err = s.Append(write)
	require.NoError(t, err)

	// クローズ前のファルサイズを取得する
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	// ストアをクローズする
	err = s.Close()
	require.NoError(t, err)

	// クローズ後のファルサイズを取得する
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	// ファイルサイズを検証する
	require.True(t, afterSize > beforeSize)
}

// ストアの書き込みをテストするヘルパー
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

// ストアの読み込みをテストするヘルパー
func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

// 特定の位置のストアの読み込みをテストするヘルパー
func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// レコードの長さを検証する
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += int64(n)

		// レコードを検証する
		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
