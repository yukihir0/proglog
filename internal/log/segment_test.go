package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/yukihir0/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	// 期待値
	want := &api.Record{Value: []byte("hello world")}

	// インデックスの検証の条件を設定する
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// セグメントを作成する
	// オフセットが初期値、セグメントが最大になっていないことを検証する
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		// レコードを追加する
		// オフセットが更新されることを検証する￥
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		// レコードを読み込む
		// レコードが期待値と一致することを検証する
		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// セグメント(インデックス)が最大になっているので、レコードを追加できないことを検証する
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// セグメント(インデックス)が最大になっていることを検証する
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Close())

	// ストアの検証の条件を設定する
	p, _ := proto.Marshal(want)
	c.Segment.MaxStoreBytes = uint64(len(p)+lenWidth) * 4
	c.Segment.MaxIndexBytes = 1024

	// 既存のセグメントを再構築する
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())
	require.NoError(t, s.Remove())

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
	require.NoError(t, s.Close())
}
