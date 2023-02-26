package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/yukihir0/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// ストアファイルを開く
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}

	// ストアを作成する
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// インデックスファイルを開く
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		return nil, err
	}

	// インデックスを作成する
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// セグメントのオフセットを設定する
	if off, _, err := s.index.Read(-1); err != nil {
		// インデックスにエントリがない場合
		s.nextOffset = baseOffset
	} else {
		// インデックスにエントリがある場合
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// レコードのオフセットを設定する
	cur := s.nextOffset
	record.Offset = cur

	// レコードをマーシャルする
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// ストアにレコードを追加する
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// インデックスにエントリを追加する
	if err = s.index.Write(
		// インデックスのオフセットは、ベースオフセットからの相対
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	// オフセットをインクリメントする
	s.nextOffset++

	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	// インデックスのエントリを取得する
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// ストアのレコードを取得する
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// レコードをアンマーシャルする
	record := &api.Record{}
	err = proto.Unmarshal(p, record)

	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

func (s *segment) Remove() error {
	// セグメントを閉じる
	if err := s.Close(); err != nil {
		return err
	}

	// インデックスファイルを削除する
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	// ストアファイルを削除する
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	// インデックスを閉じる
	if err := s.index.Close(); err != nil {
		return err
	}

	// ストアを閉じる
	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}
