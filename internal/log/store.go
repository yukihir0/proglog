package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// エンコーディング
	enc = binary.BigEndian
)

const (
	// レコードの長さを格納するために使うバイト数
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// レコードを書き込むメソッド
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// レコードの長さを書き込む
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// レコードを書き込む
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// 「レコードの長さ」を記録するバイト数 + レコードのバイト数
	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// レコードを読み込むメソッド
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// バッファをディスクにフラッシュする
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// レコードの長さを読み込む
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// レコードを読み込む
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// io.ReaderAtインタフェースの実装
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// バッファをディスクにフラッシュする
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	// offオフセットからlen(p)バイトを読み込む
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// バッファをディスクにフラッシュする
	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
