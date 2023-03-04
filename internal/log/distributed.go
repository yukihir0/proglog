package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github.com/yukihir0/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// 分散ログ
type DistributedLog struct {
	config  Config
	log     *Log
	raftLog *logStore
	raft    *raft.Raft
}

// 分散ログのコンストラクタ
func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}

	// ログを設定する
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	// Raftを設定する
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

// ログを設定するメソッド
func (l *DistributedLog) setupLog(dataDir string) error {
	// ログを保存s塗るディレクトリを作成する
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// ログを作成する
	var err error
	l.log, err = NewLog(logDir, l.config)

	return err
}

// Raftを設定するメソッド
func (l *DistributedLog) setupRaft(dataDir string) error {
	var err error

	// 有限状態機械(FSM)を作成する
	fsm := &fsm{log: l.log}

	// Raftログを保存するディレクトリを作成する
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// Raftログストアを作成する
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	l.raftLog, err = newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// 安定ストアを作成する
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	// スナップショットストアを作成する
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	// トランスポート(ストリームレイヤ)を作成する
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	// Raftの設定
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID

	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}

	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}

	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}

	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	// Raftを作成する
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		l.raftLog,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	// すでに状態を持っていた場合
	hasState, err := raft.HasExistingState(
		l.raftLog,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}

	// 初回起動の場合
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}

// 公開API: ログを書き込むメソッド
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	// Raft経由でログを書き込む(Raftログの書き込み、コミット)
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

// Raft経由でログを適用するメソッド
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	// リクエストタイプをバッファに書き込む
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	// リクエスト(ログ)をマーシャルする
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	// リクエスト(ログ)をバッファに書き込む
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	// Raft経由でログを適用する
	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}

	// レスポンスを取得する
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

// 公開API: ログを読み込むメソッド
// Raft経由ではなく、直接読み込んでいるので、緩やかな一貫性になる
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// サービスディスカバリでクラスタにJoinした時の処理を実行するハンドラ
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// サーバはすでに参加している
				return nil
			}

			// 既存のサーバを取り除く
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	// 投票者としてクラスタへ参加する
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

// サービスディスカバリでクラスタからLeaveした時の処理を実行するハンドラ
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// クラスタがリーダを選出するかタイム・アウトするまで待つメソッド
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			// リーダーが選出された場合
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

// 分散ログをクローズするメソッド
func (l *DistributedLog) Close() error {
	// Raftをシャットダウンする
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	// Raftログをクローズする
	if err := l.raftLog.Log.Close(); err != nil {
		return err
	}

	// ログをクローズする
	return l.log.Close()
}

// Raftサーバ一覧を取得するメソッド
func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	// Raftの設定を取得する
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	// サーバ一覧を取得する
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(
			servers,
			&api.Server{
				Id:       string(server.ID),
				RpcAddr:  string(server.Address),
				IsLeader: l.raft.Leader() == server.Address,
			},
		)
	}

	return servers, nil
}

var _ raft.FSM = (*fsm)(nil)

// 有限状態機械(FSM)
// ストレージへログを書き込むなど、ビジネスロジックを実装する
type fsm struct {
	log *Log
}

// リクエストタイプ
type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Raftログのコミットを処理するメソッド
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data

	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}

	return nil
}

// ログの書き込みをするメソッド
func (l *fsm) applyAppend(b []byte) interface{} {
	// ログをアンマーシャルする
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}

	// ストレージへログを書き込む
	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{Offset: offset}
}

// スナップショットを取得するメソッド
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

// スナップショットをリストアするメソッド
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)

	var buf bytes.Buffer
	for i := 0; ; i++ {
		// ログのサイズを取得する
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// ログを取得する
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}

		// ログをアンマーシャルする
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		// 最初のレコードの場合
		if i == 0 {
			// セグメントの初期オフセット(ベースオフセット)を設定する
			f.log.Config.Segment.InitialOffset = record.Offset

			// ログをリセットする(ログ削除、ディレクトリ再作成)
			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		// ストレージへログを書き込む
		if _, err = f.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

// スナップショット
type snapshot struct {
	reader io.Reader
}

// スナップショットを書き込むメソッド
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// スナップショットをリリースするメソッド
func (s *snapshot) Release() {}

var _ raft.LogStore = (*logStore)(nil)

// Raftログストア
type logStore struct {
	*Log
}

// Raftログストアのコンストラクタ
func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}

	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}

	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term

	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32((record.Type)),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// ストリームレイヤ
type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

// ストリームレイヤのコンストラクタ
func NewStreamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

// Raft向けの通信であることを判定するバイト
const RaftRPC = 1

// 他のRaftメンバーへのコネクションを作成するメソッド
func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	// 他のRaftメンバーへのコネクションを作成する
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// Raft向けの通信であることを判定するバイトを書き込む
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	// TLS設定
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err
}

// Raft通信を受け付けるコネクションを作成するメソッド
func (s *StreamLayer) Accept() (net.Conn, error) {
	// Raft通信を受け付けるコネクションを作成する
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	// Raft向けの通信であることを判定するバイトを読み込む
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	// Raft向けの通信でない場合
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}

	// TLS設定
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

// ストリームレイヤーをクローズするメソッド
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Raftのアドレスを取得するメソッド
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
