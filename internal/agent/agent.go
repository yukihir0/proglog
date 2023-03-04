package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"github.com/yukihir0/proglog/internal/auth"
	"github.com/yukihir0/proglog/internal/discovery"
	"github.com/yukihir0/proglog/internal/log"
	"github.com/yukihir0/proglog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// エージェントの設定
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool
}

// gRPC/Raftで利用するアドレスを返すメソッド
func (c *Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// エージェント
type Agent struct {
	Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

// エージェントのコンストラクタ
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	// エージェントをセットアップする関数
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}

	// エージェントをセットアップする
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	// エージェントを起動する
	go a.serve()

	return a, nil
}

// ロガー(zap)を設定するメソッド
func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)

	return nil
}

// マルチプレクサを設定するメソッド
func (a *Agent) setupMux() error {
	// gRPC/Raftで利用するアドレス
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	a.mux = cmux.New(ln)

	return nil
}

// コミットログを設定するメソッド
func (a *Agent) setupLog() error {
	// Raft向けの通信を処理するリスナー
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}

		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})

	// コミットログの設定
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	// 分散ログを生成する
	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}

	// リーダーが選出されるまで待つ
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}

	return err
}

// gRPCサーバを設定するメソッド
func (a *Agent) setupServer() error {
	// 認可設定
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)

	// サーバ設定
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	// TLS設定
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	// サーバを生成する
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	// gRPC向けの通信をするリスナー
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()

	return err
}

// サービスディスカバリ(serf)を設定するメソッド
func (a *Agent) setupMembership() error {
	// gRPC/Raftのアドレス
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}

	// Membership(serf)を生成する
	a.membership, err = discovery.New(
		a.log, // Join/Leaveの処理を実行するハンドラ
		discovery.Config{
			NodeName: a.Config.NodeName,
			BindAddr: a.Config.BindAddr,
			Tags: map[string]string{
				"rpc_addr": rpcAddr,
			},
			StartJoinAddrs: a.Config.StartJoinAddrs,
		},
	)

	return err
}

// エージェントをシャットダウンするメソッド
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	// シャットダウン済みの場合は何もしない
	if a.shutdown {
		return nil
	}

	// シャットダウンフラグを有効化し、チャンネルをクローズする
	a.shutdown = true
	close(a.shutdowns)

	// シャットダウンを実行する関数
	shutdown := []func() error{
		// サービスディスカバリ(serf)からLeaveする
		a.membership.Leave,
		func() error {
			// サーバをシャットダウンする
			a.server.GracefulStop()
			return nil
		},
		// ログをクローズする
		a.log.Close,
	}

	// シャットダウンを実行する
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

// エージェントを起動するメソッド
func (a *Agent) serve() error {
	// サーバを起動する
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}

	return nil
}
