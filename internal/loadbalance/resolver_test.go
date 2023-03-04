package loadbalance_test

import (
	"net"
	"testing"

	api "github.com/yukihir0/proglog/api/v1"

	"github.com/stretchr/testify/require"
	"github.com/yukihir0/proglog/internal/config"
	"github.com/yukihir0/proglog/internal/loadbalance"
	"github.com/yukihir0/proglog/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestResolver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// サーバのTLS設定
	tlsConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			Server:        true,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	// GetServersのモックをサーバへ設定する
	serverCreds := credentials.NewTLS(tlsConfig)
	srv, err := server.NewGRPCServer(
		&server.Config{
			GetServer: &getServers{},
		},
		grpc.Creds(serverCreds),
	)
	require.NoError(t, err)

	// サーバを起動する
	go srv.Serve(l)

	// クライアントコネクション
	conn := &clientConn{}

	// クライアントのTLS設定
	tlsConfig, err = config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}

	// リゾルバを生成する
	r := &loadbalance.Resolver{}
	_, err = r.Build(
		resolver.Target{
			Endpoint: l.Addr().String(),
		},
		conn,
		opts,
	)
	require.NoError(t, err)

	// 期待値
	wantState := resolver.State{
		Addresses: []resolver.Address{
			{Addr: "localhost:9001", Attributes: attributes.New("is_leader", true)},
			{Addr: "localhost:9002", Attributes: attributes.New("is_leader", false)},
		},
	}
	require.Equal(t, wantState, conn.state)

	// リゾルバが期待したサーバとデータでクライアントコネクションを更新したことを検証する
	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

// GetServersメソッドを実装するモック
type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:9001",
			IsLeader: true,
		},
		{
			Id:      "follower",
			RpcAddr: "localhost:9002",
		},
	}, nil
}

// resolver.ClientConnインタフェースを実装するモック
type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

// リゾルバが更新したクライアントコネクションの状態を保持する
func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewAddress(addrs []resolver.Address) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return nil
}
