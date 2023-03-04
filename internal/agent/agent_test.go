package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/yukihir0/proglog/api/v1"
	"github.com/yukihir0/proglog/internal/agent"
	"github.com/yukihir0/proglog/internal/config"
	"github.com/yukihir0/proglog/internal/loadbalance"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {
	// サーバのTLS設定
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			Server:        true,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	// クライアントのTLS設定
	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)

	// 3つのエージェントを設定する(リーダー:1台、フォロワー:2台)
	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		// 利用可能なポートを見つける
		ports := dynaport.Get(2)

		// Membership(serf)で利用するポート
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])

		// gRPC/Raftで利用するポート
		rpcPort := ports[1]

		// ファイルを保存するディレクトリ
		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		// フォロワーの場合、リーダーのアドレスをクラスタの初期アドレスに追加する
		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}

		// エージェントを設定する
		agent, err := agent.New(
			agent.Config{
				NodeName:        fmt.Sprintf("%d", i),
				StartJoinAddrs:  startJoinAddrs,
				BindAddr:        bindAddr,
				RPCPort:         rpcPort,
				DataDir:         dataDir,
				ACLModelFile:    config.ACLModelFile,
				ACLPolicyFile:   config.ACLPolicyFile,
				ServerTLSConfig: serverTLSConfig,
				PeerTLSConfig:   peerTLSConfig,
				Bootstrap:       i == 0,
			},
		)
		require.NoError(t, err)

		agents = append(agents, agent)
	}

	// テスト終了時にエージェントをシャットダウンする
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()

	time.Sleep(3 * time.Second)

	// リーダーにログを書き込む
	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("foo"),
			},
		},
	)
	require.NoError(t, err)

	// リーダーに書き込んだログがフォロワーにレプリケーションされるまで待つ
	time.Sleep(3 * time.Second)

	// リーダーでログを読み込む検証
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// フォロワー1でログを読み込む検証
	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// フォロワー2でログを読み込む検証
	followerClient = client(t, agents[2], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("foo"))

	// リーダーで不要なログがレプリケーションされていないことを検証
	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1,
		},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

// gRPCクライアントを生成するヘルパー
func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	// TLSを設定する
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}

	// gRPCサーバのアドレスを取得する
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	// gRPCサーバへ接続する
	// 独自のリゾルバとピッカーを利用するようにスキームを指定する
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", loadbalance.Name, rpcAddr),
		opts...,
	)
	require.NoError(t, err)

	// gRPCクライアントを生成する
	client := api.NewLogClient(conn)

	return client
}
