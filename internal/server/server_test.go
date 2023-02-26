package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/yukihir0/proglog/api/v1"
	"github.com/yukihir0/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume astream succeeds":                   testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()

			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, config *Config, teardown func()) {
	t.Helper()

	// リスターを設定する
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// クライアントオプションを設定する
	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// クライアントコネクションを設定する
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	config = &Config{CommitLog: clog}
	if fn != nil {
		fn(config)
	}

	// サーバを設定する
	server, err := NewGRPCServer(config)
	require.NoError(t, err)

	// サーバを起動する
	go func() {
		server.Serve(l)
	}()

	// クライアントを設定する
	client = api.NewLogClient(cc)

	return client, config, func() {
		cc.Close()
		server.Stop()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// 期待値
	want := &api.Record{
		Value: []byte("hello world"),
	}

	// ログの書き込みを検証する
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{Record: want},
	)
	require.NoError(t, err)
	want.Offset = produce.Offset

	// ログの読み込みを検証する
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produce.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// ログのレコード
	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	// ログのストリーム書き込みを検証する
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}

	// ログのストリーム読み込みを検証する
	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{Value: record.Value, Offset: uint64(i)})
		}
	}
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// ログを書き込む
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}},
	)
	require.NoError(t, err)

	// 境界を超えてログを読み込もうとする
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produce.Offset + 1},
	)
	if consume != nil {
		t.Fatal("consume not nil")
	}

	// エラーを検証する
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}
