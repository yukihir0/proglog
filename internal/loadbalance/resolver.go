package loadbalance

import (
	"context"
	"fmt"
	"sync"

	api "github.com/yukihir0/proglog/api/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)

// スキーム識別子
const Name = "proglog"

// resolver.Builderインタフェースの実装
// リゾルバがGetServersを呼び出せるように、クライアントコネクションを設定する
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	// ロガーの設定
	r.logger = zap.L().Named("resolver")

	// ユーザのクライアントコネクション
	r.clientConn = cc

	// grpcの接続オプション
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}

	// grpcのサービス設定
	// proglogのロードバランサを指定する
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)

	// リゾルバ自身のサーバへのクライアントコネクション
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}

// resolver.Builderインタフェースの実装
// リゾルバのスキーム識別子を返すメソッド
func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

// resolver.Resolverインタフェースの実装
// Raftサーバの一覧を取得し、クライアントコネクションを更新するメソッド
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Raftサーバの一覧を取得する
	client := api.NewLogClient(r.resolverConn)
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}

	// サーバ一覧を設定する
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(
			addrs,
			resolver.Address{
				Addr: server.RpcAddr,
				Attributes: attributes.New(
					"is_leader",
					server.IsLeader,
				),
			},
		)
	}

	// ユーザのクライアントコネクションを更新する
	r.clientConn.UpdateState(
		resolver.State{
			Addresses:     addrs,
			ServiceConfig: r.serviceConfig,
		},
	)
}

// resolver.Resolverインタフェースの実装
// リゾルバをクローズするメソッド
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("failed to close conn", zap.Error(err))
	}
}
