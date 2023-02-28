package log

import (
	"context"
	"sync"

	api "github.com/yukihir0/proglog/api/v1"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 遅延初期化
	r.init()

	// 既にクローズされている場合はスキップする
	if r.closed {
		return nil
	}

	// 既にレプリケーションを行っている場合はスキップする
	if _, ok := r.servers[name]; ok {
		return nil
	}

	// 参加したサーバ向けにチャンネルを作成する
	r.servers[name] = make(chan struct{})

	// レプリケーションを実行する
	go r.replicate(addr, r.servers[name])

	return nil
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 遅延初期化
	r.init()

	// 既に離脱している場合はスキップする
	if _, ok := r.servers[name]; !ok {
		return nil
	}

	// 離脱したサーバのチャンネルをクローズする
	close(r.servers[name])
	delete(r.servers, name)

	return nil
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 遅延初期化
	r.init()

	// 既にクローズされている場合はスキップする
	if r.closed {
		return nil
	}

	// レプリケータをクローズする
	r.closed = true
	close(r.close)

	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}

	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}

	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{Offset: 0},
	)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	// すべてのレコードを取得する
	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	// ローカルサーバにレコードをレプリケーションする
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(
				ctx,
				&api.ProduceRequest{Record: record},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
