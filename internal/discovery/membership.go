package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// メンバーシップの設定
type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// Join/Leaveイベントを処理するハンドラ
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// メンバーシップ
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// メンバーシップのコンストラクタ
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}

	// Serfを設定する
	if err := c.setupSerf(); err != nil {
		return nil, err
	}

	return c, nil
}

// Serfを設定するメソッド
func (m *Membership) setupSerf() (err error) {
	// Serfで利用するアドレス
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	// Serfの設定
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.NodeName

	// Serfを生成する
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	// イベントハンドラを起動する
	go m.eventHandler()

	// Join先がある場合
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// イベントをハンドルするメソッド
func (m *Membership) eventHandler() {
	// 複数のイベントが同時に発生する可能性がある
	for e := range m.events {
		switch e.EventType() {
		// Joinイベントの場合
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}

				m.handleJoin(member)
			}
		// Leaveイベントの場合
		case serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}

				m.handleLeave(member)
			}
		}
	}
}

// Joinイベントをハンドルするメソッド
func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		// JoinしたサーバのgRPC/Raftのアドレスを共有する
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

// Leaveイベントをハンドルするメソッド
func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// 自分自身かを判定するメソッド
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// 現時点のメンバー一覧を取得するメソッド
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leaveするメソッド
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// エラーを出力するメソッド
func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error

	// リーダーではなくフォロワーに変更を加えようとした場合のエラーはデバッグ出力にする
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}

	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
