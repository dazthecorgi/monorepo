package testutil

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"source.quilibrium.com/quilibrium/monorepo/config"
	"source.quilibrium.com/quilibrium/monorepo/go-libp2p-blossomsub/pb"
	"source.quilibrium.com/quilibrium/monorepo/node/rpc"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
	// "source.quilibrium.com/quilibrium/monorepo/types/channel"
	p2ptypes "source.quilibrium.com/quilibrium/monorepo/types/p2p"
)

// MockPubSub implements the p2p.PubSub interface for testing
type MockPubSub struct {
	peerID         []byte
	subscriptions  map[string][]func(message *pb.Message) error
	validators     map[string]func(peerID peer.ID, message *pb.Message) p2ptypes.ValidationResult
	publishedData  map[string][]byte
	mu             sync.RWMutex
	validatorCalls int
	messageCount   int
	logger         *zap.Logger
}

func NewMockPubSub() *MockPubSub {
	// Generate a random peer ID for testing
	peerID := make([]byte, 32)
	rand.Read(peerID)

	return &MockPubSub{
		peerID:        peerID,
		subscriptions: make(map[string][]func(message *pb.Message) error),
		validators:    make(map[string]func(peer.ID, *pb.Message) p2ptypes.ValidationResult),
		publishedData: make(map[string][]byte),
		logger:        zap.NewNop(),
	}
}

// NewMockPubSubWithLogger creates a MockPubSub with a custom logger
func NewMockPubSubWithLogger(logger *zap.Logger) *MockPubSub {
	peerID := make([]byte, 32)
	rand.Read(peerID)

	if logger == nil {
		logger = zap.NewNop()
	}

	return &MockPubSub{
		peerID:        peerID,
		subscriptions: make(map[string][]func(message *pb.Message) error),
		validators:    make(map[string]func(peer.ID, *pb.Message) p2ptypes.ValidationResult),
		publishedData: make(map[string][]byte),
		logger:        logger,
	}
}

func (m *MockPubSub) PublishToBitmask(bitmask []byte, data []byte) error {
	m.logger.Debug("PublishToBitmask called",
		zap.String("bitmask", fmt.Sprintf("%x", bitmask)),
		zap.Int("data_length", len(data)))

	m.mu.Lock()
	m.publishedData[string(bitmask)] = data

	var handlersToCall []func(message *pb.Message) error
	if handlers, exists := m.subscriptions[string(bitmask)]; exists {
		handlersToCall = make([]func(message *pb.Message) error, len(handlers))
		copy(handlersToCall, handlers)
	}
	m.messageCount++
	msgSeqno := m.messageCount
	m.mu.Unlock()

	m.logger.Debug("Publishing to subscribers",
		zap.String("bitmask", fmt.Sprintf("%x", bitmask)),
		zap.Int("handler_count", len(handlersToCall)),
		zap.Int("seqno", msgSeqno))

	if len(handlersToCall) > 0 {
		msg := &pb.Message{
			Data:    data,
			From:    m.peerID,
			Seqno:   []byte(fmt.Sprintf("%d", msgSeqno)),
			Bitmask: bitmask,
		}
		for _, handler := range handlersToCall {
			go func(h func(message *pb.Message) error) {
				h(msg)
			}(handler)
		}
	}

	return nil
}

func (m *MockPubSub) Publish(address []byte, data []byte) error {
	return m.PublishToBitmask(address, data)
}

func (m *MockPubSub) Subscribe(bitmask []byte, handler func(message *pb.Message) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bitmaskKey := string(bitmask)
	m.logger.Debug("Subscribe called",
		zap.String("bitmask", fmt.Sprintf("%x", bitmask)),
		zap.Int("existing_handlers", len(m.subscriptions[bitmaskKey])))

	if _, exists := m.subscriptions[bitmaskKey]; !exists {
		m.subscriptions[bitmaskKey] = make([]func(message *pb.Message) error, 0)
	}
	m.subscriptions[bitmaskKey] = append(m.subscriptions[bitmaskKey], handler)

	m.logger.Debug("Subscription added",
		zap.String("bitmask", fmt.Sprintf("%x", bitmask)),
		zap.Int("total_handlers", len(m.subscriptions[bitmaskKey])))

	return nil
}

func (m *MockPubSub) Unsubscribe(bitmask []byte, raw bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.subscriptions, string(bitmask))
}

func (m *MockPubSub) RegisterValidator(
	bitmask []byte,
	validator func(peerID peer.ID, message *pb.Message) p2ptypes.ValidationResult,
	sync bool,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.validators[string(bitmask)] = validator
	return nil
}

func (m *MockPubSub) UnregisterValidator(bitmask []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.validators, string(bitmask))
	return nil
}

func (m *MockPubSub) GetPeerID() []byte {
	return m.peerID
}

func (m *MockPubSub) GetValidatorCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.validatorCalls
}

func (m *MockPubSub) GetPeerstoreCount() int                       { return 5 }
func (m *MockPubSub) GetNetworkPeersCount() int                    { return 10 }
func (m *MockPubSub) GetRandomPeer(bitmask []byte) ([]byte, error) { return m.peerID, nil }
func (m *MockPubSub) GetMultiaddrOfPeerStream(ctx context.Context, peerId []byte) <-chan multiaddr.Multiaddr {
	ch := make(chan multiaddr.Multiaddr)
	close(ch)
	return ch
}
func (m *MockPubSub) GetMultiaddrOfPeer(peerId []byte) string { return "/ip4/127.0.0.1/tcp/8080" }
func (m *MockPubSub) GetOwnMultiaddrs() []multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	return []multiaddr.Multiaddr{ma}
}
func (m *MockPubSub) StartDirectChannelListener(key []byte, purpose string, server *grpc.Server) error {
	return nil
}
func (m *MockPubSub) GetDirectChannel(ctx context.Context, peerId []byte, purpose string) (*grpc.ClientConn, error) {
	return nil, nil
}
func (m *MockPubSub) GetNetworkInfo() *protobufs.NetworkInfoResponse {
	return &protobufs.NetworkInfoResponse{}
}
func (m *MockPubSub) SignMessage(msg []byte) ([]byte, error)       { return msg, nil }
func (m *MockPubSub) GetPublicKey() []byte                         { return m.peerID }
func (m *MockPubSub) GetPeerScore(peerId []byte) int64             { return 100 }
func (m *MockPubSub) SetPeerScore(peerId []byte, score int64)      {}
func (m *MockPubSub) AddPeerScore(peerId []byte, scoreDelta int64) {}
func (m *MockPubSub) Reconnect(peerId []byte) error                { return nil }
func (m *MockPubSub) Bootstrap(ctx context.Context) error          { return nil }
func (m *MockPubSub) DiscoverPeers(ctx context.Context) error      { return nil }
func (m *MockPubSub) GetNetwork() uint                             { return 0 }
func (m *MockPubSub) IsPeerConnected(peerId []byte) bool           { return true }
func (m *MockPubSub) Reachability() *wrapperspb.BoolValue          { return wrapperspb.Bool(true) }
func (m *MockPubSub) Close() error                                 { return nil }
func (m *MockPubSub) SetShutdownContext(ctx context.Context)       {}

// GetPublishedData returns the data published to a specific bitmask
func (m *MockPubSub) GetPublishedData(bitmask []byte) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.publishedData[string(bitmask)]
}

// StartPubSubProxyServer sets up a test gRPC server with TLS at the specified address and returns the cleanup function
func StartPubSubProxyServer(pubsub p2ptypes.PubSub, p2pConfig *config.P2PConfig, addr string) (func(), error) {
	// Create TLS credentials for the test server using the provided config
	// tlsCreds, err := p2p.NewPeerAuthenticator(
	// 	zap.NewNop(),
	// 	p2pConfig,
	// 	nil,
	// 	nil,
	// 	nil,
	// 	nil,
	// 	nil,
	// 	map[string]channel.AllowedPeerPolicyType{
	// 		// TODO revert this
	// 		// "quilibrium.node.proxy.pb.PubSubProxy": channel.OnlySelfPeer,
	// 		"quilibrium.node.proxy.pb.PubSubProxy": channel.AnyPeer,
	// 	},
	// 	nil,
	// ).CreateServerTLSCredentials()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create TLS credentials: %w", err)
	// }

	// Create gRPC server with TLS
	// TODO revert this
	// server := grpc.NewServer(grpc.Creds(tlsCreds))
	server := grpc.NewServer()
	proxyServer := rpc.NewPubSubProxyServer(pubsub, zap.NewNop())
	protobufs.RegisterPubSubProxyServer(server, proxyServer)

	// Start server
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on address %s: %w", addr, err)
	}

	go func() {
		if err := server.Serve(listener); err != nil {
			// Log error but don't fail - server is shutting down
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		server.Stop()
		listener.Close()
	}

	return cleanup, nil
}
