package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/big"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	libp2pconfig "github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	routedhost "github.com/libp2p/go-libp2p/p2p/host/routed"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"source.quilibrium.com/quilibrium/monorepo/config"
	blossomsub "source.quilibrium.com/quilibrium/monorepo/go-libp2p-blossomsub"
	"source.quilibrium.com/quilibrium/monorepo/go-libp2p-blossomsub/pb"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
	"source.quilibrium.com/quilibrium/monorepo/types/p2p"
	up2p "source.quilibrium.com/quilibrium/monorepo/utils/p2p"
)

var GLOBAL_CONSENSUS_BITMASK = []byte{0x00}
var GLOBAL_FRAME_BITMASK = []byte{0x00, 0x00}
var GLOBAL_PROVER_BITMASK = []byte{0x00, 0x00, 0x00}
var GLOBAL_PEER_INFO_BITMASK = []byte{0x00, 0x00, 0x00, 0x00}
var GLOBAL_ALERT_BITMASK = bytes.Repeat([]byte{0x00}, 16)

const (
	DecayInterval = 10 * time.Minute
	AppDecay      = .9
)

// ConfigDir is a distinct type for the configuration directory path
// Used by Wire for dependency injection
type ConfigDir string

// partitionedPairKey is a canonical, order-independent key for a partitioned peer pair.
// The lexicographically smaller peer.ID is always stored in Lo.
type partitionedPairKey struct {
	Lo peer.ID
	Hi peer.ID
}

func newPartitionedPairKey(a, b peer.ID) partitionedPairKey {
	if a < b {
		return partitionedPairKey{Lo: a, Hi: b}
	}
	return partitionedPairKey{Lo: b, Hi: a}
}

// NetworkPartitioner tracks which peer pairs are partitioned from each other.
// It is safe for concurrent use. Both BlossomSubProxy and GRPCProxy share one
// instance so that a single ApplyPartition call affects all transports.
type NetworkPartitioner struct {
	mu               sync.RWMutex
	partitionedPairs map[partitionedPairKey]struct{}
}

// NewNetworkPartitioner creates a new, empty NetworkPartitioner.
func NewNetworkPartitioner() *NetworkPartitioner {
	return &NetworkPartitioner{partitionedPairs: make(map[partitionedPairKey]struct{})}
}

// ForwardFilter returns true when forwarding between from and to is allowed.
func (ns *NetworkPartitioner) ForwardFilter(from, to peer.ID) bool {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	_, blocked := ns.partitionedPairs[newPartitionedPairKey(from, to)]
	return !blocked
}

// PartitionPeers suppresses forwarding between peerA and peerB. Idempotent.
func (ns *NetworkPartitioner) PartitionPeers(peerA, peerB peer.ID) {
	ns.mu.Lock()
	ns.partitionedPairs[newPartitionedPairKey(peerA, peerB)] = struct{}{}
	ns.mu.Unlock()
}

// UnpartitionPeers removes a partition between peerA and peerB. Idempotent.
func (ns *NetworkPartitioner) UnpartitionPeers(peerA, peerB peer.ID) {
	ns.mu.Lock()
	delete(ns.partitionedPairs, newPartitionedPairKey(peerA, peerB))
	ns.mu.Unlock()
}

// ClearPartitions removes all active partitions.
func (ns *NetworkPartitioner) ClearPartitions() {
	ns.mu.Lock()
	ns.partitionedPairs = make(map[partitionedPairKey]struct{})
	ns.mu.Unlock()
}

// ApplyPartition clears existing partitions and blocks all group1 × group2 pairs.
// Each element is a base58-encoded peer ID string.
func (ns *NetworkPartitioner) ApplyPartition(group1, group2 []string) {
	ns.ClearPartitions()
	for _, p1 := range group1 {
		for _, p2 := range group2 {
			pid1, _ := peer.Decode(strings.TrimSpace(p1))
			pid2, _ := peer.Decode(strings.TrimSpace(p2))
			ns.PartitionPeers(pid1, pid2)
		}
	}
}

type appScore struct {
	expire time.Time
	score  float64
}

type BlossomSubProxy struct {
	ps            *blossomsub.PubSub
	ctx           context.Context
	cancel        context.CancelFunc
	logger        *zap.Logger
	peerID        peer.ID
	derivedPeerID peer.ID
	bitmaskMap    map[string]*blossomsub.Bitmask
	// Track which bit slices belong to which original bitmasks, used to reference
	// count bitmasks for closed subscriptions
	subscriptionTracker map[string][][]byte
	subscriptions       []*blossomsub.Subscription
	subscriptionMutex   sync.RWMutex
	h                   host.Host
	signKey             crypto.PrivKey
	peerScore           map[string]*appScore
	peerScoreMx         sync.Mutex
	manualReachability  atomic.Pointer[bool]
	p2pConfig           config.P2PConfig
	dht                 *dht.IpfsDHT
	configDir           ConfigDir
	globalFrameChan     chan<- *protobufs.GlobalFrame
	globalConsensusChan chan<- uint64
	partitioner         *NetworkPartitioner
}

var ErrNoPeersAvailable = errors.New("no peers available")

func NewBlossomSubProxy(
	ctx context.Context,
	p2pConfig *config.P2PConfig,
	engineConfig *config.EngineConfig,
	logger *zap.Logger,
	configDir ConfigDir,
	globalFrameChan chan<- *protobufs.GlobalFrame,
	globalConsensusChan chan<- uint64,
	partitioner *NetworkPartitioner,
) *BlossomSubProxy {

	logger = logger.With(zap.String("process", "master"))
	listenAddr := p2pConfig.ListenMultiaddr

	opts := []libp2pconfig.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.EnableNATService(),
		libp2p.NATPortMap(),
	}

	isBootstrapPeer := true

	peerPrivKey, err := hex.DecodeString(p2pConfig.PeerPrivKey)
	if err != nil {
		logger.Panic("error unmarshaling peerkey", zap.Error(err))
	}

	privKey, err := crypto.UnmarshalEd448PrivateKey(peerPrivKey)
	if err != nil {
		logger.Panic("error unmarshaling peerkey", zap.Error(err))
	}

	derivedPeerId, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		logger.Panic("error deriving peer id", zap.Error(err))
	}

	opts = append(opts, libp2p.Identity(privKey))

	opts = append(
		opts,
		libp2p.SwarmOpts(),
	)

	ctx, cancel := context.WithCancel(ctx)
	bs := &BlossomSubProxy{
		ctx:                 ctx,
		cancel:              cancel,
		logger:              logger,
		bitmaskMap:          make(map[string]*blossomsub.Bitmask),
		subscriptionTracker: make(map[string][][]byte),
		signKey:             privKey,
		peerScore:           make(map[string]*appScore),
		p2pConfig:           *p2pConfig,
		derivedPeerID:       derivedPeerId,
		configDir:           configDir,
		globalFrameChan:     globalFrameChan,
		globalConsensusChan: globalConsensusChan,
		partitioner:           partitioner,
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		logger.Panic("error constructing p2p", zap.Error(err))
	}
	logger.Info("established peer id", zap.String("peer_id", h.ID().String()))

	bootstrappers := make([]peer.AddrInfo, 0)

	kademliaDHT := initDHT(
		ctx,
		logger,
		h,
		isBootstrapPeer,
		bootstrappers,
		p2pConfig.Network,
	)
	h = routedhost.Wrap(h, kademliaDHT)

	var tracer *blossomsub.JSONTracer
	if p2pConfig.TraceLogStdout {
		tracer, err = blossomsub.NewStdoutJSONTracer()
		if err != nil {
			panic(errors.Wrap(err, "error building stdout tracer"))
		}
	} else if p2pConfig.TraceLogFile != "" {
		tracer, err = blossomsub.NewJSONTracer(p2pConfig.TraceLogFile)
		if err != nil {
			logger.Panic("error building file tracer", zap.Error(err))
		}
	}

	blossomOpts := []blossomsub.Option{
		blossomsub.WithStrictSignatureVerification(true),
		blossomsub.WithForwardFilter(partitioner.ForwardFilter),
	}

	if tracer != nil {
		blossomOpts = append(blossomOpts, blossomsub.WithEventTracer(tracer))
	}

	blossomOpts = append(blossomOpts,
		blossomsub.WithValidateQueueSize(p2pConfig.ValidateQueueSize),
		blossomsub.WithValidateWorkers(p2pConfig.ValidateWorkers),
		blossomsub.WithPeerOutboundQueueSize(p2pConfig.PeerOutboundQueueSize),
	)
	blossomOpts = append(blossomOpts, blossomsub.WithMessageIdFn(
		func(pmsg *pb.Message) []byte {
			id := sha256.Sum256(pmsg.Data)
			return id[:]
		}),
	)

	params := toBlossomSubParams(p2pConfig)
	rt := blossomsub.NewBlossomSubRouter(h, params, bs.p2pConfig.Network)
	blossomOpts = append(blossomOpts, rt.WithDefaultTagTracer())
	pubsub, err := blossomsub.NewBlossomSubWithRouter(ctx, h, rt, blossomOpts...)
	if err != nil {
		logger.Panic("error creating pubsub", zap.Error(err))
	}

	peerID := h.ID()
	bs.dht = kademliaDHT
	bs.ps = pubsub
	bs.peerID = peerID
	bs.h = h
	bs.signKey = privKey

	go bs.background(ctx)

	return bs
}

func (b *BlossomSubProxy) background(ctx context.Context) {
	refreshScores := time.NewTicker(DecayInterval)
	defer refreshScores.Stop()

	for {
		select {
		case <-refreshScores.C:
			b.refreshScores()
		case <-ctx.Done():
			return
		}
	}
}

func (b *BlossomSubProxy) refreshScores() {
	b.peerScoreMx.Lock()

	now := time.Now()
	for p, pstats := range b.peerScore {
		if now.After(pstats.expire) {
			delete(b.peerScore, p)
			continue
		}

		pstats.score *= AppDecay
		if math.Abs(pstats.score) < .1 {
			pstats.score = 0
		}
	}

	b.peerScoreMx.Unlock()
}

func (b *BlossomSubProxy) PublishToBitmask(bitmask []byte, data []byte) error {
	err := b.ps.Publish(
		b.ctx,
		bitmask,
		data,
		blossomsub.WithSecretKeyAndPeerId(b.signKey, b.derivedPeerID),
	)
	if err != nil && errors.Is(err, blossomsub.ErrBitmaskClosed) &&
		b.p2pConfig.Network == 99 {
		// Ignore bitmask closed errors for devnet
		return nil
	}

	return errors.Wrap(
		errors.Wrapf(err, "bitmask: %x", bitmask),
		"publish to bitmask",
	)
}

func (b *BlossomSubProxy) Publish(address []byte, data []byte) error {
	bitmask := up2p.GetBloomFilter(address, 256, 3)
	return b.PublishToBitmask(bitmask, data)
}

func (b *BlossomSubProxy) Subscribe(
	bitmask []byte,
	handler func(message *pb.Message) error,
) error {
	b.logger.Info("joining broadcast")
	bm, err := b.ps.Join(bitmask)
	if err != nil {
		b.logger.Error("join failed", zap.Error(err))
		return errors.Wrap(err, "subscribe")
	}

	b.logger.Info(
		"subscribe to bitmask",
		zap.String("bitmask", hex.EncodeToString(bitmask)),
	)

	// Track the bit slices for this subscription
	b.subscriptionMutex.Lock()
	bitSlices := make([][]byte, 0, len(bm))
	for _, bit := range bm {
		sliceCopy := make([]byte, len(bit.Bitmask()))
		copy(sliceCopy, bit.Bitmask())
		bitSlices = append(bitSlices, sliceCopy)
	}
	b.subscriptionTracker[string(bitmask)] = bitSlices
	b.subscriptionMutex.Unlock()

	// If the bitmask count is greater than three, this is a broad subscribe
	// and the caller is expected to handle disambiguation of addresses
	exact := len(bm) <= 3

	subs := []*blossomsub.Subscription{}
	for _, bit := range bm {
		sub, err := bit.Subscribe(
			blossomsub.WithBufferSize(b.p2pConfig.SubscriptionQueueSize),
		)
		if err != nil {
			b.logger.Error("subscription failed", zap.Error(err))
			// Clean up on failure
			b.subscriptionMutex.Lock()
			delete(b.subscriptionTracker, string(bitmask))
			b.subscriptionMutex.Unlock()
			return errors.Wrap(err, "subscribe")
		}
		b.subscriptionMutex.Lock()
		_, ok := b.bitmaskMap[string(bit.Bitmask())]
		if !ok {
			b.bitmaskMap[string(bit.Bitmask())] = bit
		}
		b.subscriptionMutex.Unlock()
		subs = append(subs, sub)
	}

	b.logger.Info(
		"begin streaming from bitmask",
		zap.String("bitmask", hex.EncodeToString(bitmask)),
	)

	// Track subscriptions for cleanup on Close
	b.subscriptionMutex.Lock()
	b.subscriptions = append(b.subscriptions, subs...)
	b.subscriptionMutex.Unlock()

	for _, sub := range subs {
		copiedBitmask := make([]byte, len(bitmask))
		copy(copiedBitmask[:], bitmask[:])
		sub := sub

		go func() {
			for {
				if !b.subscribeHandler(sub, copiedBitmask, exact, handler) {
					return
				}
			}
		}()
	}

	b.logger.Info(
		"successfully subscribed to bitmask",
		zap.String("bitmask", hex.EncodeToString(bitmask)),
	)

	return nil
}

// subscribeHandler processes a single message from the subscription.
// Returns true if the loop should continue, false if it should exit.
func (b *BlossomSubProxy) subscribeHandler(
	sub *blossomsub.Subscription,
	copiedBitmask []byte,
	exact bool,
	handler func(message *pb.Message) error,
) bool {
	defer func() {
		if r := recover(); r != nil {
			b.logger.Error(
				"message handler panicked, recovering",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())),
			)
		}
	}()

	m, err := sub.Next(b.ctx)
	if err != nil {
		// Context cancelled or subscription closed - exit the loop
		b.logger.Debug(
			"subscription exiting",
			zap.Error(err),
		)
		return false
	}
	if m == nil {
		// Subscription closed
		return false
	}
	if bytes.Equal(m.Bitmask, copiedBitmask) || !exact {
		if err = handler(m.Message); err != nil {
			b.logger.Debug("message handler returned error", zap.Error(err))
		}
	}
	return true
}

func (b *BlossomSubProxy) Unsubscribe(bitmask []byte, raw bool) {
	b.subscriptionMutex.Lock()
	defer b.subscriptionMutex.Unlock()

	bitmaskKey := string(bitmask)
	bitSlices, exists := b.subscriptionTracker[bitmaskKey]
	if !exists {
		b.logger.Warn(
			"attempted to unsubscribe from unknown bitmask",
			zap.String("bitmask", hex.EncodeToString(bitmask)),
		)
		return
	}

	b.logger.Info(
		"unsubscribing from bitmask",
		zap.String("bitmask", hex.EncodeToString(bitmask)),
	)

	// Check each bit slice to see if it's still needed by other subscriptions
	for _, bitSlice := range bitSlices {
		bitSliceKey := string(bitSlice)

		// Check if any other subscription is using this bit slice
		stillNeeded := false
		for otherKey, otherSlices := range b.subscriptionTracker {
			if otherKey == bitmaskKey {
				continue // Skip the subscription we're removing
			}

			for _, otherSlice := range otherSlices {
				if bytes.Equal(otherSlice, bitSlice) {
					stillNeeded = true
					break
				}
			}

			if stillNeeded {
				break
			}
		}

		// Only close the bitmask if no other subscription needs it
		if !stillNeeded {
			if bm, ok := b.bitmaskMap[bitSliceKey]; ok {
				b.logger.Debug(
					"closing bit slice",
					zap.String("bit_slice", hex.EncodeToString(bitSlice)),
				)
				bm.Close()
				delete(b.bitmaskMap, bitSliceKey)
			}
		} else {
			b.logger.Debug(
				"bit slice still needed by other subscription",
				zap.String("bit_slice", hex.EncodeToString(bitSlice)),
			)
		}
	}

	// Remove the subscription from tracker
	delete(b.subscriptionTracker, bitmaskKey)
}

func (b *BlossomSubProxy) RegisterValidator(
	bitmask []byte,
	validator func(peerID peer.ID, message *pb.Message) p2p.ValidationResult,
	sync bool,
) error {
	validatorEx := func(
		ctx context.Context, peerID peer.ID, message *blossomsub.Message,
	) blossomsub.ValidationResult {
		switch v := validator(peerID, message.Message); v {
		case p2p.ValidationResultAccept:
			return blossomsub.ValidationAccept
		case p2p.ValidationResultReject:
			return blossomsub.ValidationReject
		case p2p.ValidationResultIgnore:
			return blossomsub.ValidationIgnore
		default:
			panic("unreachable")
		}
	}
	var _ blossomsub.ValidatorEx = validatorEx
	return b.ps.RegisterBitmaskValidator(
		bitmask,
		validatorEx,
		blossomsub.WithValidatorInline(sync),
	)
}

func (b *BlossomSubProxy) UnregisterValidator(bitmask []byte) error {
	return b.ps.UnregisterBitmaskValidator(bitmask)
}

func (b *BlossomSubProxy) GetPeerID() []byte {
	return []byte(b.derivedPeerID)
}

func (b *BlossomSubProxy) GetRandomPeer(bitmask []byte) ([]byte, error) {
	peers := b.ps.ListPeers(bitmask)
	if len(peers) == 0 {
		return nil, errors.Wrap(
			ErrNoPeersAvailable,
			"get random peer",
		)
	}
	b.logger.Debug("selecting from peers", zap.Any("peer_ids", peers))
	sel, err := rand.Int(rand.Reader, big.NewInt(int64(len(peers))))
	if err != nil {
		return nil, errors.Wrap(err, "get random peer")
	}

	return []byte(peers[sel.Int64()]), nil
}

func (b *BlossomSubProxy) IsPeerConnected(peerId []byte) bool {
	peerID := peer.ID(peerId)
	connectedness := b.h.Network().Connectedness(peerID)
	return connectedness == network.Connected || connectedness == network.Limited
}

func (b *BlossomSubProxy) Reachability() *wrapperspb.BoolValue {
	if manual := b.manualReachability.Load(); manual != nil {
		return wrapperspb.Bool(*manual)
	}
	reachability := b.manualReachability.Load()
	if reachability == nil {
		return nil
	}
	return &wrapperspb.BoolValue{Value: *reachability}
}

func initDHT(
	ctx context.Context,
	logger *zap.Logger,
	h host.Host,
	isBootstrapPeer bool,
	bootstrappers []peer.AddrInfo,
	network uint8,
) *dht.IpfsDHT {
	logger.Info("establishing dht")
	var mode dht.ModeOpt
	if isBootstrapPeer || network != 0 {
		logger.Warn("BOOTSTRAP PEER")
		mode = dht.ModeServer
	} else {
		mode = dht.ModeClient
	}
	opts := []dht.Option{
		dht.Mode(mode),
		dht.BootstrapPeers(bootstrappers...),
	}
	if network != 0 {
		opts = append(opts, dht.ProtocolPrefix(protocol.ID("/testnet")))
	}
	kademliaDHT, err := dht.New(
		ctx,
		h,
		opts...,
	)
	if err != nil {
		logger.Panic("error creating dht", zap.Error(err))
	}
	if err := kademliaDHT.Bootstrap(ctx); err != nil {
		logger.Panic("error bootstrapping dht", zap.Error(err))
	}
	return kademliaDHT
}

func (b *BlossomSubProxy) Reconnect(peerId []byte) error {
	peer := peer.ID(peerId)
	info := b.h.Peerstore().PeerInfo(peer)
	b.h.ConnManager().Unprotect(info.ID, "bootstrap")
	time.Sleep(10 * time.Second)
	if err := b.h.Connect(b.ctx, info); err != nil {
		return errors.Wrap(err, "reconnect")
	}

	b.h.ConnManager().Protect(info.ID, "bootstrap")
	return nil
}

func (b *BlossomSubProxy) Bootstrap(ctx context.Context) error {
	return errors.New("bootstrap not implemented")
}

func (b *BlossomSubProxy) DiscoverPeers(ctx context.Context) error {
	return errors.New("peer discovery not implemented")
}

func (b *BlossomSubProxy) GetPeerScore(peerId []byte) int64 {
	b.peerScoreMx.Lock()
	peerScore, ok := b.peerScore[string(peerId)]
	if !ok {
		b.peerScoreMx.Unlock()
		return 0
	}
	score := peerScore.score
	b.peerScoreMx.Unlock()
	return int64(score)
}

func (b *BlossomSubProxy) SetPeerScore(peerId []byte, score int64) {
	b.peerScoreMx.Lock()
	b.peerScore[string(peerId)] = &appScore{
		score:  float64(score),
		expire: time.Now().Add(1 * time.Hour),
	}
	b.peerScoreMx.Unlock()
}

func (b *BlossomSubProxy) AddPeerScore(peerId []byte, scoreDelta int64) {
	b.peerScoreMx.Lock()
	if _, ok := b.peerScore[string(peerId)]; !ok {
		b.peerScore[string(peerId)] = &appScore{
			score:  float64(scoreDelta),
			expire: time.Now().Add(1 * time.Hour),
		}
	} else {
		b.peerScore[string(peerId)] = &appScore{
			score:  b.peerScore[string(peerId)].score + float64(scoreDelta),
			expire: time.Now().Add(1 * time.Hour),
		}
	}
	b.peerScoreMx.Unlock()
}

func (b *BlossomSubProxy) GetPeerstoreCount() int {
	return len(b.h.Peerstore().Peers())
}

func (b *BlossomSubProxy) GetNetworkInfo() *protobufs.NetworkInfoResponse {
	resp := &protobufs.NetworkInfoResponse{}
	for _, p := range b.h.Network().Peers() {
		addrs := b.h.Peerstore().Addrs(p)
		multiaddrs := []string{}
		for _, a := range addrs {
			multiaddrs = append(multiaddrs, a.String())
		}
		resp.NetworkInfo = append(resp.NetworkInfo, &protobufs.NetworkInfo{
			PeerId:     []byte(p),
			Multiaddrs: multiaddrs,
			PeerScore:  b.ps.PeerScore(p),
		})
	}
	return resp
}

func (b *BlossomSubProxy) GetNetworkPeersCount() int {
	return len(b.h.Network().Peers())
}

func (b *BlossomSubProxy) GetMultiaddrOfPeerStream(
	ctx context.Context,
	peerId []byte,
) <-chan ma.Multiaddr {
	return b.h.Peerstore().AddrStream(ctx, peer.ID(peerId))
}

func (b *BlossomSubProxy) GetMultiaddrOfPeer(peerId []byte) string {
	addrs := b.h.Peerstore().Addrs(peer.ID(peerId))
	if len(addrs) == 0 {
		return ""
	}

	return addrs[0].String()
}

func (b *BlossomSubProxy) GetPublicKey() []byte {
	pub, _ := b.signKey.GetPublic().Raw()
	return pub
}

func (b *BlossomSubProxy) SignMessage(msg []byte) ([]byte, error) {
	sig, err := b.signKey.Sign(msg)
	return sig, errors.Wrap(err, "sign message")
}

func toBlossomSubParams(
	p2pConfig *config.P2PConfig,
) blossomsub.BlossomSubParams {
	return blossomsub.BlossomSubParams{
		D:                         p2pConfig.D,
		Dlo:                       p2pConfig.DLo,
		Dhi:                       p2pConfig.DHi,
		Dscore:                    p2pConfig.DScore,
		Dout:                      p2pConfig.DOut,
		HistoryLength:             p2pConfig.HistoryLength,
		HistoryGossip:             p2pConfig.HistoryGossip,
		Dlazy:                     p2pConfig.DLazy,
		GossipFactor:              p2pConfig.GossipFactor,
		GossipRetransmission:      p2pConfig.GossipRetransmission,
		HeartbeatInitialDelay:     p2pConfig.HeartbeatInitialDelay,
		HeartbeatInterval:         p2pConfig.HeartbeatInterval,
		FanoutTTL:                 p2pConfig.FanoutTTL,
		PrunePeers:                p2pConfig.PrunePeers,
		PruneBackoff:              p2pConfig.PruneBackoff,
		UnsubscribeBackoff:        p2pConfig.UnsubscribeBackoff,
		Connectors:                p2pConfig.Connectors,
		MaxPendingConnections:     p2pConfig.MaxPendingConnections,
		ConnectionTimeout:         p2pConfig.ConnectionTimeout,
		DirectConnectTicks:        p2pConfig.DirectConnectTicks,
		DirectConnectInitialDelay: p2pConfig.DirectConnectInitialDelay,
		OpportunisticGraftTicks:   p2pConfig.OpportunisticGraftTicks,
		OpportunisticGraftPeers:   p2pConfig.OpportunisticGraftPeers,
		GraftFloodThreshold:       p2pConfig.GraftFloodThreshold,
		MaxIHaveLength:            p2pConfig.MaxIHaveLength,
		MaxIHaveMessages:          p2pConfig.MaxIHaveMessages,
		MaxIDontWantMessages:      p2pConfig.MaxIDontWantMessages,
		IWantFollowupTime:         p2pConfig.IWantFollowupTime,
		IDontWantMessageThreshold: p2pConfig.IDontWantMessageThreshold,
		IDontWantMessageTTL:       p2pConfig.IDontWantMessageTTL,
		SlowHeartbeatWarning:      0.1,
	}
}

// PartitionPeers suppresses pub/sub forwarding between peerA and peerB.
// Both peers remain connected to the proxy; only messages between them are blocked.
// The operation is idempotent. peerA and peerB are raw peer.ID bytes.
func (b *BlossomSubProxy) PartitionPeers(peerA, peerB []byte) {
	pidA, pidB := peer.ID(peerA), peer.ID(peerB)
	b.partitioner.PartitionPeers(pidA, pidB)
	b.logger.Info("partitioned peers",
		zap.String("peer_a", pidA.String()),
		zap.String("peer_b", pidB.String()),
	)
}

// UnpartitionPeers removes the partition between peerA and peerB,
// allowing their messages to flow through the proxy again. Idempotent.
func (b *BlossomSubProxy) UnpartitionPeers(peerA, peerB []byte) {
	pidA, pidB := peer.ID(peerA), peer.ID(peerB)
	b.partitioner.UnpartitionPeers(pidA, pidB)
	b.logger.Info("unpartitioned peers",
		zap.String("peer_a", pidA.String()),
		zap.String("peer_b", pidB.String()),
	)
}

// ClearPartitions removes all network partitions, allowing all peers to communicate freely.
func (b *BlossomSubProxy) ClearPartitions() {
	b.partitioner.ClearPartitions()
	b.logger.Info("cleared all network partitions")
}

// ApplyPartition replaces the current partition state: clears all existing
// partitions, then blocks forwarding between every pair in group1 x group2.
// Each element is a base58-encoded peer ID string.
func (b *BlossomSubProxy) ApplyPartition(group1, group2 []string) {
	b.partitioner.ApplyPartition(group1, group2)
	b.logger.Info("applied partition",
		zap.Int("group1_size", len(group1)),
		zap.Int("group2_size", len(group2)),
	)
}

// Close implements p2p.PubSub.
func (b *BlossomSubProxy) Close() error {
	// Cancel context to signal all subscription goroutines to exit
	if b.cancel != nil {
		b.cancel()
	}

	// Cancel all subscriptions to unblock any pending Next() calls
	b.subscriptionMutex.Lock()
	for _, sub := range b.subscriptions {
		sub.Cancel()
	}
	b.subscriptions = nil
	b.subscriptionMutex.Unlock()

	return nil
}

// extractRankFromConsensusMessage peeks at the 4-byte type prefix and decodes
// the consensus message to extract its rank number.
func extractRankFromConsensusMessage(data []byte) (uint64, bool) {
	if len(data) < 4 {
		return 0, false
	}
	typePrefix := binary.BigEndian.Uint32(data[:4])
	switch typePrefix {
	case protobufs.GlobalProposalType:
		proposal := &protobufs.GlobalProposal{}
		if err := proposal.FromCanonicalBytes(data); err != nil {
			return 0, false
		}
		if proposal.State != nil && proposal.State.Header != nil {
			return proposal.State.Header.Rank, true
		}
		return 0, false
	case protobufs.ProposalVoteType:
		vote := &protobufs.ProposalVote{}
		if err := vote.FromCanonicalBytes(data); err != nil {
			return 0, false
		}
		return vote.Rank, true
	case protobufs.TimeoutStateType:
		timeout := &protobufs.TimeoutState{}
		if err := timeout.FromCanonicalBytes(data); err != nil {
			return 0, false
		}
		if timeout.Vote != nil {
			return timeout.Vote.Rank, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func (b *BlossomSubProxy) SubscribeToAllMessages() error {
	if err := b.subscribeToGlobalConsensus(); err != nil {
		return errors.Wrap(err, "subscribe to global consensus")
	}
	if err := b.subscribeToFrameMessages(); err != nil {
		return errors.Wrap(err, "subscribe to frame messages")
	}
	if err := b.subscribeToProverMessages(); err != nil {
		return errors.Wrap(err, "subscribe to prover messages")
	}
	if err := b.subscribeToPeerInfoMessages(); err != nil {
		return errors.Wrap(err, "subscribe to peer info messages")
	}
	if err := b.subscribeToAlertMessages(); err != nil {
		return errors.Wrap(err, "subscribe to alert messages")
	}
	return nil
}

func (b *BlossomSubProxy) subscribeToGlobalConsensus() error {
	if err := b.Subscribe(
		GLOBAL_CONSENSUS_BITMASK,
		func(message *pb.Message) error {
			select {
			case <-b.ctx.Done():
				return nil
			default:
				rank, ok := extractRankFromConsensusMessage(message.Data)
				if ok {
					b.logger.Info("received global consensus message",
						zap.Uint64("rank", rank))
					select {
					case b.globalConsensusChan <- rank:
					case <-b.ctx.Done():
						return nil
					}
				} else {
					b.logger.Info("received global consensus message")
				}
				return nil
			}
		},
	); err != nil {
		return errors.Wrap(err, "subscribe to global consensus")
	}

	return nil
}

func (b *BlossomSubProxy) subscribeToFrameMessages() error {
	if err := b.Subscribe(
		GLOBAL_FRAME_BITMASK,
		func(message *pb.Message) error {
			select {
			case <-b.ctx.Done():
				return nil
			default:
				frame := &protobufs.GlobalFrame{}
				if err := frame.FromCanonicalBytes(message.Data); err != nil {
					b.logger.Error("failed to decode global frame", zap.Error(err))
					return nil
				}
				b.logger.Info(
					"received global frame message",
					zap.Uint64("frame_number", frame.Header.FrameNumber),
					zap.Uint64("rank", frame.Header.Rank),
					zap.String("identity", hex.EncodeToString([]byte(frame.Identity()))),
					zap.String("parent_selector", hex.EncodeToString(frame.Header.ParentSelector)),
					zap.String("prover", hex.EncodeToString(frame.Header.Prover)),
				)

				// Push the frame to the channel
				select {
				case b.globalFrameChan <- frame:
				case <-b.ctx.Done():
					return nil
				}

				return nil
			}
		},
	); err != nil {
		return errors.Wrap(err, "subscribe to frame messages")
	}

	return nil
}

func (b *BlossomSubProxy) subscribeToProverMessages() error {
	if err := b.Subscribe(
		GLOBAL_PROVER_BITMASK,
		func(message *pb.Message) error {
			select {
			case <-b.ctx.Done():
				return nil
			default:
				b.logger.Info("received global prover message")
				return nil
			}
		},
	); err != nil {
		return errors.Wrap(err, "subscribe to prover messages")
	}

	return nil
}

func (b *BlossomSubProxy) subscribeToPeerInfoMessages() error {
	if err := b.Subscribe(
		GLOBAL_PEER_INFO_BITMASK,
		func(message *pb.Message) error {
			select {
			case <-b.ctx.Done():
				return nil
			default:
				b.logger.Info("received peer info message")
				return nil
			}
		},
	); err != nil {
		return errors.Wrap(err, "subscribe to peer info messages")
	}

	return nil
}

func (b *BlossomSubProxy) subscribeToAlertMessages() error {
	if err := b.Subscribe(
		GLOBAL_ALERT_BITMASK,
		func(message *pb.Message) error {
			select {
			case <-b.ctx.Done():
				return nil
			default:
				b.logger.Info("received global alert message")
				return nil
			}
		},
	); err != nil {
		return errors.Wrap(err, "subscribe to alert messages")
	}

	return nil
}
