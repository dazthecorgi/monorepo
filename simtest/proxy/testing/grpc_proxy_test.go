package testing

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"source.quilibrium.com/quilibrium/monorepo/config"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
	proxygrpc "source.quilibrium.com/quilibrium/monorepo/simtest/proxy/grpc"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/p2p"
)

// ── test keys (from simtest configs) ─────────────────────────────────────────

// Hex-encoded Ed448 private keys taken from simtest config files.
const (
	testKeyArchive1 = "6d5f7af9e6546ed193f2afea43afa807569770e995eb7fb813d1f6a89ef90f650dffa1161eddaf30b6794407c1253d73cdb317649a329d7cf71ed667a8ff39881cab52391e664c1b5b42e5ab841cafcf360ec0a72c4ec751e1ce88e3a367432ed7de2f9d9e6dd558aaf3c2efddb9cb5de600"
	testKeyArchive2 = "09446ac1e249611be68fe6fc7babf77e6b04f6272d91d0c5d565ed2870d939a4e0bb149fa510d65a5f94803e34e938d40b50e1a68b8d927fc8431afcc3b4bf3988279f08f7dbeb3da0627bc9c40e28bb97c4edcff75f4d7eef14fd845079bd0607e834edc807d0dd17274b46eae73be21100"
	testKeyArchive3 = "5b064e083bc058c86756ef4240bceabc356b9af058515a1bd35e20e0bc1ec08d1ec4ebae4717eb57975d1a5f64e68a1cdbbc8c2834f24dc2494abde22367a1db43cd2bde81e18959180a23ae10e33e83bdba0c4c3ea6db52dcdfde656e008d3ccc83b0badd891f34bf763e6d9ecc09349400"
)

// testIdentity holds the derived peer.ID and a PeerAuthenticator for a test key.
type testIdentity struct {
	PeerID peer.ID
	Auth   *p2p.PeerAuthenticator
}

// newTestIdentity creates a PeerAuthenticator from a hex-encoded Ed448 key and
// derives the peer.ID.
func newTestIdentity(t *testing.T, hexKey string) testIdentity {
	t.Helper()
	rawKey, err := hex.DecodeString(hexKey)
	if err != nil {
		t.Fatalf("hex decode: %v", err)
	}
	privKey, err := libp2pcrypto.UnmarshalEd448PrivateKey(rawKey)
	if err != nil {
		t.Fatalf("unmarshal ed448: %v", err)
	}
	pid, err := peer.IDFromPublicKey(privKey.GetPublic())
	if err != nil {
		t.Fatalf("peer id from public key: %v", err)
	}
	cfg := &config.P2PConfig{PeerPrivKey: hexKey}
	auth := p2p.NewPeerAuthenticator(
		zap.NewNop(), cfg,
		nil, nil, nil, nil, nil, nil, nil,
	)
	return testIdentity{PeerID: pid, Auth: auth}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// startBackendTLS starts an in-process gRPC backend server with TLS.
func startBackendTLS(t *testing.T, svc protobufs.GlobalServiceServer, serverCreds credentials.TransportCredentials) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startBackendTLS: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(serverCreds))
	protobufs.RegisterGlobalServiceServer(srv, svc)
	go srv.Serve(ln) //nolint:errcheck
	return ln.Addr().String(), func() { srv.GracefulStop() }
}

// startRawStreamingBackendTLS starts a TLS backend that handles ANY
// service/method by keeping the response stream open, sending a byte every
// 20 ms until context done.
func startRawStreamingBackendTLS(t *testing.T, serverCreds credentials.TransportCredentials) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startRawStreamingBackendTLS: %v", err)
	}
	srv := grpc.NewServer(
		grpc.Creds(serverCreds),
		//lint:ignore SA1019 ForceCodec is CallOption-only in grpc v1.72; CustomCodec is the only server-side option
		grpc.CustomCodec(passThroughCodec{}),
		grpc.UnknownServiceHandler(func(_ interface{}, stream grpc.ServerStream) error {
			// drain the incoming message
			var b []byte
			if err := stream.RecvMsg(&b); err != nil && err != io.EOF {
				return err
			}
			// keep streaming until context cancelled
			for {
				if stream.Context().Err() != nil {
					return stream.Context().Err()
				}
				if err := stream.SendMsg([]byte{0x00}); err != nil {
					return err
				}
				time.Sleep(20 * time.Millisecond)
			}
		}),
	)
	go srv.Serve(ln) //nolint:errcheck
	return ln.Addr().String(), func() { srv.GracefulStop() }
}

// passThroughCodec satisfies the deprecated grpc.Codec interface so that raw
// []byte messages pass through without proto encoding.
type passThroughCodec struct{}

func (passThroughCodec) Marshal(v interface{}) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return nil, fmt.Errorf("passThroughCodec: expected []byte, got %T", v)
}
func (passThroughCodec) Unmarshal(data []byte, v interface{}) error {
	if ptr, ok := v.(*[]byte); ok {
		*ptr = append((*ptr)[:0], data...)
		return nil
	}
	return fmt.Errorf("passThroughCodec: expected *[]byte, got %T", v)
}
func (passThroughCodec) Name() string   { return "proto" }
func (passThroughCodec) String() string { return "proto" }

// dialProxyTLS dials the proxy with TLS using callerAuth's identity, expecting
// the proxy to present backendPeerID's certificate.
func dialProxyTLS(t *testing.T, port int, callerAuth *p2p.PeerAuthenticator, backendPeerID peer.ID) (protobufs.GlobalServiceClient, func()) {
	t.Helper()
	creds, err := callerAuth.CreateClientTLSCredentials([]byte(backendPeerID))
	if err != nil {
		t.Fatalf("dialProxyTLS: create client creds: %v", err)
	}
	cc, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		t.Fatalf("dialProxyTLS: %v", err)
	}
	return protobufs.NewGlobalServiceClient(cc), func() { cc.Close() }
}

// dialProxyRawTLS opens a low-level ClientConn to the proxy port with TLS.
func dialProxyRawTLS(t *testing.T, port int, callerAuth *p2p.PeerAuthenticator, backendPeerID peer.ID) (*grpc.ClientConn, func()) {
	t.Helper()
	creds, err := callerAuth.CreateClientTLSCredentials([]byte(backendPeerID))
	if err != nil {
		t.Fatalf("dialProxyRawTLS: create client creds: %v", err)
	}
	cc, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		t.Fatalf("dialProxyRawTLS: %v", err)
	}
	return cc, func() { cc.Close() }
}

// freePort returns an OS-assigned TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// stubGlobalServer is a minimal GlobalService implementation for tests.
type stubGlobalServer struct {
	protobufs.UnimplementedGlobalServiceServer
	frameNumber uint64
}

func (s *stubGlobalServer) GetGlobalFrame(
	_ context.Context,
	_ *protobufs.GetGlobalFrameRequest,
) (*protobufs.GlobalFrameResponse, error) {
	return &protobufs.GlobalFrameResponse{
		Frame: &protobufs.GlobalFrame{
			Header: &protobufs.GlobalFrameHeader{FrameNumber: s.frameNumber},
		},
	}, nil
}

// makeBackendEntry creates a BackendEntry with TLS credentials.
// serverCreds impersonate backendID; per-caller client creds carry each
// caller's identity when forwarding to the backend.
func makeBackendEntry(t *testing.T, listenPort int, backendAddr string, backendID testIdentity, callers []testIdentity) proxygrpc.BackendEntry {
	t.Helper()
	serverCreds, err := backendID.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("makeBackendEntry: server creds: %v", err)
	}
	clientCredsPerCaller := make(map[peer.ID]credentials.TransportCredentials, len(callers))
	for _, caller := range callers {
		creds, err := caller.Auth.CreateClientTLSCredentials([]byte(backendID.PeerID))
		if err != nil {
			t.Fatalf("makeBackendEntry: client creds for caller %s: %v", caller.PeerID, err)
		}
		clientCredsPerCaller[caller.PeerID] = creds
	}
	return proxygrpc.BackendEntry{
		ListenPort:           listenPort,
		BackendAddr:          backendAddr,
		PeerID:               backendID.PeerID,
		ServerCreds:          serverCreds,
		ClientCredsPerCaller: clientCredsPerCaller,
	}
}

// newProxy is a convenience wrapper that creates and starts a GRPCProxy.
func newProxy(
	t *testing.T,
	partitioner *p2p.NetworkPartitioner,
	backends []proxygrpc.BackendEntry,
) *proxygrpc.GRPCProxy {
	t.Helper()
	proxy := proxygrpc.NewGRPCProxy(
		zap.NewNop(),
		partitioner,
		backends,
		map[peer.ID]string{},
	)
	if err := proxy.Serve(); err != nil {
		t.Fatalf("proxy.Serve: %v", err)
	}
	t.Cleanup(proxy.Close)
	return proxy
}

// ── Test 1: forwarding when not partitioned ──────────────────────────────────

func TestGRPCProxy_ForwardsWhenNotPartitioned(t *testing.T) {
	idA := newTestIdentity(t, testKeyArchive1)
	idB := newTestIdentity(t, testKeyArchive2)
	caller := newTestIdentity(t, testKeyArchive3)

	serverCredsA, err := idA.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds A: %v", err)
	}
	serverCredsB, err := idB.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds B: %v", err)
	}

	addrA, cleanA := startBackendTLS(t, &stubGlobalServer{frameNumber: 1}, serverCredsA)
	defer cleanA()
	addrB, cleanB := startBackendTLS(t, &stubGlobalServer{frameNumber: 2}, serverCredsB)
	defer cleanB()

	portA, portB := freePort(t), freePort(t)
	callers := []testIdentity{caller}

	newProxy(t, p2p.NewNetworkPartitioner(), []proxygrpc.BackendEntry{
		makeBackendEntry(t, portA, addrA, idA, callers),
		makeBackendEntry(t, portB, addrB, idB, callers),
	})

	clientA, cleanCA := dialProxyTLS(t, portA, caller.Auth, idA.PeerID)
	defer cleanCA()
	respA, err := clientA.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if err != nil {
		t.Fatalf("proxy→peerA: %v", err)
	}
	if respA.Frame.Header.FrameNumber != 1 {
		t.Errorf("expected frame 1, got %d", respA.Frame.Header.FrameNumber)
	}

	clientB, cleanCB := dialProxyTLS(t, portB, caller.Auth, idB.PeerID)
	defer cleanCB()
	respB, err := clientB.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if err != nil {
		t.Fatalf("proxy→peerB: %v", err)
	}
	if respB.Frame.Header.FrameNumber != 2 {
		t.Errorf("expected frame 2, got %d", respB.Frame.Header.FrameNumber)
	}
}

// ── Test 2: blocking when partitioned ────────────────────────────────────────

func TestGRPCProxy_BlocksWhenPartitioned(t *testing.T) {
	idB := newTestIdentity(t, testKeyArchive1)
	idC := newTestIdentity(t, testKeyArchive2)
	caller := newTestIdentity(t, testKeyArchive3)

	serverCredsB, err := idB.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds B: %v", err)
	}
	serverCredsC, err := idC.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds C: %v", err)
	}

	addrB, cleanB := startBackendTLS(t, &stubGlobalServer{}, serverCredsB)
	defer cleanB()
	addrC, cleanC := startBackendTLS(t, &stubGlobalServer{}, serverCredsC)
	defer cleanC()

	portB, portC := freePort(t), freePort(t)
	partitioner := p2p.NewNetworkPartitioner()
	partitioner.PartitionPeers(caller.PeerID, idB.PeerID)
	callers := []testIdentity{caller}

	newProxy(t, partitioner, []proxygrpc.BackendEntry{
		makeBackendEntry(t, portB, addrB, idB, callers),
		makeBackendEntry(t, portC, addrC, idC, callers),
	})

	// cross-partition → Unavailable
	clientB, cleanCB := dialProxyTLS(t, portB, caller.Auth, idB.PeerID)
	defer cleanCB()
	_, err = clientB.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable for partitioned pair, got %v", err)
	}

	// intra-partition → success
	clientC, cleanCC := dialProxyTLS(t, portC, caller.Auth, idC.PeerID)
	defer cleanCC()
	if _, err := clientC.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Errorf("expected success for non-partitioned pair, got %v", err)
	}
}

// ── Test 3: dynamic partition change ─────────────────────────────────────────

func TestGRPCProxy_DynamicPartitionChange(t *testing.T) {
	idB := newTestIdentity(t, testKeyArchive1)
	caller := newTestIdentity(t, testKeyArchive2)

	serverCredsB, err := idB.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds B: %v", err)
	}

	addrB, cleanB := startBackendTLS(t, &stubGlobalServer{}, serverCredsB)
	defer cleanB()

	portB := freePort(t)
	partitioner := p2p.NewNetworkPartitioner()
	newProxy(t, partitioner, []proxygrpc.BackendEntry{
		makeBackendEntry(t, portB, addrB, idB, []testIdentity{caller}),
	})

	client, cleanC := dialProxyTLS(t, portB, caller.Auth, idB.PeerID)
	defer cleanC()

	// no partition → success
	if _, err := client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Fatalf("expected success (no partition): %v", err)
	}

	// add partition → Unavailable
	partitioner.PartitionPeers(caller.PeerID, idB.PeerID)
	_, err = client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable after partition, got %v", err)
	}

	// clear partition → success
	partitioner.ClearPartitions()
	if _, err := client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Fatalf("expected success after clearing partition: %v", err)
	}
}

// ── Test 4: caller not registered in connMap ──────────────────────────────────

func TestGRPCProxy_UnknownCallerRejected(t *testing.T) {
	idB := newTestIdentity(t, testKeyArchive1)
	registeredCaller := newTestIdentity(t, testKeyArchive2)
	unknownCaller := newTestIdentity(t, testKeyArchive3)

	serverCredsB, err := idB.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds B: %v", err)
	}

	addrB, cleanB := startBackendTLS(t, &stubGlobalServer{}, serverCredsB)
	defer cleanB()

	portB := freePort(t)
	// Only registeredCaller is in ClientCredsPerCaller; unknownCaller is not.
	be := makeBackendEntry(t, portB, addrB, idB, []testIdentity{registeredCaller})

	grpcProxy := proxygrpc.NewGRPCProxy(
		zap.NewNop(),
		p2p.NewNetworkPartitioner(),
		[]proxygrpc.BackendEntry{be},
		map[peer.ID]string{registeredCaller.PeerID: "archive-registered"},
	)
	if err := grpcProxy.Serve(); err != nil {
		t.Fatalf("proxy.Serve: %v", err)
	}
	t.Cleanup(grpcProxy.Close)

	// unknownCaller connects — peer ID is not in connMap → Unauthenticated.
	client, cleanC := dialProxyTLS(t, portB, unknownCaller.Auth, idB.PeerID)
	defer cleanC()
	_, err = client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for unknown caller, got %v", err)
	}
}

// ── Test 5: multiple backends with mixed partition sets ───────────────────────

func TestGRPCProxy_MultipleBackends(t *testing.T) {
	idB := newTestIdentity(t, testKeyArchive1)
	idC := newTestIdentity(t, testKeyArchive2)
	caller := newTestIdentity(t, testKeyArchive3) // set-1

	serverCredsB, err := idB.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds B: %v", err)
	}
	serverCredsC, err := idC.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds C: %v", err)
	}

	addrB, cleanB := startBackendTLS(t, &stubGlobalServer{}, serverCredsB)
	defer cleanB()
	addrC, cleanC := startBackendTLS(t, &stubGlobalServer{}, serverCredsC)
	defer cleanC()

	portB, portC := freePort(t), freePort(t)
	partitioner := p2p.NewNetworkPartitioner()
	partitioner.PartitionPeers(caller.PeerID, idB.PeerID) // caller ↔ B blocked
	callers := []testIdentity{caller}

	newProxy(t, partitioner, []proxygrpc.BackendEntry{
		makeBackendEntry(t, portB, addrB, idB, callers),
		makeBackendEntry(t, portC, addrC, idC, callers),
	})

	clientB, cleanCB := dialProxyTLS(t, portB, caller.Auth, idB.PeerID)
	defer cleanCB()
	_, err = clientB.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable for cross-partition call, got %v", err)
	}

	clientC, cleanCC := dialProxyTLS(t, portC, caller.Auth, idC.PeerID)
	defer cleanCC()
	if _, err := clientC.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Errorf("expected success for intra-partition call, got %v", err)
	}
}

// ── Test 6: active stream terminated on partition ────────────────────────────

func TestGRPCProxy_ActiveStreamTerminatedOnPartition(t *testing.T) {
	idB := newTestIdentity(t, testKeyArchive1)
	caller := newTestIdentity(t, testKeyArchive2)

	serverCredsB, err := idB.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds B: %v", err)
	}

	backendAddr, cleanBackend := startRawStreamingBackendTLS(t, serverCredsB)
	defer cleanBackend()

	proxyPort := freePort(t)
	partitioner := p2p.NewNetworkPartitioner()

	be := makeBackendEntry(t, proxyPort, backendAddr, idB, []testIdentity{caller})

	grpcProxy := proxygrpc.NewGRPCProxy(
		zap.NewNop(),
		partitioner,
		[]proxygrpc.BackendEntry{be},
		map[peer.ID]string{caller.PeerID: "archive-test"},
	)
	if err := grpcProxy.Serve(); err != nil {
		t.Fatalf("proxy.Serve: %v", err)
	}
	defer grpcProxy.Close()

	// Open a raw streaming call through the proxy.
	cc, cleanCC := dialProxyRawTLS(t, proxyPort, caller.Auth, idB.PeerID)
	defer cleanCC()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//nolint:staticcheck
	stream, err := cc.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/test.EchoStream/Stream",
		grpc.ForceCodec(passThroughCodec{}),
	)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := stream.SendMsg([]byte{0x01}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Confirm stream is alive — drain at least one message.
	var msg []byte
	if err := stream.RecvMsg(&msg); err != nil {
		t.Fatalf("RecvMsg (want alive stream): %v", err)
	}

	// Now apply the partition while the stream is open.
	partitioner.PartitionPeers(caller.PeerID, idB.PeerID)

	// The proxy's monitor polls every 50 ms; allow up to 500 ms for termination.
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		var buf []byte
		recvErr := stream.RecvMsg(&buf)
		if recvErr != nil {
			st, ok := status.FromError(recvErr)
			if ok && st.Code() == codes.Unavailable {
				return // success: stream terminated with correct code
			}
			t.Fatalf("stream ended with unexpected error: %v", recvErr)
		}
		if time.Now().After(deadline) {
			t.Error("stream was not terminated within 500 ms after partition was applied")
			return
		}
	}
}

// ── Test 7: nil/empty credentials rejected ───────────────────────────────────

func TestGRPCProxy_NilCredsRejected(t *testing.T) {
	idB := newTestIdentity(t, testKeyArchive1)
	caller := newTestIdentity(t, testKeyArchive2)

	serverCreds, err := idB.Auth.CreateServerTLSCredentials()
	if err != nil {
		t.Fatalf("server creds: %v", err)
	}
	clientCreds, err := caller.Auth.CreateClientTLSCredentials([]byte(idB.PeerID))
	if err != nil {
		t.Fatalf("client creds: %v", err)
	}

	port := freePort(t)

	// nil ServerCreds
	p1 := proxygrpc.NewGRPCProxy(zap.NewNop(), p2p.NewNetworkPartitioner(),
		[]proxygrpc.BackendEntry{{
			ListenPort: port, BackendAddr: "127.0.0.1:1234", PeerID: idB.PeerID,
			ServerCreds:          nil,
			ClientCredsPerCaller: map[peer.ID]credentials.TransportCredentials{caller.PeerID: clientCreds},
		}}, map[peer.ID]string{})
	if err := p1.Serve(); err == nil {
		p1.Close()
		t.Error("expected error for nil ServerCreds")
	}

	// empty ClientCredsPerCaller
	p2 := proxygrpc.NewGRPCProxy(zap.NewNop(), p2p.NewNetworkPartitioner(),
		[]proxygrpc.BackendEntry{{
			ListenPort: port, BackendAddr: "127.0.0.1:1234", PeerID: idB.PeerID,
			ServerCreds:          serverCreds,
			ClientCredsPerCaller: nil,
		}}, map[peer.ID]string{})
	if err := p2.Serve(); err == nil {
		p2.Close()
		t.Error("expected error for nil ClientCredsPerCaller")
	}
}
