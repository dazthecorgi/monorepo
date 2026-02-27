package testing

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
	proxygrpc "source.quilibrium.com/quilibrium/monorepo/simtest/proxy/grpc"
	"source.quilibrium.com/quilibrium/monorepo/simtest/proxy/p2p"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// startBackend starts an in-process gRPC backend server. The caller must call
// the returned cleanup func when done.
func startBackend(t *testing.T, svc protobufs.GlobalServiceServer) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startBackend: %v", err)
	}
	srv := grpc.NewServer()
	protobufs.RegisterGlobalServiceServer(srv, svc)
	go srv.Serve(ln) //nolint:errcheck
	return ln.Addr().String(), func() { srv.GracefulStop() }
}

// startRawStreamingBackend starts a backend that handles ANY service/method by
// keeping the response stream open, sending a byte every 20 ms until context done.
func startRawStreamingBackend(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startRawStreamingBackend: %v", err)
	}
	srv := grpc.NewServer(
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

// dialProxy dials the proxy on the given port and returns a GlobalServiceClient.
func dialProxy(t *testing.T, port int) (protobufs.GlobalServiceClient, func()) {
	t.Helper()
	cc, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialProxy: %v", err)
	}
	return protobufs.NewGlobalServiceClient(cc), func() { cc.Close() }
}

// dialProxyRaw opens a low-level ClientConn to the proxy port, suitable for
// NewStream calls with raw []byte payloads.
func dialProxyRaw(t *testing.T, port int) (*grpc.ClientConn, func()) {
	t.Helper()
	cc, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialProxyRaw: %v", err)
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

// newProxy is a convenience wrapper that creates and starts a GRPCProxy with
// the test's loopback IP mapped to srcPeerID.
func newProxy(
	t *testing.T,
	partitioner *p2p.NetworkPartitioner,
	srcPeerID peer.ID,
	backends []proxygrpc.BackendEntry,
) *proxygrpc.GRPCProxy {
	t.Helper()
	proxy := proxygrpc.NewGRPCProxy(
		zap.NewNop(),
		partitioner,
		backends,
		map[string]peer.ID{"127.0.0.1": srcPeerID},
	)
	if err := proxy.Serve(); err != nil {
		t.Fatalf("proxy.Serve: %v", err)
	}
	t.Cleanup(proxy.Close)
	return proxy
}

// ── Test 1: forwarding when not partitioned ──────────────────────────────────

func TestGRPCProxy_ForwardsWhenNotPartitioned(t *testing.T) {
	addrA, cleanA := startBackend(t, &stubGlobalServer{frameNumber: 1})
	defer cleanA()
	addrB, cleanB := startBackend(t, &stubGlobalServer{frameNumber: 2})
	defer cleanB()

	portA, portB := freePort(t), freePort(t)
	callerID := peer.ID("callerX")
	peerA := peer.ID("peerA")
	peerB := peer.ID("peerB")

	newProxy(t, p2p.NewNetworkPartitioner(), callerID, []proxygrpc.BackendEntry{
		{ListenPort: portA, BackendAddr: addrA, PeerID: peerA},
		{ListenPort: portB, BackendAddr: addrB, PeerID: peerB},
	})

	clientA, cleanCA := dialProxy(t, portA)
	defer cleanCA()
	respA, err := clientA.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if err != nil {
		t.Fatalf("proxy→peerA: %v", err)
	}
	if respA.Frame.Header.FrameNumber != 1 {
		t.Errorf("expected frame 1, got %d", respA.Frame.Header.FrameNumber)
	}

	clientB, cleanCB := dialProxy(t, portB)
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
	callerID := peer.ID("callerA")
	peerB := peer.ID("peerB")
	peerC := peer.ID("peerC")

	addrB, cleanB := startBackend(t, &stubGlobalServer{})
	defer cleanB()
	addrC, cleanC := startBackend(t, &stubGlobalServer{})
	defer cleanC()

	portB, portC := freePort(t), freePort(t)
	partitioner := p2p.NewNetworkPartitioner()
	partitioner.PartitionPeers(callerID, peerB)

	newProxy(t, partitioner, callerID, []proxygrpc.BackendEntry{
		{ListenPort: portB, BackendAddr: addrB, PeerID: peerB},
		{ListenPort: portC, BackendAddr: addrC, PeerID: peerC},
	})

	// cross-partition → Unavailable
	clientB, cleanCB := dialProxy(t, portB)
	defer cleanCB()
	_, err := clientB.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable for partitioned pair, got %v", err)
	}

	// intra-partition → success
	clientC, cleanCC := dialProxy(t, portC)
	defer cleanCC()
	if _, err := clientC.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Errorf("expected success for non-partitioned pair, got %v", err)
	}
}

// ── Test 3: dynamic partition change ─────────────────────────────────────────

func TestGRPCProxy_DynamicPartitionChange(t *testing.T) {
	callerID := peer.ID("callerA")
	peerB := peer.ID("peerB")

	addrB, cleanB := startBackend(t, &stubGlobalServer{})
	defer cleanB()

	portB := freePort(t)
	partitioner := p2p.NewNetworkPartitioner()
	newProxy(t, partitioner, callerID, []proxygrpc.BackendEntry{
		{ListenPort: portB, BackendAddr: addrB, PeerID: peerB},
	})

	client, cleanC := dialProxy(t, portB)
	defer cleanC()

	// no partition → success
	if _, err := client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Fatalf("expected success (no partition): %v", err)
	}

	// add partition → Unavailable
	partitioner.PartitionPeers(callerID, peerB)
	_, err := client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable after partition, got %v", err)
	}

	// clear partition → success
	partitioner.ClearPartitions()
	if _, err := client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Fatalf("expected success after clearing partition: %v", err)
	}
}

// ── Test 4: unknown source IP ─────────────────────────────────────────────────

func TestGRPCProxy_UnknownSourceIP(t *testing.T) {
	addrB, cleanB := startBackend(t, &stubGlobalServer{})
	defer cleanB()

	portB := freePort(t)
	// Empty ipToPeerID map — no IP is recognised.
	proxy := proxygrpc.NewGRPCProxy(
		zap.NewNop(),
		p2p.NewNetworkPartitioner(),
		[]proxygrpc.BackendEntry{
			{ListenPort: portB, BackendAddr: addrB, PeerID: peer.ID("peerB")},
		},
		map[string]peer.ID{}, // intentionally empty
	)
	if err := proxy.Serve(); err != nil {
		t.Fatalf("proxy.Serve: %v", err)
	}
	t.Cleanup(proxy.Close)

	client, cleanC := dialProxy(t, portB)
	defer cleanC()
	_, err := client.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for unknown IP, got %v", err)
	}
}

// ── Test 5: multiple backends with mixed partition sets ───────────────────────

func TestGRPCProxy_MultipleBackends(t *testing.T) {
	callerID := peer.ID("peerA") // set-1
	peerB := peer.ID("peerB")    // set-2 → blocked
	peerC := peer.ID("peerC")    // set-1 → allowed

	addrB, cleanB := startBackend(t, &stubGlobalServer{})
	defer cleanB()
	addrC, cleanC := startBackend(t, &stubGlobalServer{})
	defer cleanC()

	portB, portC := freePort(t), freePort(t)
	partitioner := p2p.NewNetworkPartitioner()
	partitioner.PartitionPeers(callerID, peerB)

	newProxy(t, partitioner, callerID, []proxygrpc.BackendEntry{
		{ListenPort: portB, BackendAddr: addrB, PeerID: peerB},
		{ListenPort: portC, BackendAddr: addrC, PeerID: peerC},
	})

	clientB, cleanCB := dialProxy(t, portB)
	defer cleanCB()
	_, err := clientB.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable for cross-partition call, got %v", err)
	}

	clientC, cleanCC := dialProxy(t, portC)
	defer cleanCC()
	if _, err := clientC.GetGlobalFrame(context.Background(), &protobufs.GetGlobalFrameRequest{}); err != nil {
		t.Errorf("expected success for intra-partition call, got %v", err)
	}
}

// ── Test 6: active stream terminated on partition ────────────────────────────

func TestGRPCProxy_ActiveStreamTerminatedOnPartition(t *testing.T) {
	callerID := peer.ID("peerA")
	peerB := peer.ID("peerB")

	backendAddr, cleanBackend := startRawStreamingBackend(t)
	defer cleanBackend()

	proxyPort := freePort(t)
	partitioner := p2p.NewNetworkPartitioner()

	proxy := proxygrpc.NewGRPCProxy(
		zap.NewNop(),
		partitioner,
		[]proxygrpc.BackendEntry{
			{ListenPort: proxyPort, BackendAddr: backendAddr, PeerID: peerB},
		},
		map[string]peer.ID{"127.0.0.1": callerID},
	)
	if err := proxy.Serve(); err != nil {
		t.Fatalf("proxy.Serve: %v", err)
	}
	defer proxy.Close()

	// Open a raw streaming call through the proxy.
	cc, cleanCC := dialProxyRaw(t, proxyPort)
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
	partitioner.PartitionPeers(callerID, peerB)

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
