package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Partitioner decides whether forwarding between two peers is allowed.
// *p2p.NetworkPartitioner satisfies this interface.
type Partitioner interface {
	ForwardFilter(from, to peer.ID) bool
}

// rawBytesCodec is a server-side codec that passes proto-encoded bytes through
// unchanged, enabling transparent gRPC proxying without proto schema knowledge.
type rawBytesCodec struct{}

func (rawBytesCodec) Marshal(v interface{}) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("rawBytesCodec: marshal expects []byte, got %T", v)
	}
	return b, nil
}

func (rawBytesCodec) Unmarshal(data []byte, v interface{}) error {
	ptr, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawBytesCodec: unmarshal expects *[]byte, got %T", v)
	}
	*ptr = make([]byte, len(data))
	copy(*ptr, data)
	return nil
}

// Name returns "proto" so the codec matches the standard content-type sent by
// gRPC clients, avoiding a content-type negotiation mismatch.
func (rawBytesCodec) Name() string { return "proto" }

// String satisfies the deprecated grpc.Codec interface required by CustomCodec.
func (rawBytesCodec) String() string { return "proto" }

// BackendEntry maps a proxy listen port to a backend archive node.
type BackendEntry struct {
	// ListenPort is the TCP port the proxy listens on for this backend.
	ListenPort int
	// BackendAddr is the dial address for the archive node (e.g. "archive-1:8340").
	BackendAddr string
	// PeerID is the archive node's libp2p peer ID, used as the destination in
	// partition checks.
	PeerID peer.ID
	// ServerCreds are the TLS credentials presented by the proxy listener
	// impersonating the backend node.
	ServerCreds credentials.TransportCredentials
	// ClientCreds are the TLS credentials used when dialling the backend node.
	ClientCreds credentials.TransportCredentials
}

// GRPCProxy proxies gRPC calls between archive nodes, enforcing network
// partitions from a shared Partitioner. One TCP listener per backend
// gives the proxy port-based routing without inspecting request payloads.
type GRPCProxy struct {
	logger      *zap.Logger
	partitioner Partitioner
	backends    []BackendEntry
	ipToPeerID  map[string]peer.ID
	servers     []*grpc.Server
	listeners   []net.Listener
	conns       []*grpc.ClientConn
}

// NewGRPCProxy creates a GRPCProxy. It does not start listening; call Serve.
// ipToPeerID maps each archive node's container IP to its peer.ID so that the
// proxy can identify the caller from the remote address of an incoming connection.
func NewGRPCProxy(
	logger *zap.Logger,
	partitioner Partitioner,
	backends []BackendEntry,
	ipToPeerID map[string]peer.ID,
) *GRPCProxy {
	return &GRPCProxy{
		logger:      logger,
		partitioner: partitioner,
		backends:    backends,
		ipToPeerID:  ipToPeerID,
	}
}

// Serve starts one TCP listener and one gRPC server per backend entry. Returns
// an error if any listener or client connection cannot be created; in that case
// all already-created resources are cleaned up before returning.
func (g *GRPCProxy) Serve() error {
	for i, backend := range g.backends {
		if backend.ServerCreds == nil {
			return fmt.Errorf("grpc proxy: backend %d (%s): ServerCreds is required", i, backend.BackendAddr)
		}
		if backend.ClientCreds == nil {
			return fmt.Errorf("grpc proxy: backend %d (%s): ClientCreds is required", i, backend.BackendAddr)
		}

		ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", backend.ListenPort))
		if err != nil {
			g.Close()
			return fmt.Errorf("grpc proxy: listen port %d: %w", backend.ListenPort, err)
		}

		cc, err := grpc.NewClient(
			backend.BackendAddr,
			grpc.WithTransportCredentials(backend.ClientCreds),
		)
		if err != nil {
			ln.Close()
			g.Close()
			return fmt.Errorf("grpc proxy: dial backend %s: %w", backend.BackendAddr, err)
		}

		g.listeners = append(g.listeners, ln)
		g.conns = append(g.conns, cc)

		// grpc.CustomCodec sets a server-wide codec so RecvMsg/SendMsg operate on
		// raw []byte rather than proto.Message, enabling transparent forwarding.
		// ForceCodec is CallOption-only in grpc v1.72; CustomCodec is the only
		// server-side option available.
		srv := grpc.NewServer(
			//lint:ignore SA1019 ForceCodec is CallOption-only in grpc v1.72; CustomCodec is the only server-side option
			grpc.CustomCodec(rawBytesCodec{}),
			grpc.UnknownServiceHandler(g.makeHandler(backend.PeerID, cc)),
			grpc.Creds(backend.ServerCreds),
		)
		g.servers = append(g.servers, srv)

		g.logger.Info("grpc proxy backend registered",
			zap.Int("port", backend.ListenPort),
			zap.String("backend", backend.BackendAddr),
			zap.String("peer_id", backend.PeerID.String()),
		)

		go func(s *grpc.Server, l net.Listener, idx int) {
			if err := s.Serve(l); err != nil {
				g.logger.Error("grpc proxy server stopped",
					zap.Int("backend_index", idx),
					zap.Error(err),
				)
			}
		}(srv, ln, i)
	}
	return nil
}

// Close gracefully stops all proxy servers and closes backend connections.
func (g *GRPCProxy) Close() {
	for _, srv := range g.servers {
		srv.GracefulStop()
	}
	for _, cc := range g.conns {
		_ = cc.Close()
	}
}

// makeHandler returns a grpc.StreamHandler that:
//  1. Identifies the calling archive node by its remote IP.
//  2. Checks the (source, dstPeerID) pair against the partition table.
//  3. Bridges the ServerStream to a ClientStream on the backend, checking the
//     partition on every forwarded message and via a background monitor goroutine
//     so that active streams are terminated within ~100 ms of a partition change.
func (g *GRPCProxy) makeHandler(dstPeerID peer.ID, cc *grpc.ClientConn) grpc.StreamHandler {
	return func(_ interface{}, serverStream grpc.ServerStream) error {
		// Identify source peer from the incoming connection's remote IP.
		p, ok := grpcpeer.FromContext(serverStream.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "simtest: no peer info in context")
		}
		host, _, err := net.SplitHostPort(p.Addr.String())
		if err != nil {
			return status.Error(codes.Unauthenticated, "simtest: failed to parse remote address")
		}
		srcPeerID, ok := g.ipToPeerID[host]
		if !ok {
			return status.Error(codes.Unauthenticated, "simtest: unknown source IP")
		}

		// Reject immediately if already partitioned.
		if !g.partitioner.ForwardFilter(srcPeerID, dstPeerID) {
			return status.Error(codes.Unavailable, "simtest: network partition")
		}

		// Retrieve the full method name (/package.Service/Method) from the
		// server transport stream stored in the context.
		transport := grpc.ServerTransportStreamFromContext(serverStream.Context())
		fullMethod := ""
		if transport != nil {
			fullMethod = transport.Method()
		}

		ctx, cancel := context.WithCancel(serverStream.Context())
		defer cancel()

		// Open a client stream to the backend using the passthrough codec.
		//nolint:staticcheck // ForceCodec is CallOption; CustomCodec used on server above
		clientStream, err := cc.NewStream(ctx, &grpc.StreamDesc{
			ServerStreams: true,
			ClientStreams: true,
		}, fullMethod, grpc.ForceCodec(rawBytesCodec{}))
		if err != nil {
			return status.Errorf(codes.Unavailable, "simtest: backend unavailable: %v", err)
		}

		// firstErr is set once by the winning goroutine; cancel() stops the rest.
		var once sync.Once
		firstErrCh := make(chan error, 1)
		setErr := func(err error) {
			once.Do(func() {
				firstErrCh <- err
				cancel()
			})
		}

		var wg sync.WaitGroup
		wg.Add(3)

		// Partition monitor: cancel context quickly after a partition is applied.
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if !g.partitioner.ForwardFilter(srcPeerID, dstPeerID) {
						setErr(status.Error(codes.Unavailable, "simtest: network partition"))
						return
					}
				}
			}
		}()

		// Forward caller → backend.
		go func() {
			defer wg.Done()
			for {
				var frame []byte
				if err := serverStream.RecvMsg(&frame); err != nil {
					if err == io.EOF {
						// Half-close the client stream so the backend knows the
						// client is done sending.  Do NOT call cancel() here:
						// the backend may still have responses to deliver.
						_ = clientStream.CloseSend()
					} else {
						setErr(err)
					}
					return
				}
				if !g.partitioner.ForwardFilter(srcPeerID, dstPeerID) {
					setErr(status.Error(codes.Unavailable, "simtest: network partition"))
					return
				}
				if err := clientStream.SendMsg(frame); err != nil {
					setErr(err)
					return
				}
			}
		}()

		// Forward backend → caller.
		go func() {
			defer wg.Done()
			for {
				var frame []byte
				if err := clientStream.RecvMsg(&frame); err != nil {
					if err == io.EOF {
						cancel()
					} else {
						setErr(err)
					}
					return
				}
				if err := serverStream.SendMsg(frame); err != nil {
					setErr(err)
					return
				}
			}
		}()

		wg.Wait()

		select {
		case err := <-firstErrCh:
			if err == io.EOF {
				return nil
			}
			return err
		default:
			return nil
		}
	}
}
