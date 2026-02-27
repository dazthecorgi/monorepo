➜  monorepo-temp git:(temp) claude --dangerously-skip-permissions --max-turns 200 -p "Write a plan for extending simtest/proxy to handle proxying grpc requests between archive nodes while obeying the network partition schedule. The archive nodes can use the announceStreamListenMultiaddr p2p config option to announce the proxy as their grpc endpoint for the other nodes. In the plan, also include how to add integration tests for this functionality in simtest/proxy. Relevant top level directories: config, node, simtest"
Here is the plan:

---

## Plan: gRPC Proxying in `simtest/proxy` with Partition Enforcement

### Background

The existing proxy is a DHT-only pub/sub node that enforces network partitions by filtering BlossomSub message forwarding between partitioned peer pairs. It currently monitors archive nodes by connecting **out** to their gRPC endpoints.

The goal is to make it also proxy **inter-node** gRPC calls—requests that archive nodes make to each other—subject to the same partition schedule. Archive nodes advertise the proxy as their gRPC endpoint via `P2PConfig.AnnounceStreamListenMultiaddr`, so connections from peer B to peer A's announced gRPC address land at the proxy instead of directly at A.

---

### Part 1: TLS Consideration

**Problem**: `event_distributor.go` creates outbound gRPC connections using `p2p.NewPeerAuthenticator(...).CreateClientTLSCredentials(peerId)`, which performs mTLS with peer-ID verification. If the announced stream address points at the proxy, the TLS handshake will fail because the proxy cannot present archive-1's certificate.

**Solution**: Add a `DisableGlobalServiceAuthentication bool` field to `config.EngineConfig` (`config/engine.go`), as proposed in [issue #510](https://github.com/QuilibriumNetwork/monorepo/issues/510). When true, the node's gRPC server accepts unauthenticated connections and the node skips mTLS for outbound gRPC calls. In simtest, all archive nodes set `disableGlobalServiceAuthentication: true`. This field must never be set in production—it is safe only on an isolated local network.

The mTLS server code that is currently commented out in `global_consensus_engine.go` should be **uncommented** and guarded by this flag:

```go
if !e.config.Engine.DisableGlobalServiceAuthentication {
    tlsCreds, err := e.authProvider.CreateServerTLSCredentials()
    if err != nil {
        return errors.Wrap(err, "setup gRPC server")
    }
    e.grpcServer = qgrpc.NewServer(
        grpc.Creds(tlsCreds),
        grpc.ChainUnaryInterceptor(e.authProvider.UnaryInterceptor),
        grpc.ChainStreamInterceptor(e.authProvider.StreamInterceptor),
        grpc.MaxRecvMsgSize(e.config.Engine.SyncMessageLimits.MaxRecvMsgSize),
        grpc.MaxSendMsgSize(e.config.Engine.SyncMessageLimits.MaxSendMsgSize),
    )
} else {
    e.grpcServer = qgrpc.NewServer(
        grpc.MaxRecvMsgSize(e.config.Engine.SyncMessageLimits.MaxRecvMsgSize),
        grpc.MaxSendMsgSize(e.config.Engine.SyncMessageLimits.MaxSendMsgSize),
    )
}
var err error
```

In `event_distributor.go`, the outbound connection creation becomes:

```go
var dialCreds credentials.TransportCredentials
if e.config.Engine.DisableGlobalServiceAuthentication {
    dialCreds = insecure.NewCredentials()
} else {
    dialCreds, err = p2p.NewPeerAuthenticator(
        e.logger, e.config.P2P, nil, nil, nil, nil,
        [][]byte{[]byte(peerId)},
        map[string]channel.AllowedPeerPolicyType{},
        map[string]channel.AllowedPeerPolicyType{},
    ).CreateClientTLSCredentials([]byte(peerId))
    if err != nil { return nil, false }
}
cc, err := grpc.NewClient(mga.String(), grpc.WithTransportCredentials(dialCreds))
```

The proxy's gRPC listeners always use insecure transport (simtest-only component).

---

### Part 2: Source Identification

The proxy must identify *which* archive node is the caller so it can check the (source, destination) pair against the partition table.

**Approach**: Identify the caller by its IP address using gRPC's `peer.FromContext(ctx)` (from `google.golang.org/grpc/peer`). At startup the proxy builds a `map[string]peer.ID` from the `NODE_ADDRESSES` env var and the peer IDs in `RANK_PARTITIONS`: for each `archive-N` backend, resolve its hostname to an IP and store `ip → peerID`. Within the Docker Compose network each container gets a stable IP, so this mapping is valid for the lifetime of the test run.

In `simtest/proxy/grpc/middleware.go`, the `UnknownServiceHandler` wrapper performs:

```go
p, ok := peer.FromContext(ctx)
if !ok {
    return status.Error(codes.Unauthenticated, "no peer info in context")
}
host, _, _ := net.SplitHostPort(p.Addr.String())
srcPeerID, ok := ipToPeerID[host]
if !ok {
    return status.Error(codes.Unauthenticated, "unknown source IP")
}
```

No changes to `event_distributor.go` or any node-side code are required for source identification.


### Part 3: Proxy Port Assignment

The proxy exposes **one listening TCP port per archive node**. Port-based routing lets the proxy know the intended destination without any request inspection.

- Default base port: `9000` (configurable via `GRPC_PROXY_BASE_PORT` env var)
- `archive-1` → proxy listens on port `9001`, forwards to `archive-1:8340`
- `archive-2` → proxy listens on port `9002`, forwards to `archive-2:8340`
- etc.

The `NODE_ADDRESSES` env var already carries `archive-1:8340,archive-2:8340,...`. The proxy derives the proxy port by index: entry at position `i` gets port `basePort + i + 1`.

Each archive node's `announceStreamListenMultiaddr` is pre-configured statically in its config YAML (see Part 6) so that when containers start they already advertise the correct proxy address.

---

### Part 4: New Component — `simtest/proxy/grpc/grpc_proxy.go`

```
simtest/proxy/grpc/
    constructors.go      (existing)
    middleware.go        (existing — add IP→peerID lookup in UnknownServiceHandler)
    peer_id.go           (existing)
    observability.go     (existing)
    grpc_proxy.go        (NEW)
```

**`GRPCProxy` struct**:

```go
type BackendEntry struct {
    ListenPort int
    BackendAddr string    // e.g. "archive-1:8340"
    PeerID      peer.ID   // the archive node's peer ID
}

type GRPCProxy struct {
    logger      *zap.Logger
    partitioner *networkPartitioner  // shared with BlossomSubProxy
    backends    []BackendEntry
    servers     []*grpc.Server
    listeners   []net.Listener
}
```

**`NewGRPCProxy`**: Accepts `logger`, `partitioner`, and `[]BackendEntry`. For each entry, starts a `net.Listener` on `0.0.0.0:<port>` and a `grpc.Server` that handles all incoming services via a generic codec director.

**Generic director pattern**: Use a `grpc.UnknownServiceHandler` that:
1. Extracts the source peer ID from the incoming connection's remote IP address (via `peer.FromContext`), looked up in the IP→peerID map.
2. Looks up the destination peer ID from the `BackendEntry` for this server's port.
3. Calls `partitioner.forwardFilter(srcPeerID, dstPeerID)`.
4. If blocked: returns `status.Error(codes.Unavailable, "simtest: network partition")`.
5. If allowed: dials the backend address (using a cached `grpc.ClientConn` per backend), then uses `grpc.ForwardServerToClient` / streams bridging to transparently forward the call.

For the transparent forwarding, use the codec-based approach: set `grpc.ForceCodec(proxy.Codec())` on the server and a director function, matching the pattern from `mwitkow/grpc-proxy` (which is a well-known OSS library for this exact use case). If taking on a new dependency is undesirable, the same can be achieved with a small custom implementation using `grpc.UnknownServiceHandler` + raw stream forwarding via `grpc.ServerStream` and `grpc.ClientStream`.

**`GRPCProxy.Serve`**: Starts all listeners in goroutines. Returns a `Close()` that gracefully stops all servers.

---

### Part 5: Wire into `simtest/proxy/main.go`

Changes to `main.go`:

1. Read `GRPC_PROXY_BASE_PORT` (default `9000`).
2. Parse `NODE_ADDRESSES` into `[]BackendEntry` by splitting on `,` and assigning sequential proxy ports. Peer IDs are taken from `RANK_PARTITIONS` peer ID strings (they are already passed as resolved peer IDs).
3. Build the IP→peerID map by resolving each backend hostname to its IP and storing `ip → peerID`; pass this map to `NewGRPCProxy`.
4. Construct `GRPCProxy` and call `Serve()` before the main loop.
5. Call `grpcProxy.Close()` in the shutdown sequence.

```go
grpcProxy := grpc.NewGRPCProxy(logger, partitioner, backends)
if err := grpcProxy.Serve(); err != nil {
    logger.Fatal("failed to start gRPC proxy", zap.Error(err))
}
defer grpcProxy.Close()
```

The `networkPartitioner` pointer is currently created inside `NewBlossomSubProxy`—extract it to be created in `main.go` and passed to both `NewBlossomSubProxy` and `NewGRPCProxy`, so both share the exact same partition state.

---

### Part 6: `simtest/main.go` — Config Injection

**No runtime injection needed.** Instead, edit each archive node's config YAML once (customising the peer ID and DNS name per node). For `archive-1` the relevant P2P fields become:

```yaml
listenMultiaddr: /ip4/0.0.0.0/udp/8336/quic-v1
streamListenMultiaddr: "/ip4/0.0.0.0/tcp/8340/"
announceListenMultiaddr: "/dns/archive-1/udp/8336/quic-v1/p2p/<archive-1-peer-id>"
announceStreamListenMultiaddr: "/dns/proxy/tcp/8341/"
```

The proxy port per node uses fixed offsets from the gRPC base port (archive-1 → `8341`, archive-2 → `8342`, etc.). The proxy hostname `proxy` resolves within the Docker Compose network.

These values are committed once to `simtest/config/archive-{1..4}-config/config.yml`. No code changes to `simtest/main.go` are required for address injection.

---

### Part 7: Integration Tests in `simtest/proxy/testing/`

Add a new file: **`simtest/proxy/testing/grpc_proxy_test.go`**

**Test 1: `TestGRPCProxy_ForwardsWhenNotPartitioned`**
- Start two in-process gRPC servers (`grpc.NewServer()`) each implementing `GlobalService` with a stub `GetGlobalFrame`.
- Create a `networkPartitioner` with no partitions.
- Create a `GRPCProxy` with two `BackendEntry`s pointing at the in-process servers' ports.
- Dial the proxy ports using insecure credentials.
- Make `GetGlobalFrame` calls through both proxy ports.
- Assert responses arrive and match the stub responses.

**Test 2: `TestGRPCProxy_BlocksWhenPartitioned`**
- Same setup as Test 1.
- Add peerA and peerB to the partitioner via `partitioner.PartitionPeers(peerA, peerB)`.
- Pre-populate the IP→peerID map to map the test client's loopback IP to peerA.
- Make a request through the proxy port for peerB: assert the call returns `codes.Unavailable`.
- Make a request through the proxy port for peerA (self-to-self not blocked): assert the call succeeds.

**Test 3: `TestGRPCProxy_DynamicPartitionChange`**
- Start with no partition: verify call succeeds.
- Apply partition between peerA and peerB: verify call fails.
- Clear partition: verify call succeeds again.
- This tests the live partition update path used by `applyRankPartition`.

**Test 4: `TestGRPCProxy_UnknownSourceIP`**
- Leave the IP→peerID map empty (no entry for the test client's loopback IP).
- Send a request through any proxy port.
- Assert the proxy returns `codes.Unauthenticated`.

**Test 5: `TestGRPCProxy_MultipleBackends`**
- Three backends (peerA, peerB, peerC). peerA and peerB are in opposite partition sets; peerA and peerC are in the same set.
- Pre-populate the IP→peerID map so the test client's IP resolves to peerA.
- Verify the proxy port for peerB (cross-set) is blocked (`codes.Unavailable`).
- Verify the proxy port for peerC (intra-set) succeeds — confirming that partitions only block cross-set calls.

**Test 6: `TestGRPCProxy_ActiveStreamTerminatedOnPartition`**
- Start a long-lived server-streaming RPC from a source peer (peerA) to a destination peer (peerB) via the proxy, using a stub backend that keeps the stream open indefinitely.
- While the stream is open, apply a blocking partition between peerA and peerB via `partitioner.PartitionPeers(peerA, peerB)`.
- Assert the stream is closed by the proxy within a bounded time (e.g. 2 seconds) with `codes.Unavailable`.
- Assert that a subsequent fresh unary call from peerA to peerB's proxy port also returns `codes.Unavailable`, confirming the partition is enforced for new connections too.

Each test creates fresh `net.Listener`s on `:0` (OS-assigned ports) to avoid port conflicts. The in-process gRPC servers use mock `GlobalServiceServer` implementations (can reuse the existing mock in `simtest/proxy/mocks/`).

---

### Summary of File Changes

| File | Change |
|---|---|
| `config/engine.go` | Add `DisableGlobalServiceAuthentication bool` field |
| `node/consensus/global/global_consensus_engine.go` | Uncomment mTLS server setup; guard with `DisableGlobalServiceAuthentication` |
| `node/consensus/global/event_distributor.go` | Use insecure credentials when `DisableGlobalServiceAuthentication` is set |
| `simtest/proxy/grpc/grpc_proxy.go` | **New**: `GRPCProxy`, `BackendEntry`, transparent forwarding + partition enforcement |
| `simtest/proxy/grpc/middleware.go` | Add IP→peerID lookup in `UnknownServiceHandler` |
| `simtest/proxy/main.go` | Extract `networkPartitioner`, build IP→peerID map, construct `GRPCProxy`, read `GRPC_PROXY_BASE_PORT` |
| `simtest/config/archive-{1..4}-config/config.yml` | One-time edit: set `announceListenMultiaddr` and `announceStreamListenMultiaddr` |
| `simtest/proxy/testing/grpc_proxy_test.go` | **New**: Integration tests for proxy forwarding and partition enforcement |
