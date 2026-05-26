package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"source.quilibrium.com/quilibrium/monorepo/protobufs"
)

// GLOBAL_INTRINSIC_ADDRESS is the 32-byte global-intrinsic application address
// (all-0xFF). A prover registry vertex's 64-byte address is this prefix
// concatenated with the 32-byte Poseidon(BLS pubkey) prover address. Mirrors
// `crates/quil-execution/src/global_schema.rs:163`.
var globalIntrinsicAddress = bytes.Repeat([]byte{0xFF}, 32)

// EnrollmentTarget describes one client whose prover registration should be
// confirmed in the archives' registry. ProverAddress is the hex-encoded
// 32-byte Poseidon(BLS pubkey).
//
// When NodeAddress is non-empty, the monitor also polls the client's own
// NodeService.GetWorkerInfo and requires at least one worker to be bound to
// a filter — a downstream signal that the join round-tripped back into the
// client's local CRDT via the archive poller. Archive-quorum is the
// load-bearing check; the client-side worker check is supplementary and
// requires the client to expose its NodeService gRPC (`listenGrpcMultiaddr`
// non-empty in the client's config).
//
// ExpectedCores, when > 0, tightens the client-side check from "at least
// one worker bound" to "exactly N workers bound" — used by tests that pin
// the client's CPU set so the join's filter count is deterministic.
type EnrollmentTarget struct {
	Name          string
	ProverAddress []byte // 32 bytes
	NodeAddress   string // host:port of client NodeService gRPC; "" disables client-side check
	ExpectedCores int    // when > 0, require exactly this many workers bound
}

// ArchiveTarget describes one archive whose registry should be polled.
type ArchiveTarget struct {
	Name    string
	Address string // host:port of the NodeService gRPC endpoint
}

// EnrollmentMonitor polls each archive's NodeService.GetVertexData for each
// client's prover address. A poll succeeds for an archive when EVERY client
// vertex returns at least one entry. The monitor's overall verdict is success
// when at least `minArchives` archives have returned a fully-successful poll.
//
// Mirrors the FrameMonitor's "minimum-N-of-total" quorum shape, but for the
// registry side of the join lifecycle.
type EnrollmentMonitor struct {
	ctx          context.Context
	logger       *zap.Logger
	archives     []ArchiveTarget
	clients      []EnrollmentTarget
	pollInterval time.Duration
	timeout      time.Duration
	minArchives  int

	clients_      map[string]protobufs.NodeServiceClient // archive name → NodeService client
	conns         map[string]*grpc.ClientConn            // archive name → conn
	clientNodes   map[string]protobufs.NodeServiceClient // client name → NodeService client
	clientConns   map[string]*grpc.ClientConn            // client name → conn

	mu           sync.Mutex
	status       map[string]archiveStatus // archive name → most-recent archive poll
	clientStatus map[string]clientStatus  // client name → most-recent client-side worker poll
}

type archiveStatus struct {
	lastPolled   time.Time
	firstPolled  time.Time
	allConfirmed bool                 // every client vertex found
	missing      []string             // client names whose vertex was missing on the last poll
	err          error                // most recent transport/RPC error
	perClient    map[string]bool      // client name → vertex found on last successful poll
	rawEntries   map[string]int       // client name → entry count returned (for diagnostics)
}

// clientStatus tracks what a single client reports about its own enrollment
// via NodeService.GetWorkerInfo. Confirmed=true when at least one worker is
// bound to a non-empty filter (or, when expectedCores>0, when exactly that
// many are bound) — the client's local CRDT has caught up enough to know it
// owns an allocation. Tracked separately from the archive-side quorum
// because the client's view lags the archive view (it polls archives for
// frames, replays them locally) and we want to surface both signals.
type clientStatus struct {
	lastPolled    time.Time
	firstPolled   time.Time
	confirmed     bool   // workers-bound matches expectation (>=1, or ==expectedCores)
	workersBound  int    // count of workers with non-empty filter
	workersTotal  int    // total worker entries returned
	expectedCores int    // 0 = "any positive"; >0 = exact match required
	err           error  // most recent transport/RPC error
	skipped       bool   // true when NodeAddress is empty — no client-side check requested
}

// NewEnrollmentMonitor builds a monitor with a gRPC client for each archive.
// All connections are plaintext because the rust binary's NodeService listens
// in the clear at `listen_grpc_multiaddr` (qclient-facing convention).
func NewEnrollmentMonitor(
	ctx context.Context,
	logger *zap.Logger,
	archives []ArchiveTarget,
	clients []EnrollmentTarget,
	pollInterval time.Duration,
	minArchives int,
	timeout time.Duration,
) (*EnrollmentMonitor, error) {
	em := &EnrollmentMonitor{
		ctx:          ctx,
		logger:       logger,
		archives:     archives,
		clients:      clients,
		pollInterval: pollInterval,
		timeout:      timeout,
		minArchives:  minArchives,
		clients_:     make(map[string]protobufs.NodeServiceClient, len(archives)),
		conns:        make(map[string]*grpc.ClientConn, len(archives)),
		clientNodes:  make(map[string]protobufs.NodeServiceClient, len(clients)),
		clientConns:  make(map[string]*grpc.ClientConn, len(clients)),
		status:       make(map[string]archiveStatus, len(archives)),
		clientStatus: make(map[string]clientStatus, len(clients)),
	}
	for _, a := range archives {
		conn, err := grpc.NewClient(a.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("enrollment monitor: dial %s (%s): %w", a.Name, a.Address, err)
		}
		em.conns[a.Name] = conn
		em.clients_[a.Name] = protobufs.NewNodeServiceClient(conn)
		em.status[a.Name] = archiveStatus{perClient: make(map[string]bool), rawEntries: make(map[string]int)}
	}
	for _, c := range clients {
		if c.NodeAddress == "" {
			em.clientStatus[c.Name] = clientStatus{skipped: true, expectedCores: c.ExpectedCores}
			continue
		}
		conn, err := grpc.NewClient(c.NodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("enrollment monitor: dial client %s (%s): %w", c.Name, c.NodeAddress, err)
		}
		em.clientConns[c.Name] = conn
		em.clientNodes[c.Name] = protobufs.NewNodeServiceClient(conn)
		em.clientStatus[c.Name] = clientStatus{expectedCores: c.ExpectedCores}
	}
	return em, nil
}

// Close tears down the gRPC connections.
func (em *EnrollmentMonitor) Close() {
	for name, conn := range em.conns {
		if err := conn.Close(); err != nil {
			em.logger.Warn("enrollment monitor: close archive conn",
				zap.String("archive", name), zap.Error(err))
		}
	}
	for name, conn := range em.clientConns {
		if err := conn.Close(); err != nil {
			em.logger.Warn("enrollment monitor: close client conn",
				zap.String("client", name), zap.Error(err))
		}
	}
}

// WaitForEnrollment polls archives until `minArchives` of them confirm every
// client's prover_address is in the registry, or the timeout elapses.
// Returns nil on success; a descriptive error otherwise.
//
// No-op (returns nil immediately) when the client list is empty — there's
// nothing to assert.
func (em *EnrollmentMonitor) WaitForEnrollment() error {
	if len(em.clients) == 0 {
		em.logger.Info("enrollment monitor: no clients to verify, skipping")
		return nil
	}
	if em.minArchives <= 0 || em.minArchives > len(em.archives) {
		return fmt.Errorf(
			"enrollment monitor: minArchives=%d out of range (archives=%d)",
			em.minArchives, len(em.archives))
	}

	em.logger.Info("enrollment monitor: starting",
		zap.Int("archives", len(em.archives)),
		zap.Int("clients", len(em.clients)),
		zap.Int("min_archives", em.minArchives),
		zap.Duration("timeout", em.timeout),
		zap.Duration("poll_interval", em.pollInterval))

	deadline := time.Now().Add(em.timeout)
	ticker := time.NewTicker(em.pollInterval)
	defer ticker.Stop()

	em.pollAll()
	if ok, _ := em.quorumReached(); ok {
		em.logSummary("succeeded on initial poll")
		return nil
	}

	for {
		select {
		case <-ticker.C:
			em.pollAll()
			if ok, _ := em.quorumReached(); ok {
				em.logSummary("succeeded")
				return nil
			}
			if time.Now().After(deadline) {
				return em.timeoutError()
			}
		case <-em.ctx.Done():
			return fmt.Errorf("enrollment monitor: cancelled: %w", em.ctx.Err())
		}
	}
}

// pollAll fans out one poll per archive AND one per client (when the client
// has a NodeAddress configured) in parallel.
func (em *EnrollmentMonitor) pollAll() {
	var wg sync.WaitGroup
	for _, a := range em.archives {
		wg.Add(1)
		go func(a ArchiveTarget) {
			defer wg.Done()
			em.pollArchive(a)
		}(a)
	}
	for _, c := range em.clients {
		if c.NodeAddress == "" {
			continue
		}
		wg.Add(1)
		go func(c EnrollmentTarget) {
			defer wg.Done()
			em.pollClient(c)
		}(c)
	}
	wg.Wait()
}

// pollClient queries a single client's NodeService.GetWorkerInfo and
// records whether any worker is bound to a non-empty filter. A bound
// worker means the client has locally observed its own allocation land —
// the join round-trip through the archives is complete from the client's
// perspective.
func (em *EnrollmentMonitor) pollClient(c EnrollmentTarget) {
	node, ok := em.clientNodes[c.Name]
	if !ok {
		return
	}
	now := time.Now()

	ctx, cancel := context.WithTimeout(em.ctx, 5*time.Second)
	resp, err := node.GetWorkerInfo(ctx, &protobufs.GetWorkerInfoRequest{})
	cancel()

	em.mu.Lock()
	defer em.mu.Unlock()
	st := em.clientStatus[c.Name]
	if st.firstPolled.IsZero() {
		st.firstPolled = now
	}
	st.lastPolled = now
	if err != nil {
		st.err = err
		st.confirmed = false
		st.workersBound = 0
		st.workersTotal = 0
		em.clientStatus[c.Name] = st
		em.logger.Debug("enrollment client poll",
			zap.String("client", c.Name),
			zap.Error(err))
		return
	}
	st.err = nil
	st.workersTotal = len(resp.WorkerInfo)
	bound := 0
	for _, w := range resp.WorkerInfo {
		if len(w.Filter) > 0 {
			bound++
		}
	}
	st.workersBound = bound
	if st.expectedCores > 0 {
		st.confirmed = bound == st.expectedCores
	} else {
		st.confirmed = bound > 0
	}
	em.clientStatus[c.Name] = st
	em.logger.Debug("enrollment client poll",
		zap.String("client", c.Name),
		zap.Bool("confirmed", st.confirmed),
		zap.Int("workers_bound", bound),
		zap.Int("workers_total", st.workersTotal),
		zap.Int("expected_cores", st.expectedCores))
}

// pollArchive checks every client's vertex on a single archive. Updates
// `em.status[a.Name]` with the outcome.
func (em *EnrollmentMonitor) pollArchive(a ArchiveTarget) {
	client := em.clients_[a.Name]
	now := time.Now()

	em.mu.Lock()
	st := em.status[a.Name]
	if st.firstPolled.IsZero() {
		st.firstPolled = now
	}
	st.lastPolled = now
	if st.perClient == nil {
		st.perClient = make(map[string]bool, len(em.clients))
	}
	if st.rawEntries == nil {
		st.rawEntries = make(map[string]int, len(em.clients))
	}
	em.mu.Unlock()

	var (
		allFound = true
		missing  []string
		callErr  error
	)
	for _, c := range em.clients {
		addr := append([]byte(nil), globalIntrinsicAddress...)
		addr = append(addr, c.ProverAddress...)

		ctx, cancel := context.WithTimeout(em.ctx, 5*time.Second)
		resp, err := client.GetVertexData(ctx, &protobufs.GetVertexDataRequest{
			Address:  addr,
			FullData: false,
		})
		cancel()

		em.mu.Lock()
		if err != nil {
			callErr = err
			allFound = false
			missing = append(missing, c.Name)
			st.perClient[c.Name] = false
			st.rawEntries[c.Name] = 0
		} else {
			n := len(resp.Entries)
			st.rawEntries[c.Name] = n
			if n > 0 {
				st.perClient[c.Name] = true
			} else {
				st.perClient[c.Name] = false
				allFound = false
				missing = append(missing, c.Name)
			}
		}
		em.mu.Unlock()
	}

	em.mu.Lock()
	st.err = callErr
	st.allConfirmed = allFound
	st.missing = missing
	em.status[a.Name] = st
	em.mu.Unlock()

	em.logger.Debug("enrollment poll",
		zap.String("archive", a.Name),
		zap.Bool("all_confirmed", allFound),
		zap.Strings("missing", missing),
		zap.Any("entries_per_client", st.rawEntries),
		zap.Error(callErr))
}

// quorumReached returns true when:
//   - at least `minArchives` archives have allConfirmed=true, AND
//   - every client with a NodeAddress has its own confirmed=true (skipped
//     clients pass trivially).
//
// Both signals must clear: archive quorum is the load-bearing source of
// truth, but the client-side worker check catches the case where the
// archives agree the prover is registered yet the client itself hasn't
// pulled the frame back into its local state. The second return value is
// the archive-confirmed count for diagnostic purposes.
func (em *EnrollmentMonitor) quorumReached() (bool, int) {
	em.mu.Lock()
	defer em.mu.Unlock()
	archiveCount := 0
	for _, st := range em.status {
		if st.allConfirmed {
			archiveCount++
		}
	}
	if archiveCount < em.minArchives {
		return false, archiveCount
	}
	for _, st := range em.clientStatus {
		if st.skipped {
			continue
		}
		if !st.confirmed {
			return false, archiveCount
		}
	}
	return true, archiveCount
}

func (em *EnrollmentMonitor) logSummary(prefix string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	archiveNames := make([]string, 0, len(em.status))
	for n := range em.status {
		archiveNames = append(archiveNames, n)
	}
	sort.Strings(archiveNames)
	for _, n := range archiveNames {
		st := em.status[n]
		em.logger.Info("enrollment archive status",
			zap.String("prefix", prefix),
			zap.String("archive", n),
			zap.Bool("all_confirmed", st.allConfirmed),
			zap.Any("entries_per_client", st.rawEntries),
			zap.Strings("missing", st.missing))
	}

	clientNames := make([]string, 0, len(em.clientStatus))
	for n := range em.clientStatus {
		clientNames = append(clientNames, n)
	}
	sort.Strings(clientNames)
	for _, n := range clientNames {
		st := em.clientStatus[n]
		if st.skipped {
			em.logger.Info("enrollment client status",
				zap.String("prefix", prefix),
				zap.String("client", n),
				zap.String("note", "no NodeAddress configured — client-side check skipped"))
			continue
		}
		em.logger.Info("enrollment client status",
			zap.String("prefix", prefix),
			zap.String("client", n),
			zap.Bool("confirmed", st.confirmed),
			zap.Int("workers_bound", st.workersBound),
			zap.Int("workers_total", st.workersTotal))
	}
}

func (em *EnrollmentMonitor) timeoutError() error {
	em.mu.Lock()
	defer em.mu.Unlock()

	confirmed := 0
	clientCoverage := make(map[string]int, len(em.clients))
	archiveSummaries := make([]string, 0, len(em.archives))

	names := make([]string, 0, len(em.status))
	for n := range em.status {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		st := em.status[n]
		if st.allConfirmed {
			confirmed++
		}
		for c, found := range st.perClient {
			if found {
				clientCoverage[c]++
			}
		}
		summary := fmt.Sprintf("%s=", n)
		if st.err != nil {
			summary += fmt.Sprintf("err(%v)", st.err)
		} else if st.allConfirmed {
			summary += "ok"
		} else {
			summary += fmt.Sprintf("missing%v", st.missing)
		}
		archiveSummaries = append(archiveSummaries, summary)
	}

	clientNames := make([]string, 0, len(em.clients))
	for _, c := range em.clients {
		clientNames = append(clientNames, c.Name)
	}
	sort.Strings(clientNames)
	clientLines := make([]string, 0, len(clientNames))
	for _, c := range clientNames {
		clientLines = append(clientLines,
			fmt.Sprintf("%s=%d/%d", c, clientCoverage[c], len(em.archives)))
	}

	// Per-client side (worker info) summary, separate from the archive-coverage view.
	clientStatusLines := make([]string, 0, len(em.clientStatus))
	clientStatusNames := make([]string, 0, len(em.clientStatus))
	for n := range em.clientStatus {
		clientStatusNames = append(clientStatusNames, n)
	}
	sort.Strings(clientStatusNames)
	for _, n := range clientStatusNames {
		st := em.clientStatus[n]
		switch {
		case st.skipped:
			clientStatusLines = append(clientStatusLines, fmt.Sprintf("%s=skipped", n))
		case st.err != nil:
			clientStatusLines = append(clientStatusLines, fmt.Sprintf("%s=err(%v)", n, st.err))
		case st.confirmed:
			clientStatusLines = append(clientStatusLines, fmt.Sprintf("%s=ok(workers=%d/%d)", n, st.workersBound, st.workersTotal))
		case st.expectedCores > 0:
			clientStatusLines = append(clientStatusLines, fmt.Sprintf("%s=workers-mismatch(bound=%d expected=%d total=%d)", n, st.workersBound, st.expectedCores, st.workersTotal))
		default:
			clientStatusLines = append(clientStatusLines, fmt.Sprintf("%s=no-workers-bound(%d/%d)", n, st.workersBound, st.workersTotal))
		}
	}

	return fmt.Errorf(
		"enrollment monitor: timeout after %s; archive confirmed=%d/%d (need %d) | per-client (archive-side): %v | per-archive: %v | per-client (worker-side): %v",
		em.timeout, confirmed, len(em.archives), em.minArchives, clientLines, archiveSummaries, clientStatusLines)
}
