package shared

import (
	"fmt"
	"strconv"
	"strings"
)

type NotificationType string

const (
	NotificationTypeTerminalFrame NotificationType = "terminal_frame_reached"
	NotificationTypeGlobalTimeout NotificationType = "global_timeout"
)

// NodeInfo holds per-node address and identity information.
type NodeInfo struct {
	Name        string `json:"name"`          // service name, e.g. "archive-1"
	Hostname    string `json:"hostname"`      // hostname, e.g. "archive-1"
	StreamPort  int    `json:"stream_port"`   // TCP stream port, e.g. 8340
	PeerID      string `json:"peer_id"`       // base58-encoded peer ID, empty if unknown
	PeerPrivKey string `json:"peer_priv_key"` // hex-encoded Ed448 private key
}

func (n NodeInfo) StreamAddress() string { return fmt.Sprintf("%s:%d", n.Hostname, n.StreamPort) }

// Ordinal returns the numeric suffix of the service name (e.g. "archive-3" → 3).
// Returns an error if the name has no numeric suffix.
func (n NodeInfo) Ordinal() (int, error) {
	parts := strings.Split(n.Name, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

type FrameNotification struct {
	RunID                 string           `json:"run_id,omitempty"`
	StopFrame           uint64           `json:"frame_number"`
	Type                  NotificationType `json:"type"`
	SafetyError           string           `json:"safety_error,omitempty"`
	NodesReachedStopFrame int              `json:"nodes_reached_stop_frame"`
	TotalNodes            int              `json:"total_nodes"`
}
