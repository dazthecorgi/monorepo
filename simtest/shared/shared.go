package shared

import "fmt"

type NotificationType string

const (
	NotificationTypeTerminalFrame NotificationType = "terminal_frame_reached"
)

// NodeInfo holds per-node address and identity information.
type NodeInfo struct {
	Name        string `json:"name"`          // service name, e.g. "archive-1"
	IpAddress   string `json:"ip_address"`    // IP address, e.g. "172.20.1.10"
	StreamPort  int    `json:"stream_port"`   // TCP stream port, e.g. 8340
	PeerID      string `json:"peer_id"`       // base58-encoded peer ID, empty if unknown
	PeerPrivKey string `json:"peer_priv_key"` // hex-encoded Ed448 private key
}

func (n NodeInfo) StreamAddress() string { return fmt.Sprintf("%s:%d", n.IpAddress, n.StreamPort) }

type FrameNotification struct {
	RunID                 string           `json:"run_id,omitempty"`
	FrameNumber           uint64           `json:"frame_number"`
	Type                  NotificationType `json:"type"`
	SafetyError           string           `json:"safety_error,omitempty"`
	NodesReachedStopFrame int              `json:"nodes_reached_stop_frame"`
	TotalNodes            int              `json:"total_nodes"`
}
