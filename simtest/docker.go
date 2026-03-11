package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"gopkg.in/yaml.v3"
	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)

// getArchiveServices discovers archive services from docker-compose.yml
// and returns a list of NodeInfo for each archive node.
func getArchiveServices(ctx context.Context, workDir string) ([]shared.NodeInfo, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "config", "--services")
	cmd.Dir = workDir

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list docker compose services: %w", err)
	}

	services := strings.Split(strings.TrimSpace(string(output)), "\n")
	var serviceNames []string

	for _, service := range services {
		service = strings.TrimSpace(service)
		if strings.HasPrefix(service, "archive-") {
			serviceNames = append(serviceNames, service)
		}
	}

	if len(serviceNames) == 0 {
		return nil, fmt.Errorf("no archive node addresses found")
	}
	sort.Strings(serviceNames)

	identities, err := resolveNodeIdentities(workDir, serviceNames)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve node identities: %w", err)
	}

	nodes := make([]shared.NodeInfo, len(serviceNames))
	for i, name := range serviceNames {
		port, err := resolveNodeStreamPort(workDir, name)
		if err != nil {
			return nil, err
		}
		id := identities[name]
		nodes[i] = shared.NodeInfo{
			Name:        name,
			Hostname:    name,
			StreamPort:  port,
			PeerID:      id.PeerID,
			PeerPrivKey: id.PeerPrivKey,
		}
	}

	return nodes, nil
}

// nodeConfigYAML is a minimal struct for unmarshalling the fields we need from config.yml.
type nodeConfigYAML struct {
	P2P struct {
		PeerPrivKey           string `yaml:"peerPrivKey"`
		StreamListenMultiaddr string `yaml:"streamListenMultiaddr"`
	} `yaml:"p2p"`
}

// nodeIdentity holds the peer ID and raw private key for a node.
type nodeIdentity struct {
	PeerID      string
	PeerPrivKey string
}

// resolveNodeIdentities derives the peer ID and preserves the hex-encoded
// private key for each named node from its config.yml.
func resolveNodeIdentities(execDir string, nodeNames []string) (map[string]nodeIdentity, error) {
	result := make(map[string]nodeIdentity, len(nodeNames))
	for _, name := range nodeNames {
		name = strings.TrimSpace(name)
		configFile := filepath.Join(execDir, "config", name+"-config", "config.yml")
		data, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config for node %s: %w", name, err)
		}
		var cfg nodeConfigYAML
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config for node %s: %w", name, err)
		}
		if cfg.P2P.PeerPrivKey == "" {
			return nil, fmt.Errorf("p2p.peerPrivKey not found in config for node %s", name)
		}
		rawKey, err := hex.DecodeString(cfg.P2P.PeerPrivKey)
		if err != nil {
			return nil, fmt.Errorf("failed to hex-decode peerPrivKey for node %s: %w", name, err)
		}
		privKey, err := libp2pcrypto.UnmarshalEd448PrivateKey(rawKey)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal peerPrivKey for node %s: %w", name, err)
		}
		pid, err := peer.IDFromPublicKey(privKey.GetPublic())
		if err != nil {
			return nil, fmt.Errorf("failed to derive peer ID for node %s: %w", name, err)
		}
		result[name] = nodeIdentity{
			PeerID:      pid.String(),
			PeerPrivKey: cfg.P2P.PeerPrivKey,
		}
	}
	return result, nil
}

// resolveNodeStreamPort reads the TCP port from p2p.streamListenMultiaddr in a node's config.yml.
// The expected format is e.g. "/ip4/0.0.0.0/tcp/8340/".
func resolveNodeStreamPort(execDir, name string) (int, error) {
	configFile := filepath.Join(execDir, "config", name+"-config", "config.yml")
	data, err := os.ReadFile(configFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read config for node %s: %w", name, err)
	}
	var cfg nodeConfigYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return 0, fmt.Errorf("failed to parse config for node %s: %w", name, err)
	}
	ma := cfg.P2P.StreamListenMultiaddr
	if ma == "" {
		return 0, fmt.Errorf("p2p.streamListenMultiaddr not found in config for node %s", name)
	}
	// Format: /ip4/0.0.0.0/tcp/8340/
	parts := strings.Split(ma, "/")
	for i, p := range parts {
		if p == "tcp" && i+1 < len(parts) {
			port, err := strconv.Atoi(parts[i+1])
			if err != nil {
				return 0, fmt.Errorf("invalid TCP port in streamListenMultiaddr for node %s: %w", name, err)
			}
			return port, nil
		}
	}
	return 0, fmt.Errorf("no TCP component in streamListenMultiaddr for node %s: %q", name, ma)
}

// executeTest executes "docker compose up" using CLI commands.
func executeTest(ctx context.Context, runId string, execDir string, bearerToken string, projectName string, stopFrame int, verbose bool, parallelRuns int, nodes []shared.NodeInfo, minimumNodes int, resolvedRankPartitions string) error {
	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}
	logger.Debugw("Found docker-compose.yml", "path", composePath, "project", projectName)

	nodeInfosJSON, err := json.Marshal(nodes)
	if err != nil {
		return fmt.Errorf("failed to serialize node infos: %w", err)
	}

	env := map[string]string{
		"RUN_ID":          runId,
		"RUNNER_AUTH":     bearerToken,
		"RUNNER_ADDRESS":  "host.docker.internal:" + strings.TrimPrefix(*listenPort, ":"),
		"STOP_FRAME":      fmt.Sprintf("%d", stopFrame),
		"NODE_INFOS":      string(nodeInfosJSON),
		"MIN_NODES":       fmt.Sprintf("%d", minimumNodes),
		"RANK_PARTITIONS": resolvedRankPartitions,
	}

	if err := dockerComposeUp(ctx, execDir, projectName, env, verbose, parallelRuns); err != nil {
		return fmt.Errorf("failed to start compose stack: %w", err)
	}

	return nil
}

// dockerComposeBuild executes "docker compose build" in the specified working directory.
func dockerComposeBuild(ctx context.Context, workDir string, verbose bool) error {
	cmd := exec.CommandContext(ctx, "docker", "compose", "build")
	cmd.Dir = workDir

	if verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose build failed: %w", err)
	}
	return nil
}

// dockerComposeUp executes "docker compose up" with environment variables.
// It waits for services to be healthy and removes orphaned containers.
// The --no-build flag is used since build is done separately.
func dockerComposeUp(ctx context.Context, workDir string, projectName string, env map[string]string, verbose bool, parallel int) error {
	logger.Debugw("Executing docker compose up", "project", projectName, "env", env)

	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "up",
		"-d",               // detached mode
		"--wait",           // wait for services to be healthy
		"--remove-orphans", // remove orphaned containers
		"--no-build",       // don't build images (already done separately)
	)
	cmd.Dir = workDir

	cmd.Env = make([]string, 0, len(env))
	for key, value := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
	}

	// Suppress output for parallel runs to avoid interleaving logs, but show for single runs if verbose
	if verbose && parallel == 1 {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose up failed: %w", err)
	}
	return nil
}

// dockerComposeProjectServices lists all service names for a running Docker Compose project.
func dockerComposeProjectServices(ctx context.Context, workDir string, projectName string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "ps", "--services")
	cmd.Dir = workDir
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker compose ps --services failed: %w", err)
	}
	var services []string
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if s := strings.TrimSpace(line); s != "" {
			services = append(services, s)
		}
	}
	return services, nil
}

// dockerComposeServiceLogs captures logs for a specific service in a Docker Compose project.
func dockerComposeServiceLogs(ctx context.Context, workDir string, projectName string, service string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "logs", "--no-color", service)
	cmd.Dir = workDir
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker compose logs for %s failed: %w", service, err)
	}
	return output, nil
}

// dockerComposeDown executes "docker compose down" with cleanup flags.
// It removes containers, networks, orphaned containers, and volumes.
func dockerComposeDown(ctx context.Context, workDir string, projectName string, verbose bool, parallelRuns int) error {
	logger.Debugw("Executing docker compose down", "project", projectName)

	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", projectName, "down",
		"--remove-orphans", // remove orphaned containers
		"--volumes",        // remove named volumes
	)
	cmd.Dir = workDir

	// Suppress output for parallel runs to avoid interleaving logs, but show for single runs if verbose
	if verbose && parallelRuns == 1 {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose down failed: %w", err)
	}
	return nil
}
