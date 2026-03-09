package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

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

	peerIDs, err := resolveNodePeerIDs(workDir, serviceNames)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve peer IDs: %w", err)
	}

	nodes := make([]shared.NodeInfo, len(serviceNames))
	for i, name := range serviceNames {
		port, err := resolveNodeStreamPort(workDir, name)
		if err != nil {
			return nil, err
		}
		nodes[i] = shared.NodeInfo{
			Name:       name,
			Hostname:   name,
			StreamPort: port,
			PeerID:     peerIDs[name],
		}
	}

	return nodes, nil
}

// resolveNodePeerIDs reads the peer ID for each named node from its config.yml comment
// and returns a map from node name to peer ID.
// Each config.yml starts with a line of the form: "# Peer id: QmXXX..."
func resolveNodePeerIDs(execDir string, nodeNames []string) (map[string]string, error) {
	result := make(map[string]string, len(nodeNames))
	for _, name := range nodeNames {
		name = strings.TrimSpace(name)
		configFile := filepath.Join(execDir, "config", name+"-config", "config.yml")
		data, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config for node %s: %w", name, err)
		}
		// First line format: "# Peer id: QmXXX..."
		firstLine := strings.SplitN(string(data), "\n", 2)[0]
		firstLine = strings.TrimSpace(firstLine)
		const prefix = "# Peer id: "
		if !strings.HasPrefix(firstLine, prefix) {
			return nil, fmt.Errorf("config for node %s does not contain peer ID on first line (expected '# Peer id: ...')", name)
		}
		peerID := strings.TrimSpace(strings.TrimPrefix(firstLine, prefix))
		if peerID == "" {
			return nil, fmt.Errorf("empty peer ID in config for node %s", name)
		}
		result[name] = peerID
	}
	return result, nil
}

// resolveNodeStreamPort reads the TCP port from streamListenMultiaddr in a node's config.yml.
// The expected format is e.g. "/ip4/0.0.0.0/tcp/8340/".
func resolveNodeStreamPort(execDir, name string) (int, error) {
	configFile := filepath.Join(execDir, "config", name+"-config", "config.yml")
	data, err := os.ReadFile(configFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read config for node %s: %w", name, err)
	}
	const key = "streamListenMultiaddr:"
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, key) {
			continue
		}
		ma := strings.TrimSpace(strings.TrimPrefix(trimmed, key))
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
	return 0, fmt.Errorf("streamListenMultiaddr not found in config for node %s", name)
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
