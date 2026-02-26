package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// getArchiveServices discovers archive services from docker-compose.yml
// and returns a list of node addresses (e.g., "archive-1:8340").
func getArchiveServices(ctx context.Context, workDir string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "config", "--services")
	cmd.Dir = workDir

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list docker compose services: %w", err)
	}

	services := strings.Split(strings.TrimSpace(string(output)), "\n")
	var archiveAddresses []string

	for _, service := range services {
		service = strings.TrimSpace(service)
		if strings.HasPrefix(service, "archive-") {
			// Archive nodes expose global consensus gRPC on port 8340
			archiveAddresses = append(archiveAddresses, fmt.Sprintf("%s:8340", service))
		}
	}

	if len(archiveAddresses) == 0 {
		return nil, fmt.Errorf("no archive node addresses found")
	}

	return archiveAddresses, nil
}

// resolveNodePeerIDs reads the peer ID for each named node from its config.yml comment.
// Each config.yml starts with a line of the form: "# Peer id: QmXXX..."
func resolveNodePeerIDs(execDir string, nodeNames []string) ([]string, error) {
	var peerIDs []string
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
		peerIDs = append(peerIDs, peerID)
	}
	return peerIDs, nil
}

// executeTest executes "docker compose up" using CLI commands.
func executeTest(ctx context.Context, runId string, execDir string, bearerToken string, projectName string, stopFrame int, verbose bool, parallelRuns int, nodeAddresses []string, minimumNodes int, resolvedRankPartitions string) error {
	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}
	logger.Debugw("Found docker-compose.yml", "path", composePath, "project", projectName)

	env := map[string]string{
		"RUN_ID":          runId,
		"RUNNER_AUTH":     bearerToken,
		"RUNNER_ADDRESS":  "host.docker.internal:" + strings.TrimPrefix(*listenPort, ":"),
		"STOP_FRAME":      fmt.Sprintf("%d", stopFrame),
		"NODE_ADDRESSES":  strings.Join(nodeAddresses, ","),
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
