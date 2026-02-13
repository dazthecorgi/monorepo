package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

var workingDir = flag.String(
	"dir",
	"",
	"working directory containing docker-compose.yml (defaults to current directory)",
)

func main() {
	flag.Parse()

	// Create uuid for this test run
	runId := uuid.New().String()
	fmt.Printf("Test run ID: %s\n", runId)

	// Get the directory where docker-compose.yml is located
	execDir := *workingDir
	if execDir == "" {
		// Fall back to current working directory
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get current directory: %v\n", err)
			os.Exit(1)
		}
		execDir = cwd
	}

	fmt.Printf("Working directory: %s\n", execDir)

	if err := Run(runId, execDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// Run executes "docker compose up -d --build" using testcontainers compose module
func Run(runId string, execDir string) error {
	ctx := context.Background()

	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}

	// Create compose stack
	composeStack, err := compose.NewDockerCompose(composePath)
	if err != nil {
		return fmt.Errorf("failed to create compose stack: %w", err)
	}

	// Start all services - testcontainers will build automatically
	err = composeStack.Up(ctx, compose.Wait(true))
	if err != nil {
		return fmt.Errorf("failed to run docker compose up: %w", err)
	}

	defer func() {
        err = composeStack.Down(
            context.Background(),
            compose.RemoveOrphans(true),
            compose.RemoveVolumes(true),
        )
        if err == nil {
			fmt.Println("Compose stack stopped successfully")
        } else {
            fmt.Printf("Failed to stop compose stack: %v", err)
		}
    }()

	fmt.Println("Docker Compose started successfully")
	return nil
}
