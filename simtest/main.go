package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/testcontainers/testcontainers-go/modules/compose"
)

func main() {
	if err := Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// Run executes "docker compose up -d --build" using testcontainers compose module
func Run() error {
	ctx := context.Background()

	// Get the directory where docker-compose.yml is located
	execDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %w", err)
	}

	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}

	fmt.Printf("Running: docker compose up -d --build in %s\n", execDir)

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
