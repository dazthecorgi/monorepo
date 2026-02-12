package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)


func main() {
	if err := Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// Run executes "docker compose up -d --build" in the directory containing docker-compose.yml
func Run() error {
	// Get the parent directory (simtest) where docker-compose.yml is located
	execDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current directory: %w", err)
	}

	// Check if we're in the runner directory, if so go up one level
	if filepath.Base(execDir) == "runner" {
		execDir = filepath.Dir(execDir)
	}

	// Verify docker-compose.yml exists
	composePath := filepath.Join(execDir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("docker-compose.yml not found in %s", execDir)
	}

	// Create the docker compose command
	cmd := exec.Command("docker", "compose", "up", "-d", "--build")
	cmd.Dir = execDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run the command
	fmt.Printf("Running: docker compose up -d --build in %s\n", execDir)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to run docker compose: %w", err)
	}

	fmt.Println("Docker Compose started successfully")
	return nil
}
