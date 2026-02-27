package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)

type testConfig struct {
	RunID          string                      `yaml:"run_id"`
	StopFrame      int                         `yaml:"stop_frame"`
	NodeAddresses  []string                    `yaml:"node_addresses"`
	MinimumNodes   int                         `yaml:"minimum_nodes"`
	RankPartitions []shared.RankPartitionEntry `yaml:"rank_partitions,omitempty"`
}

type testResultOutput struct {
	RunID        string `yaml:"run_id"`
	Success      bool   `yaml:"success"`
	ErrorMessage string `yaml:"error_message,omitempty"`
}

// saveFailureArtifacts writes test config, result, and per-service logs to <outDir>/<runID>/.
// It must be called before docker compose down so that service logs are still available.
func saveFailureArtifacts(outDir, runID, projectName, execDir string, result TestResult, cfg testConfig) {
	runDir := filepath.Join(outDir, runID)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		logger.Errorw("Failed to create artifact directory", "error", err, "dir", runDir)
		return
	}

	if data, err := yaml.Marshal(cfg); err != nil {
		logger.Errorw("Failed to marshal test config", "error", err, "run_id", runID)
	} else if err := os.WriteFile(filepath.Join(runDir, "config.yaml"), data, 0644); err != nil {
		logger.Errorw("Failed to write config artifact", "error", err, "run_id", runID)
	}

	out := testResultOutput{
		RunID:        result.RunID,
		Success:      result.Success,
		ErrorMessage: result.ErrorMessage,
	}
	if data, err := yaml.Marshal(out); err != nil {
		logger.Errorw("Failed to marshal test result", "error", err, "run_id", runID)
	} else if err := os.WriteFile(filepath.Join(runDir, "result.yaml"), data, 0644); err != nil {
		logger.Errorw("Failed to write result artifact", "error", err, "run_id", runID)
	}

	saveServiceLogs(runDir, runID, projectName, execDir)

	logger.Infow("Saved failure artifacts", "dir", runDir, "run_id", runID)
}

// saveServiceLogs captures logs for each service and writes them to separate files under logs/.
func saveServiceLogs(runDir, runID, projectName, execDir string) {
	logsDir := filepath.Join(runDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		logger.Errorw("Failed to create logs directory", "error", err, "dir", logsDir)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	services, err := dockerComposeProjectServices(ctx, execDir, projectName)
	if err != nil {
		logger.Errorw("Failed to list services", "error", err, "run_id", runID)
		return
	}

	for _, service := range services {
		data, err := dockerComposeServiceLogs(ctx, execDir, projectName, service)
		if err != nil {
			logger.Errorw("Failed to capture service logs", "error", err, "run_id", runID, "service", service)
			continue
		}
		logFile := filepath.Join(logsDir, service+".log")
		if err := os.WriteFile(logFile, data, 0644); err != nil {
			logger.Errorw("Failed to write service log", "error", err, "run_id", runID, "service", service)
		}
	}
}
