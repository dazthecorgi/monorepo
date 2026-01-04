# Quilibrium Simulation Test Environment

This directory contains a Docker Compose setup for running multiple Quilibrium archive nodes in an isolated network for testing purposes.

## Quick Start

```
# Start
docker compose up -d --build --remove-orphans

# Check logs
docker compose logs -f

# Stop
docker compose down -v
```
