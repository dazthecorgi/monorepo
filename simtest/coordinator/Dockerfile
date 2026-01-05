# It needs the repo root as context

# syntax=docker.io/docker/dockerfile:1.7-labs
FROM --platform=${TARGETPLATFORM} ubuntu:24.04 AS builder

ARG GO_VERSION=1.24.9

RUN apt update && apt install -y make git wget && \
    ARCH=$(dpkg --print-architecture) && \
    case ${ARCH} in \
    amd64) GOARCH=amd64 ;; \
    arm64) GOARCH=arm64 ;; \
    *) echo "Unsupported architecture: ${ARCH}" && exit 1 ;; \
    esac && \
    wget https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-${GOARCH}.tar.gz && \
    rm go${GO_VERSION}.linux-${GOARCH}.tar.gz

ENV PATH=$PATH:/usr/local/go/bin
ENV GOPROXY=direct
ENV GOMODCACHE=/go/pkg/mod
ENV GOCACHE=/root/.cache/go-build

WORKDIR /build

COPY . .

# Build the coordinator binary
WORKDIR /build/simtest/coordinator
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o coordinator main.go

# Final stage
FROM ubuntu:24.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    # nc is used for health checks
    netcat-traditional \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the coordinator binary from builder
COPY --from=builder /build/simtest/coordinator/coordinator /app/coordinator

# Make it executable
RUN chmod +x /app/coordinator

# Expose the default port
EXPOSE 8340

# Run the coordinator
ENTRYPOINT ["/app/coordinator"]
