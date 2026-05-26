# It needs the repo root as context

# syntax=docker.io/docker/dockerfile:1.7-labs
FROM ubuntu:24.04 AS builder

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

# Build the proxy binary
WORKDIR /build/simtest/proxy
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o proxy .

# Build p2p-ping
WORKDIR /build/go-libp2p/examples/p2p-ping
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o p2p-ping .

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

# Copy the proxy binary from builder
COPY --from=builder /build/simtest/proxy/proxy /app/proxy
COPY --from=builder /build/go-libp2p/examples/p2p-ping/p2p-ping /usr/local/bin/p2p-ping

# Make it executable
RUN chmod +x /app/proxy /usr/local/bin/p2p-ping

# Expose the default port
EXPOSE 8340

# Run the proxy
ENTRYPOINT ["/app/proxy"]
