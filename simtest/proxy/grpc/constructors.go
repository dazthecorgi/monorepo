package grpc

import (
	"google.golang.org/grpc"
)

// NewServer returns a new grpc.Server with the given options.
func NewServer(opts ...grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(ServerOptions(opts...)...)
}

// NewClient returns a new grpc.ClientConn with the given target and options.
func NewClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, ClientOptions(opts...)...)
}
