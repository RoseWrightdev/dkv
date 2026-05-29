package gateway

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a gRPC client to interact with the dkv service.
type Client struct {
	conn    *grpc.ClientConn
	Api     pb.DkvServiceClient
	timeout time.Duration
}

// NewClient initializes a new Client using transport credentials.
func NewClient(addr string, timeout time.Duration, creds credentials.TransportCredentials) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		Api:     pb.NewDkvServiceClient(conn),
		timeout: timeout,
	}, nil
}

// NewInsecureClient initializes a new Client using insecure credentials.
func NewInsecureClient(addr string, timeout time.Duration) (*Client, error) {
	slog.Warn("Using `insecure.NewCredentials()`. See google.golang.org/grpc/credentials/insecure")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		Api:     pb.NewDkvServiceClient(conn),
		timeout: timeout,
	}, nil
}

// Get retrieves a value by key.
func (c *Client) Get(key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.Api.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}

	return resp.Value, resp.Exists, nil
}

// Set stores a key-value pair.
func (c *Client) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.Api.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	return err
}

// Delete removes a key from the store.
func (c *Client) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.Api.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
