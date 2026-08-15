// Package gateway implements the gRPC API gateway and client routing cache.
package gateway

import (
	"context"
	"time"

	pb "github.com/rosewrightdev/oryx/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client represents a gRPC client to interact with the oryx service.
type Client struct {
	conn    *grpc.ClientConn
	API     pb.OryxServiceClient
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
		API:     pb.NewOryxServiceClient(conn),
		timeout: timeout,
	}, nil
}

// NewInsecureClient initializes a new Client using insecure credentials.
// Callers opt in explicitly by using this constructor; no additional warning is emitted.
func NewInsecureClient(addr string, timeout time.Duration) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		API:     pb.NewOryxServiceClient(conn),
		timeout: timeout,
	}, nil
}

// Get retrieves a value by key.
func (c *Client) Get(key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.API.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}

	return resp.Value, resp.Exists, nil
}

// Set stores a key-value pair.
func (c *Client) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.API.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	return err
}

// Delete removes a key from the store.
// Returns true if the key existed and was deleted, false if the key was not found.
func (c *Client) Delete(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.API.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		return false, err
	}
	return resp.Existed, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
