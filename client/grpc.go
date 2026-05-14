package client

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn    *grpc.ClientConn
	api     pb.DkvServiceClient
	timeout time.Duration
}

func NewClient(addr string, timeout time.Duration, creds credentials.TransportCredentials) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		api:     pb.NewDkvServiceClient(conn),
		timeout: timeout,
	}, nil
}

func NewInsecureClient(addr string, timeout time.Duration) (*Client, error) {
	slog.Warn("Using `insecure.NewCredentials()`. See google.golang.org/grpc/credentials/insecure")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		api:     pb.NewDkvServiceClient(conn),
		timeout: timeout,
	}, nil
}

// Get retrieves a value by key.
func (c *Client) Get(key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.api.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}

	return resp.Value, resp.Exists, nil
}

// Set stores a key-value pair.
func (c *Client) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.api.Set(ctx, &pb.SetRequest{Key: key, Value: value})
	return err
}

// Delete removes a key from the store.
func (c *Client) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err := c.api.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
