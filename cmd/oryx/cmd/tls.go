package cmd

import (
	"crypto/tls"
	"fmt"

	"google.golang.org/grpc/credentials"
)

// loadClientTLS builds transport credentials from a certificate file.
func loadClientTLS(certFile, keyFile string) (credentials.TransportCredentials, error) {
	if keyFile != "" {
		// mTLS: load a client certificate + key pair
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client TLS key pair: %w", err)
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		return credentials.NewTLS(tlsCfg), nil
	}

	// Server-cert-only: use system pool + override ServerName from file
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	return credentials.NewTLS(tlsCfg), nil
}
