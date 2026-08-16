package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

func buildClientTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	if keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client TLS key pair: %w", err)
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}, nil
	}

	var rootCAs *x509.CertPool
	if certFile != "" {
		caCert, err := os.ReadFile(certFile)
		if err != nil {
			return nil, fmt.Errorf("reading TLS cert file: %w", err)
		}
		rootCAs = x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append certs from %s", certFile)
		}
	}

	return &tls.Config{
		RootCAs:    rootCAs,
		MinVersion: tls.VersionTLS12,
	}, nil
}

// loadClientTLS builds transport credentials from a certificate file.
func loadClientTLS(certFile, keyFile string) (credentials.TransportCredentials, error) {
	tlsCfg, err := buildClientTLSConfig(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
}
