package cmd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateTestCertAndKey(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Oryx Test"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPath := filepath.Join(dir, "test.crt")
	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	defer certOut.Close()
	require.NoError(t, pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))

	keyPath := filepath.Join(dir, "test.key")
	keyOut, err := os.Create(keyPath)
	require.NoError(t, err)
	defer keyOut.Close()
	privBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes}))

	return certPath, keyPath
}

func TestBuildClientTLSConfig_CertOnly(t *testing.T) {
	tmpDir := t.TempDir()
	certFile, _ := generateTestCertAndKey(t, tmpDir)

	tlsCfg, err := buildClientTLSConfig(certFile, "")
	require.NoError(t, err)
	assert.NotNil(t, tlsCfg)
	assert.NotNil(t, tlsCfg.RootCAs, "RootCAs should be populated when certFile is provided without keyFile")
	assert.Empty(t, tlsCfg.Certificates, "Client certificates should be empty for cert-only mode")
}

func TestBuildClientTLSConfig_mTLS(t *testing.T) {
	tmpDir := t.TempDir()
	certFile, keyFile := generateTestCertAndKey(t, tmpDir)

	tlsCfg, err := buildClientTLSConfig(certFile, keyFile)
	require.NoError(t, err)
	assert.NotNil(t, tlsCfg)
	assert.Len(t, tlsCfg.Certificates, 1, "Client certificate should be loaded for mTLS mode")
}
