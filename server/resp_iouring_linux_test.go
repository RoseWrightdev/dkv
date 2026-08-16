//go:build linux

package server

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/rosewrightdev/oryx"
)

func TestIOUringServer_GetSetDeletePing(t *testing.T) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(t.TempDir() + "/wal").
		SetSnpPath(t.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		t.Fatalf("failed to build db engine: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	port := 35000 + rand.Intn(10000)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := NewRESPServer(eng, addr)
	go func() {
		_ = srv.RunIOUring()
	}()
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect to io_uring server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Test PING
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		t.Fatalf("failed to write PING: %v", err)
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read PING response: %v", err)
	}
	if resp != "+PONG\r\n" {
		t.Fatalf("expected +PONG\\r\\n, got %q", resp)
	}

	// Test SET
	_, err = conn.Write([]byte("*3\r\n$3\r\nSET\r\n$4\r\nmykey\r\n$7\r\nmyvalue\r\n"))
	if err != nil {
		t.Fatalf("failed to write SET: %v", err)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read SET response: %v", err)
	}
	if resp != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n, got %q", resp)
	}

	// Test GET
	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$4\r\nmykey\r\n"))
	if err != nil {
		t.Fatalf("failed to write GET: %v", err)
	}
	resp, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read GET response len: %v", err)
	}
	if resp != "$7\r\n" {
		t.Fatalf("expected $7\\r\\n, got %q", resp)
	}
	val, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read GET value: %v", err)
	}
	if val != "myvalue\r\n" {
		t.Fatalf("expected myvalue\\r\\n, got %q", val)
	}
}
