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
	_, err = conn.Write([]byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"))
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
	_, err = conn.Write([]byte("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"))
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

// TestIOUringServer_SplitFrame verifies that a RESP command sent across two
// separate writes (simulating a command straddling two TCP segments / two
// separate READV completions) is still parsed correctly, rather than being
// silently corrupted by the fixed-size per-read scratch buffer.
func TestIOUringServer_SplitFrame(t *testing.T) {
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

	port := 39000 + rand.Intn(10000)
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

	cmd := []byte("*3\r\n$3\r\nSET\r\n$6\r\nsplitk\r\n$9\r\nsplitval!\r\n")
	split := len(cmd) / 2

	if _, err := conn.Write(cmd[:split]); err != nil {
		t.Fatalf("failed to write first half of SET: %v", err)
	}
	// Give the server a chance to process the first, incomplete half as its
	// own READV completion before the rest arrives.
	time.Sleep(20 * time.Millisecond)
	if _, err := conn.Write(cmd[split:]); err != nil {
		t.Fatalf("failed to write second half of SET: %v", err)
	}

	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read SET response: %v", err)
	}
	if resp != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n, got %q", resp)
	}

	if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$6\r\nsplitk\r\n")); err != nil {
		t.Fatalf("failed to write GET: %v", err)
	}
	respLen, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read GET response len: %v", err)
	}
	if respLen != "$9\r\n" {
		t.Fatalf("expected $9\\r\\n, got %q", respLen)
	}
	val, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read GET value: %v", err)
	}
	if val != "splitval!\r\n" {
		t.Fatalf("expected splitval!\\r\\n, got %q", val)
	}
}

func BenchmarkIOUringServer_Get(b *testing.B) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	_ = eng.Set("benchkey", []byte("benchvalue"))

	port := 36000 + rand.Intn(10000)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := NewRESPServer(eng, addr)
	go func() {
		_ = srv.RunIOUring()
	}()
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	cmd := []byte("*2\r\n$3\r\nGET\r\n$8\r\nbenchkey\r\n")
	buf := make([]byte, 256)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(cmd); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := conn.Read(buf); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkIOUringServer_Set(b *testing.B) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	port := 37000 + rand.Intn(10000)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := NewRESPServer(eng, addr)
	go func() {
		_ = srv.RunIOUring()
	}()
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	cmd := []byte("*3\r\n$3\r\nSET\r\n$8\r\nbenchkey\r\n$10\r\nbenchvalue\r\n")
	buf := make([]byte, 256)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(cmd); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := conn.Read(buf); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

func BenchmarkIOUringServer_Get_Parallel(b *testing.B) {
	eng, err := oryx.NewDatabaseBuilder().Default().
		SetWalPath(b.TempDir() + "/wal").
		SetSnpPath(b.TempDir() + "/snapshot.bin").
		SetInsecure().
		SingleNode().
		Build()
	if err != nil {
		b.Fatalf("build database: %v", err)
	}
	eng.Start()
	defer eng.Stop()

	_ = eng.Set("benchkey", []byte("benchvalue"))

	port := 38000 + rand.Intn(10000)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := NewRESPServer(eng, addr)
	go func() {
		_ = srv.RunIOUring()
	}()
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	cmd := []byte("*2\r\n$3\r\nGET\r\n$8\r\nbenchkey\r\n")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", srv.Addr())
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		buf := make([]byte, 256)

		for pb.Next() {
			if _, err := conn.Write(cmd); err != nil {
				return
			}
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	})
}
