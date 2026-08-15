package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/rosewrightdev/oryx"
	"github.com/rosewrightdev/oryx/kv"
)

// RESPServer exposes a lightweight Redis-compatible RESP endpoint directly to the
// key-value engine, skipping the gRPC serialization layer on the hot path.
type RESPServer struct {
	eng      oryx.Database
	addr     string
	listener net.Listener
	stop     chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
}

// NewRESPServer creates a RESP server bound to the provided address.
func NewRESPServer(eng oryx.Database, addr string) *RESPServer {
	return &RESPServer{
		eng:  eng,
		addr: addr,
		stop: make(chan struct{}),
	}
}

// Addr returns the listening address once the server starts.
func (s *RESPServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Run starts the RESP listener and serves connections.
func (s *RESPServer) Run() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return nil
			default:
				return err
			}
		}
		go s.handleConn(conn)
	}
}

// Stop shuts down the listener and closes active connections.
func (s *RESPServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stop)
		s.mu.Lock()
		if s.listener != nil {
			_ = s.listener.Close()
			s.listener = nil
		}
		s.mu.Unlock()
	})
}

func (s *RESPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		cmd, err := readRESPArray(reader)
		if err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			if _, writeErr := conn.Write([]byte("-ERR bad request\r\n")); writeErr != nil {
				return
			}
			continue
		}
		if len(cmd) == 0 {
			continue
		}

		resp, err := s.dispatch(cmd)
		if err != nil {
			if _, werr := conn.Write([]byte("-ERR " + err.Error() + "\r\n")); werr != nil {
				return
			}
			continue
		}
		if _, werr := conn.Write(resp); werr != nil {
			return
		}
	}
}

func (s *RESPServer) dispatch(args []string) ([]byte, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		if len(args) > 1 {
			return bulkString(args[1]), nil
		}
		return []byte("+PONG\r\n"), nil
	case "ECHO":
		if len(args) < 2 {
			return []byte("$-1\r\n"), nil
		}
		return bulkString(args[1]), nil
	case "SET":
		if len(args) != 3 {
			return nil, fmt.Errorf("wrong number of arguments for SET")
		}
		if err := s.eng.Set(kv.Key(args[1]), []byte(args[2])); err != nil {
			return nil, err
		}
		return []byte("+OK\r\n"), nil
	case "GET":
		if len(args) != 2 {
			return nil, fmt.Errorf("wrong number of arguments for GET")
		}
		val, ok := s.eng.Get(kv.Key(args[1]))
		if !ok {
			return []byte("$-1\r\n"), nil
		}
		return bulkString(string(val)), nil
	case "DEL":
		if len(args) < 2 {
			return nil, fmt.Errorf("wrong number of arguments for DEL")
		}
		count := 0
		for _, key := range args[1:] {
			existed, err := s.eng.Delete(kv.Key(key))
			if err != nil {
				return nil, err
			}
			if existed {
				count++
			}
		}
		return integer(count), nil
	case "EXISTS":
		if len(args) != 2 {
			return nil, fmt.Errorf("wrong number of arguments for EXISTS")
		}
		_, ok := s.eng.Get(kv.Key(args[1]))
		if ok {
			return integer(1), nil
		}
		return integer(0), nil
	case "QUIT":
		return []byte("+OK\r\n"), nil
	default:
		return nil, fmt.Errorf("unknown command: %s", cmd)
	}
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != '*' {
		return nil, fmt.Errorf("expected RESP array")
	}

	line, err := readRESPLine(r)
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(line)
	if err != nil || count < 0 {
		return nil, fmt.Errorf("invalid array length")
	}
	if count == 0 {
		return []string{}, nil
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		arg, err := readRESPBulk(r)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func readRESPBulk(r *bufio.Reader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	if b != '$' {
		return "", fmt.Errorf("expected RESP bulk string")
	}

	line, err := readRESPLine(r)
	if err != nil {
		return "", err
	}
	length, err := strconv.Atoi(line)
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length")
	}
	if length < 0 {
		return "", nil
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(r, make([]byte, 2)); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readRESPLine(r *bufio.Reader) (string, error) {
	var line bytes.Buffer
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\r' {
			next, err := r.ReadByte()
			if err != nil {
				return "", err
			}
			if next != '\n' {
				return "", fmt.Errorf("expected CRLF")
			}
			return line.String(), nil
		}
		line.WriteByte(b)
	}
}

func bulkString(value string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(value), value)
}

func integer(value int) []byte {
	return fmt.Appendf(nil, ":%d\r\n", value)
}
