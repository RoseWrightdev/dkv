package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/panjf2000/gnet/v2"
	"github.com/rosewrightdev/oryx"
	"github.com/rosewrightdev/oryx/kv"
)

var (
	respOK            = []byte("+OK\r\n")
	respPONG          = []byte("+PONG\r\n")
	respNil           = []byte("$-1\r\n")
	respZeroInt       = []byte(":0\r\n")
	respOneInt        = []byte(":1\r\n")
	respErrCmd        = []byte("-ERR unknown command\r\n")
	respErrArgs       = []byte("-ERR wrong number of arguments\r\n")
	respErrBadRequest = []byte("-ERR bad request\r\n")
	crlfBytes         = []byte("\r\n")
)

type connState struct {
	argBuf [][]byte
	outBuf []byte
}

type RESPServer struct {
	*gnet.BuiltinEventEngine
	eng      oryx.Database
	addr     string
	stopOnce sync.Once
	mu       sync.Mutex
	bound    string
}

func NewRESPServer(eng oryx.Database, addr string) *RESPServer {
	return &RESPServer{
		eng:  eng,
		addr: addr,
	}
}

func newGnetServer(eng oryx.Database, addr string) *RESPServer {
	return NewRESPServer(eng, addr)
}

func (s *RESPServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound != "" {
		return s.bound
	}
	return s.addr
}

func (s *RESPServer) OnBoot(eng gnet.Engine) gnet.Action {
	runtime.LockOSThread()
	s.mu.Lock()
	s.bound = s.addr
	s.mu.Unlock()
	return gnet.None
}

func (s *RESPServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	c.SetContext(&connState{
		argBuf: make([][]byte, 0, 8),
		outBuf: make([]byte, 0, 16384),
	})
	return
}

func (s *RESPServer) OnClose(c gnet.Conn, err error) gnet.Action {
	return gnet.None
}

func (s *RESPServer) OnTraffic(c gnet.Conn) gnet.Action {
	state, _ := c.Context().(*connState)
	if state == nil {
		state = &connState{
			argBuf: make([][]byte, 0, 8),
			outBuf: make([]byte, 0, 16384),
		}
		c.SetContext(state)
	}

	data, _ := c.Next(-1)
	state.outBuf = state.outBuf[:0]

	for len(data) > 0 {
		args, read, ok := parseRESPFrame(data, state.argBuf)
		if !ok {
			break
		}
		data = data[read:]
		state.outBuf = s.dispatchToBuffer(args, state.outBuf)
	}

	if len(state.outBuf) > 0 {
		_, _ = c.Write(state.outBuf)
	}

	return gnet.None
}

func stringFromBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (s *RESPServer) dispatchToBuffer(args [][]byte, out []byte) []byte {
	if len(args) == 0 {
		return append(out, "-ERR empty command\r\n"...)
	}

	cmd := args[0]
	switch len(cmd) {
	case 3:
		if bytes.EqualFold(cmd, []byte("GET")) {
			if len(args) != 2 {
				return append(out, respErrArgs...)
			}
			val, ok := s.eng.Get(kv.Key(stringFromBytes(args[1])))
			if !ok {
				return append(out, respNil...)
			}
			return appendBulkBytes(out, val)
		}
		if bytes.EqualFold(cmd, []byte("SET")) {
			if len(args) != 3 {
				return append(out, respErrArgs...)
			}
			if err := s.eng.Set(kv.Key(stringFromBytes(args[1])), args[2]); err != nil {
				return append(out, fmt.Sprintf("-ERR %v\r\n", err)...)
			}
			return append(out, respOK...)
		}
		if bytes.EqualFold(cmd, []byte("DEL")) {
			if len(args) < 2 {
				return append(out, respErrArgs...)
			}
			count := 0
			for _, keyBytes := range args[1:] {
				existed, err := s.eng.Delete(kv.Key(stringFromBytes(keyBytes)))
				if err != nil {
					return append(out, fmt.Sprintf("-ERR %v\r\n", err)...)
				}
				if existed {
					count++
				}
			}
			return appendInt(out, count)
		}

	case 4:
		if bytes.EqualFold(cmd, []byte("PING")) {
			if len(args) > 1 {
				return appendBulkBytes(out, args[1])
			}
			return append(out, respPONG...)
		}
		if bytes.EqualFold(cmd, []byte("ECHO")) {
			if len(args) < 2 {
				return append(out, respNil...)
			}
			return appendBulkBytes(out, args[1])
		}
		if bytes.EqualFold(cmd, []byte("QUIT")) {
			return append(out, respOK...)
		}

	case 6:
		if bytes.EqualFold(cmd, []byte("EXISTS")) {
			if len(args) != 2 {
				return append(out, respErrArgs...)
			}
			_, ok := s.eng.Get(kv.Key(args[1]))
			if ok {
				return append(out, respOneInt...)
			}
			return append(out, respZeroInt...)
		}
	}

	return append(out, respErrCmd...)
}

func appendBulkBytes(out []byte, val []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(val)), 10)
	out = append(out, crlfBytes...)
	out = append(out, val...)
	out = append(out, crlfBytes...)
	return out
}

func appendInt(out []byte, val int) []byte {
	out = append(out, ':')
	out = strconv.AppendInt(out, int64(val), 10)
	out = append(out, crlfBytes...)
	return out
}

func parseRESPFrame(data []byte, argBuf [][]byte) ([][]byte, int, bool) {
	if len(data) == 0 {
		return nil, 0, false
	}
	if data[0] != '*' {
		return nil, 0, false
	}

	idx := bytes.IndexByte(data, '\n')
	if idx < 0 || idx < 2 || data[idx-1] != '\r' {
		return nil, 0, false
	}

	numArgs, err := parseFastInt(data[1 : idx-1])
	if err != nil || numArgs < 0 {
		return nil, 0, false
	}

	read := idx + 1
	argBuf = argBuf[:0]

	for range numArgs {
		if read >= len(data) || data[read] != '$' {
			return nil, 0, false
		}
		crlfIdx := bytes.IndexByte(data[read:], '\n')
		if crlfIdx < 0 || crlfIdx < 2 || data[read+crlfIdx-1] != '\r' {
			return nil, 0, false
		}

		bulkLen, err := parseFastInt(data[read+1 : read+crlfIdx-1])
		if err != nil || bulkLen < 0 {
			return nil, 0, false
		}

		bulkStart := read + crlfIdx + 1
		bulkEnd := bulkStart + bulkLen
		if bulkEnd+2 > len(data) {
			return nil, 0, false
		}

		if data[bulkEnd] != '\r' || data[bulkEnd+1] != '\n' {
			return nil, 0, false
		}

		argBuf = append(argBuf, data[bulkStart:bulkEnd])
		read = bulkEnd + 2
	}

	return argBuf, read, true
}

func parseFastInt(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty int")
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid digit")
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func (s *RESPServer) Run() error {
	addr := s.addr
	if strings.HasSuffix(addr, ":0") || addr == ":0" {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		time.Sleep(10 * time.Millisecond)
		addr = fmt.Sprintf("127.0.0.1:%d", port)
	}

	s.mu.Lock()
	s.bound = addr
	s.mu.Unlock()

	protoAddr := "tcp://" + addr
	return gnet.Run(s, protoAddr,
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
	)
}

func (s *RESPServer) Stop() {
	s.stopOnce.Do(func() {
		protoAddr := "tcp://" + s.addr
		_ = gnet.Stop(context.Background(), protoAddr)
	})
}
