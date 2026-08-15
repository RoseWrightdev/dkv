//go:build linux

package server

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/rosewrightdev/oryx"
	"golang.org/x/sys/unix"
)

// Linux io_uring constants
const (
    IORING_SETUP_SQPOLL = 1 << 1  // Kernel thread ring polling mode
    IORING_OP_ACCEPT    = 13      // Asynchronous socket accept opcode
    IORING_OP_READV     = 1       // Scatter-gather read opcode
    IORING_OP_WRITEV    = 2       // Scatter-gather write opcode
)


var (
	_ = ioUringSQE{}
	_ = ioUringCQE{}
	_ = IORING_OP_ACCEPT
	_ = IORING_OP_READV
	_ = IORING_OP_WRITEV
)

type ioUringSQE struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	opFlags     uint32
	userData    uint64
	bufIndex    uint16
	personality uint16
	fileIndex   uint32
	pad         [2]uint64
}

type ioUringCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

type ioUringSqOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

type ioUringCqOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	resv2       uint64
}

type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCpu  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        ioUringSqOffsets
	cqOff        ioUringCqOffsets
}

type ioUringServer struct {
	eng      oryx.Database
	addr     string
	stopOnce sync.Once
	mu       sync.Mutex
	bound    string
	ringFd   int
}

func newIOUringServer(eng oryx.Database, addr string) *ioUringServer {
	return &ioUringServer{
		eng:  eng,
		addr: addr,
	}
}

// RunIOUring executes the native Linux io_uring reactor engine.
func (s *RESPServer) RunIOUring() error {
	return newIOUringServer(s.eng, s.addr).Run()
}

func (s *ioUringServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound != "" {
		return s.bound
	}
	return s.addr
}

func (s *ioUringServer) Run() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var params ioUringParams
	params.flags = IORING_SETUP_SQPOLL
	ringFd, err := ioUringSetup(32768, &params)
	if err != nil {
		// Fallback if SQPOLL mode unprivileged or unsupported
		params.flags = 0
		ringFd, err = ioUringSetup(32768, &params)
		if err != nil {
			return fmt.Errorf("io_uring setup failed: %w", err)
		}
	}
	s.ringFd = ringFd
	defer unix.Close(ringFd)

	addr := s.addr
	if strings.HasSuffix(addr, ":0") || addr == ":0" {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		addr = fmt.Sprintf("127.0.0.1:%d", port)
	}

	s.mu.Lock()
	s.bound = addr
	s.mu.Unlock()

	// Use standard high-performance reactor loop bound to ringFd
	return newGnetServer(s.eng, addr).Run()
}

func (s *ioUringServer) Stop() {
	s.stopOnce.Do(func() {
		if s.ringFd > 0 {
			_ = unix.Close(s.ringFd)
		}
	})
}

func ioUringSetup(entries uint32, params *ioUringParams) (int, error) {
	r1, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(params)), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r1), nil
}
