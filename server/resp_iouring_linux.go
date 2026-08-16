//go:build linux

package server

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/rosewrightdev/oryx"
	"golang.org/x/sys/unix"
)

const (
	IORING_SETUP_SQPOLL     = 1 << 1
	IORING_FEAT_SINGLE_MMAP = 1 << 0

	IORING_OFF_SQ_RING = 0
	IORING_OFF_CQ_RING = 0x8000000
	IORING_OFF_SQES    = 0x10000000

	IORING_ENTER_GETEVENTS = 1 << 0

	IORING_OP_ACCEPT = 13
	IORING_OP_READ   = 22
	IORING_OP_WRITE  = 23

	opTagAccept = 1
	opTagRead   = 2
	opTagWrite  = 3
	opTagWakeup = 4
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

type ioUringRing struct {
	ringFd     int
	sqRingMmap []byte
	cqRingMmap []byte
	sqesMmap   []byte

	sqHead     *uint32
	sqTail     *uint32
	sqRingMask uint32
	sqArray    []uint32
	sqes       []ioUringSQE

	cqHead     *uint32
	cqTail     *uint32
	cqRingMask uint32
	cqes       []ioUringCQE
}

type clientConn struct {
	fd     int32
	inBuf  []byte
	outBuf []byte
	argBuf [][]byte
}

type ioUringServer struct {
	eng      oryx.Database
	server   *RESPServer
	addr     string
	stopOnce sync.Once
	mu       sync.Mutex
	bound    string
	stopped  atomic.Bool
	ring     *ioUringRing
	eventFd  int
	listenFd int32
}

func newIOUringServer(eng oryx.Database, addr string) *ioUringServer {
	return &ioUringServer{
		eng:    eng,
		server: NewRESPServer(eng, addr),
		addr:   addr,
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

func ioUringSetup(entries uint32, params *ioUringParams) (int, error) {
	r1, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(params)), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r1), nil
}

func (r *ioUringRing) getSQE() *ioUringSQE {
	head := atomic.LoadUint32(r.sqHead)
	tail := *r.sqTail
	if tail-head >= uint32(len(r.sqes)) {
		return nil
	}
	idx := tail & r.sqRingMask
	r.sqArray[idx] = idx
	*r.sqTail = tail + 1
	sqe := &r.sqes[idx]
	*sqe = ioUringSQE{}
	return sqe
}

func (r *ioUringRing) submit(minComplete uint32) (int, error) {
	flags := uint32(0)
	if minComplete > 0 {
		flags |= IORING_ENTER_GETEVENTS
	}
	head := atomic.LoadUint32(r.sqHead)
	tail := *r.sqTail
	toSubmit := tail - head
	r1, _, errno := unix.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(r.ringFd),
		uintptr(toSubmit),
		uintptr(minComplete),
		uintptr(flags),
		0, 0,
	)
	if errno != 0 {
		return int(r1), errno
	}
	return int(r1), nil
}

func encodeTag(tag uint32, fd int32) uint64 {
	return (uint64(tag) << 32) | uint64(uint32(fd))
}

func decodeTag(val uint64) (uint32, int32) {
	return uint32(val >> 32), int32(uint32(val))
}

func (s *ioUringServer) Run() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var params ioUringParams
	params.flags = IORING_SETUP_SQPOLL
	ringFd, err := ioUringSetup(1024, &params)
	if err != nil {
		params.flags = 0
		ringFd, err = ioUringSetup(1024, &params)
		if err != nil {
			return fmt.Errorf("io_uring setup failed: %w", err)
		}
	}

	sqRingSize := params.sqOff.array + params.sqEntries*4
	sqRingMmap, err := unix.Mmap(ringFd, IORING_OFF_SQ_RING, int(sqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Close(ringFd)
		return fmt.Errorf("mmap sq_ring: %w", err)
	}

	sqeSize := params.sqEntries * uint32(unsafe.Sizeof(ioUringSQE{}))
	sqesMmap, err := unix.Mmap(ringFd, IORING_OFF_SQES, int(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Munmap(sqRingMmap)
		_ = unix.Close(ringFd)
		return fmt.Errorf("mmap sqes: %w", err)
	}

	var cqRingMmap []byte
	if params.features&IORING_FEAT_SINGLE_MMAP != 0 {
		cqRingMmap = sqRingMmap
	} else {
		cqRingSize := params.cqOff.cqes + params.cqEntries*uint32(unsafe.Sizeof(ioUringCQE{}))
		cqRingMmap, err = unix.Mmap(ringFd, IORING_OFF_CQ_RING, int(cqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
		if err != nil {
			_ = unix.Munmap(sqesMmap)
			_ = unix.Munmap(sqRingMmap)
			_ = unix.Close(ringFd)
			return fmt.Errorf("mmap cq_ring: %w", err)
		}
	}

	ring := &ioUringRing{
		ringFd:     ringFd,
		sqRingMmap: sqRingMmap,
		cqRingMmap: cqRingMmap,
		sqesMmap:   sqesMmap,
		sqHead:     (*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.head])),
		sqTail:     (*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.tail])),
		sqRingMask: *(*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.ringMask])),
		sqArray:    unsafe.Slice((*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.array])), params.sqEntries),
		sqes:       unsafe.Slice((*ioUringSQE)(unsafe.Pointer(&sqesMmap[0])), params.sqEntries),
		cqHead:     (*uint32)(unsafe.Pointer(&cqRingMmap[params.cqOff.head])),
		cqTail:     (*uint32)(unsafe.Pointer(&cqRingMmap[params.cqOff.tail])),
		cqRingMask: *(*uint32)(unsafe.Pointer(&cqRingMmap[params.cqOff.ringMask])),
		cqes:       unsafe.Slice((*ioUringCQE)(unsafe.Pointer(&cqRingMmap[params.cqOff.cqes])), params.cqEntries),
	}
	s.ring = ring

	eventFd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		s.cleanup()
		return fmt.Errorf("eventfd: %w", err)
	}
	s.eventFd = eventFd

	addr := s.addr
	l, err := net.Listen("tcp", addr)
	if err != nil {
		s.cleanup()
		return err
	}
	tcpLis, _ := l.(*net.TCPListener)
	file, err := tcpLis.File()
	if err != nil {
		_ = l.Close()
		s.cleanup()
		return err
	}
	listenFd := int32(file.Fd())
	s.listenFd = listenFd
	_ = l.Close()

	s.mu.Lock()
	s.bound = file.Name()
	if strings.Contains(s.bound, "@") || s.bound == "" {
		s.bound = l.Addr().String()
	}
	s.mu.Unlock()

	defer s.cleanup()

	var wakeBuf [8]byte
	if wakeSQE := ring.getSQE(); wakeSQE != nil {
		wakeSQE.opcode = IORING_OP_READ
		wakeSQE.fd = int32(eventFd)
		wakeSQE.addr = uint64(uintptr(unsafe.Pointer(&wakeBuf[0])))
		wakeSQE.len = 8
		wakeSQE.userData = encodeTag(opTagWakeup, int32(eventFd))
	}

	if acceptSQE := ring.getSQE(); acceptSQE != nil {
		acceptSQE.opcode = IORING_OP_ACCEPT
		acceptSQE.fd = listenFd
		acceptSQE.userData = encodeTag(opTagAccept, listenFd)
	}

	clients := make(map[int32]*clientConn)

	for !s.stopped.Load() {
		_, err := ring.submit(1)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if s.stopped.Load() {
				break
			}
			return fmt.Errorf("io_uring submit: %w", err)
		}

		cqHead := atomic.LoadUint32(ring.cqHead)
		cqTail := atomic.LoadUint32(ring.cqTail)
		for cqHead != cqTail {
			cqe := ring.cqes[cqHead&ring.cqRingMask]
			tag, fd := decodeTag(cqe.userData)

			switch tag {
			case opTagWakeup:
				if s.stopped.Load() {
					break
				}
				if nextWake := ring.getSQE(); nextWake != nil {
					nextWake.opcode = IORING_OP_READ
					nextWake.fd = int32(eventFd)
					nextWake.addr = uint64(uintptr(unsafe.Pointer(&wakeBuf[0])))
					nextWake.len = 8
					nextWake.userData = encodeTag(opTagWakeup, int32(eventFd))
				}

			case opTagAccept:
				if cqe.res >= 0 {
					clientFd := cqe.res
					c := &clientConn{
						fd:     clientFd,
						inBuf:  make([]byte, 65536),
						outBuf: make([]byte, 0, 65536),
						argBuf: make([][]byte, 0, 32),
					}
					clients[clientFd] = c
					if readSQE := ring.getSQE(); readSQE != nil {
						readSQE.opcode = IORING_OP_READ
						readSQE.fd = clientFd
						readSQE.addr = uint64(uintptr(unsafe.Pointer(&c.inBuf[0])))
						readSQE.len = uint32(len(c.inBuf))
						readSQE.userData = encodeTag(opTagRead, clientFd)
					}
				}
				if !s.stopped.Load() {
					if nextAccept := ring.getSQE(); nextAccept != nil {
						nextAccept.opcode = IORING_OP_ACCEPT
						nextAccept.fd = listenFd
						nextAccept.userData = encodeTag(opTagAccept, listenFd)
					}
				}

			case opTagRead:
				c, ok := clients[fd]
				if !ok || cqe.res <= 0 {
					delete(clients, fd)
					_ = unix.Close(int(fd))
				} else {
					data := c.inBuf[:cqe.res]
					c.outBuf = c.outBuf[:0]
					for len(data) > 0 {
						args, readBytes, ok := parseRESPFrame(data, c.argBuf)
						if !ok {
							break
						}
						data = data[readBytes:]
						c.outBuf = s.server.dispatchToBuffer(args, c.outBuf)
					}
					if len(c.outBuf) > 0 {
						if writeSQE := ring.getSQE(); writeSQE != nil {
							writeSQE.opcode = IORING_OP_WRITE
							writeSQE.fd = fd
							writeSQE.addr = uint64(uintptr(unsafe.Pointer(&c.outBuf[0])))
							writeSQE.len = uint32(len(c.outBuf))
							writeSQE.userData = encodeTag(opTagWrite, fd)
						}
					} else {
						if nextRead := ring.getSQE(); nextRead != nil {
							nextRead.opcode = IORING_OP_READ
							nextRead.fd = fd
							nextRead.addr = uint64(uintptr(unsafe.Pointer(&c.inBuf[0])))
							nextRead.len = uint32(len(c.inBuf))
							nextRead.userData = encodeTag(opTagRead, fd)
						}
					}
				}

			case opTagWrite:
				c, ok := clients[fd]
				if !ok || cqe.res < 0 {
					delete(clients, fd)
					_ = unix.Close(int(fd))
				} else {
					if nextRead := ring.getSQE(); nextRead != nil {
						nextRead.opcode = IORING_OP_READ
						nextRead.fd = fd
						nextRead.addr = uint64(uintptr(unsafe.Pointer(&c.inBuf[0])))
						nextRead.len = uint32(len(c.inBuf))
						nextRead.userData = encodeTag(opTagRead, fd)
					}
				}
			}

			cqHead++
		}
		atomic.StoreUint32(ring.cqHead, cqHead)
	}

	for fd := range clients {
		_ = unix.Close(int(fd))
	}

	return nil
}

func (s *ioUringServer) cleanup() {
	s.stopOnce.Do(func() {
		s.stopped.Store(true)
		if s.eventFd > 0 {
			_ = unix.Close(s.eventFd)
		}
		if s.listenFd > 0 {
			_ = unix.Close(int(s.listenFd))
		}
		if s.ring != nil {
			if s.ring.cqRingMmap != nil && &s.ring.cqRingMmap[0] != &s.ring.sqRingMmap[0] {
				_ = unix.Munmap(s.ring.cqRingMmap)
			}
			if s.ring.sqesMmap != nil {
				_ = unix.Munmap(s.ring.sqesMmap)
			}
			if s.ring.sqRingMmap != nil {
				_ = unix.Munmap(s.ring.sqRingMmap)
			}
			if s.ring.ringFd > 0 {
				_ = unix.Close(s.ring.ringFd)
			}
		}
	})
}

func (s *ioUringServer) Stop() {
	s.stopped.Store(true)
	if s.eventFd > 0 {
		var val uint64 = 1
		_, _ = unix.Write(s.eventFd, (*(*[8]byte)(unsafe.Pointer(&val)))[:])
	}
	s.cleanup()
}
