//go:build linux

package server

import (
	"errors"
	"fmt"
	"net"
	"runtime"
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
	IORING_ENTER_SQ_WAKEUP = 1 << 1

	IORING_SQ_NEED_WAKEUP = 1 << 0

	IORING_OP_READV  = 1
	IORING_OP_WRITEV = 2
	IORING_OP_ACCEPT = 13

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
	sqFlags    *uint32
	sqRingMask uint32
	sqArray    []uint32
	sqes       []ioUringSQE

	cqHead     *uint32
	cqTail     *uint32
	cqRingMask uint32
	cqes       []ioUringCQE

	unsubmittedTail uint32
	sqpoll          bool
}

type clientConn struct {
	fd       int32
	inBuf    []byte
	outBuf   []byte
	argBuf   [][]byte
	inIovec  unix.Iovec
	outIovec unix.Iovec
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
	wakeBuf  [8]byte
	wakeIov  unix.Iovec
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
	iou := newIOUringServer(s.eng, s.addr)
	iou.server = s
	s.mu.Lock()
	s.ioUring = iou
	s.mu.Unlock()
	return iou.Run()
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
	tail := r.unsubmittedTail
	if tail-head >= uint32(len(r.sqes)) {
		return nil
	}
	idx := tail & r.sqRingMask
	r.sqArray[idx] = idx
	sqe := &r.sqes[idx]
	*sqe = ioUringSQE{}
	r.unsubmittedTail = tail + 1
	return sqe
}

func (r *ioUringRing) submit(minComplete uint32) (int, error) {
	head := atomic.LoadUint32(r.sqHead)
	tail := r.unsubmittedTail
	toSubmit := tail - head
	atomic.StoreUint32(r.sqTail, tail)

	flags := uint32(0)
	if minComplete > 0 {
		flags |= IORING_ENTER_GETEVENTS
	}
	if r.sqpoll && (atomic.LoadUint32(r.sqFlags)&IORING_SQ_NEED_WAKEUP != 0) {
		flags |= IORING_ENTER_SQ_WAKEUP
	}

	if !r.sqpoll || flags != 0 || toSubmit > 0 {
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
	return int(toSubmit), nil
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

	ring, err := initRing(1024)
	if err != nil {
		return err
	}
	s.ring = ring

	eventFd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		s.cleanup()
		return fmt.Errorf("eventfd: %w", err)
	}
	s.eventFd = eventFd

	listenFd, boundAddr, err := initListener(s.addr)
	if err != nil {
		s.cleanup()
		return err
	}
	s.listenFd = listenFd
	if boundAddr != "" {
		s.mu.Lock()
		s.bound = boundAddr
		s.mu.Unlock()
	}

	defer s.cleanup()
	s.initInitialSQEs()

	clients := make(map[int32]*clientConn)
	for !s.stopped.Load() {
		if _, err := ring.submit(1); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if s.stopped.Load() {
				break
			}
			return fmt.Errorf("io_uring submit: %w", err)
		}
		s.processCQEs(clients)
	}

	for fd := range clients {
		_ = unix.Close(int(fd))
	}
	return nil
}

func initRing(entries uint32) (*ioUringRing, error) {
	var params ioUringParams
	params.flags = IORING_SETUP_SQPOLL
	ringFd, err := ioUringSetup(entries, &params)
	if err != nil {
		params.flags = 0
		ringFd, err = ioUringSetup(entries, &params)
		if err != nil {
			return nil, fmt.Errorf("io_uring setup failed: %w", err)
		}
	}

	sqRingSize := params.sqOff.array + params.sqEntries*4
	sqRingMmap, err := unix.Mmap(ringFd, IORING_OFF_SQ_RING, int(sqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Close(ringFd)
		return nil, fmt.Errorf("mmap sq_ring: %w", err)
	}

	sqeSize := params.sqEntries * uint32(unsafe.Sizeof(ioUringSQE{}))
	sqesMmap, err := unix.Mmap(ringFd, IORING_OFF_SQES, int(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Munmap(sqRingMmap)
		_ = unix.Close(ringFd)
		return nil, fmt.Errorf("mmap sqes: %w", err)
	}

	cqRingMmap, err := mmapCQRing(ringFd, &params, sqRingMmap, sqesMmap)
	if err != nil {
		return nil, err
	}

	return &ioUringRing{
		ringFd:          ringFd,
		sqRingMmap:      sqRingMmap,
		cqRingMmap:      cqRingMmap,
		sqesMmap:        sqesMmap,
		sqHead:          (*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.head])),
		sqTail:          (*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.tail])),
		sqFlags:         (*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.flags])),
		sqRingMask:      *(*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.ringMask])),
		sqArray:         unsafe.Slice((*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.array])), params.sqEntries),
		sqes:            unsafe.Slice((*ioUringSQE)(unsafe.Pointer(&sqesMmap[0])), params.sqEntries),
		cqHead:          (*uint32)(unsafe.Pointer(&cqRingMmap[params.cqOff.head])),
		cqTail:          (*uint32)(unsafe.Pointer(&cqRingMmap[params.cqOff.tail])),
		cqRingMask:      *(*uint32)(unsafe.Pointer(&cqRingMmap[params.cqOff.ringMask])),
		cqes:            unsafe.Slice((*ioUringCQE)(unsafe.Pointer(&cqRingMmap[params.cqOff.cqes])), params.cqEntries),
		unsubmittedTail: *(*uint32)(unsafe.Pointer(&sqRingMmap[params.sqOff.tail])),
		sqpoll:          (params.flags & IORING_SETUP_SQPOLL) != 0,
	}, nil
}

func mmapCQRing(ringFd int, params *ioUringParams, sqRingMmap, sqesMmap []byte) ([]byte, error) {
	if params.features&IORING_FEAT_SINGLE_MMAP != 0 {
		return sqRingMmap, nil
	}
	cqRingSize := params.cqOff.cqes + params.cqEntries*uint32(unsafe.Sizeof(ioUringCQE{}))
	cqRingMmap, err := unix.Mmap(ringFd, IORING_OFF_CQ_RING, int(cqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Munmap(sqesMmap)
		_ = unix.Munmap(sqRingMmap)
		_ = unix.Close(ringFd)
		return nil, fmt.Errorf("mmap cq_ring: %w", err)
	}
	return cqRingMmap, nil
}

func initListener(addr string) (int32, string, error) {
	listenFd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return 0, "", fmt.Errorf("socket: %w", err)
	}
	_ = unix.SetsockoptInt(listenFd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	_ = unix.SetsockoptInt(listenFd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		_ = unix.Close(listenFd)
		return 0, "", fmt.Errorf("resolve addr: %w", err)
	}
	sa := &unix.SockaddrInet4{Port: tcpAddr.Port}
	if len(tcpAddr.IP) == 0 || tcpAddr.IP.IsUnspecified() {
		sa.Addr = [4]byte{0, 0, 0, 0}
	} else if ip4 := tcpAddr.IP.To4(); ip4 != nil {
		copy(sa.Addr[:], ip4)
	}

	if err := unix.Bind(listenFd, sa); err != nil {
		_ = unix.Close(listenFd)
		return 0, "", fmt.Errorf("bind: %w", err)
	}
	if err := unix.Listen(listenFd, 1024); err != nil {
		_ = unix.Close(listenFd)
		return 0, "", fmt.Errorf("listen: %w", err)
	}

	bound := ""
	if boundSa, err := unix.Getsockname(listenFd); err == nil {
		if bsa, ok := boundSa.(*unix.SockaddrInet4); ok {
			bound = fmt.Sprintf("127.0.0.1:%d", bsa.Port)
		}
	}
	return int32(listenFd), bound, nil
}

func (s *ioUringServer) initInitialSQEs() {
	s.wakeIov.Base = &s.wakeBuf[0]
	s.wakeIov.Len = 8
	if wakeSQE := s.ring.getSQE(); wakeSQE != nil {
		wakeSQE.opcode = IORING_OP_READV
		wakeSQE.fd = int32(s.eventFd)
		wakeSQE.addr = uint64(uintptr(unsafe.Pointer(&s.wakeIov)))
		wakeSQE.len = 1
		wakeSQE.userData = encodeTag(opTagWakeup, int32(s.eventFd))
	}
	if acceptSQE := s.ring.getSQE(); acceptSQE != nil {
		acceptSQE.opcode = IORING_OP_ACCEPT
		acceptSQE.fd = s.listenFd
		acceptSQE.userData = encodeTag(opTagAccept, s.listenFd)
	}
}

func (s *ioUringServer) processCQEs(clients map[int32]*clientConn) {
	cqHead := atomic.LoadUint32(s.ring.cqHead)
	cqTail := atomic.LoadUint32(s.ring.cqTail)
	for cqHead != cqTail {
		cqe := s.ring.cqes[cqHead&s.ring.cqRingMask]
		tag, fd := decodeTag(cqe.userData)

		switch tag {
		case opTagWakeup:
			s.handleWakeup()
		case opTagAccept:
			s.handleAccept(cqe, clients)
		case opTagRead:
			s.handleRead(cqe, fd, clients)
		case opTagWrite:
			s.handleWrite(cqe, fd, clients)
		}
		cqHead++
	}
	atomic.StoreUint32(s.ring.cqHead, cqHead)
}

func (s *ioUringServer) handleWakeup() {
	if s.stopped.Load() {
		return
	}
	if nextWake := s.ring.getSQE(); nextWake != nil {
		nextWake.opcode = IORING_OP_READV
		nextWake.fd = int32(s.eventFd)
		nextWake.addr = uint64(uintptr(unsafe.Pointer(&s.wakeIov)))
		nextWake.len = 1
		nextWake.userData = encodeTag(opTagWakeup, int32(s.eventFd))
	}
}

func (s *ioUringServer) handleAccept(cqe ioUringCQE, clients map[int32]*clientConn) {
	if cqe.res >= 0 {
		clientFd := cqe.res
		c := &clientConn{
			fd:     clientFd,
			inBuf:  make([]byte, 65536),
			outBuf: make([]byte, 0, 65536),
			argBuf: make([][]byte, 0, 32),
		}
		c.inIovec.Base = &c.inBuf[0]
		c.inIovec.Len = uint64(len(c.inBuf))
		clients[clientFd] = c
		if readSQE := s.ring.getSQE(); readSQE != nil {
			readSQE.opcode = IORING_OP_READV
			readSQE.fd = clientFd
			readSQE.addr = uint64(uintptr(unsafe.Pointer(&c.inIovec)))
			readSQE.len = 1
			readSQE.userData = encodeTag(opTagRead, clientFd)
		}
	}
	if !s.stopped.Load() {
		if nextAccept := s.ring.getSQE(); nextAccept != nil {
			nextAccept.opcode = IORING_OP_ACCEPT
			nextAccept.fd = s.listenFd
			nextAccept.userData = encodeTag(opTagAccept, s.listenFd)
		}
	}
}

func (s *ioUringServer) handleRead(cqe ioUringCQE, fd int32, clients map[int32]*clientConn) {
	c, ok := clients[fd]
	if !ok || cqe.res <= 0 {
		delete(clients, fd)
		_ = unix.Close(int(fd))
		return
	}

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
		c.outIovec.Base = &c.outBuf[0]
		c.outIovec.Len = uint64(len(c.outBuf))
		if writeSQE := s.ring.getSQE(); writeSQE != nil {
			writeSQE.opcode = IORING_OP_WRITEV
			writeSQE.fd = fd
			writeSQE.addr = uint64(uintptr(unsafe.Pointer(&c.outIovec)))
			writeSQE.len = 1
			writeSQE.userData = encodeTag(opTagWrite, fd)
		}
	} else {
		c.inIovec.Base = &c.inBuf[0]
		c.inIovec.Len = uint64(len(c.inBuf))
		if nextRead := s.ring.getSQE(); nextRead != nil {
			nextRead.opcode = IORING_OP_READV
			nextRead.fd = fd
			nextRead.addr = uint64(uintptr(unsafe.Pointer(&c.inIovec)))
			nextRead.len = 1
			nextRead.userData = encodeTag(opTagRead, fd)
		}
	}
}

func (s *ioUringServer) handleWrite(cqe ioUringCQE, fd int32, clients map[int32]*clientConn) {
	c, ok := clients[fd]
	if !ok || cqe.res < 0 {
		delete(clients, fd)
		_ = unix.Close(int(fd))
		return
	}
	c.inIovec.Base = &c.inBuf[0]
	c.inIovec.Len = uint64(len(c.inBuf))
	if nextRead := s.ring.getSQE(); nextRead != nil {
		nextRead.opcode = IORING_OP_READV
		nextRead.fd = fd
		nextRead.addr = uint64(uintptr(unsafe.Pointer(&c.inIovec)))
		nextRead.len = 1
		nextRead.userData = encodeTag(opTagRead, fd)
	}
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
