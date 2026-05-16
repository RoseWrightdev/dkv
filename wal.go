package dkv

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	pb "github.com/rosewrightdev/dkv/api"
	"google.golang.org/protobuf/proto"
)

// Waler defines the interface for a durable write-ahead log.
type Waler interface {
	publish(key Key, hash hashKey, msg proto.Message) error
	replay() (map[Key]Value, error)
	clear() error
	start()
	stop()
}

type walSegment struct {
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	syncInterval time.Duration
	wrt          *bufio.Writer
	file         *os.File
	path         string
}

func (s *walSegment) backgroundSync() {
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.wrt.Buffered() > 0 {
				s.wrt.Flush()
				s.file.Sync()
			}
			s.mu.Unlock()
		case <-s.ctx.Done():
			return
		}
	}
}

type Wal struct {
	segments   []*walSegment
	count      int
	headerPool sync.Pool
	entryPool  sync.Pool
	bufferPool sync.Pool
}

func newWal(dirPath string, syncInterval time.Duration, bufferSize uint32, segmentCount int) (*Wal, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	wal := &Wal{
		segments: make([]*walSegment, segmentCount),
		count:    segmentCount,
		headerPool: sync.Pool{
			New: func() any {
				b := make([]byte, 4)
				return &b
			},
		},
		entryPool: sync.Pool{
			New: func() any {
				return &pb.WalEntry{}
			},
		},
		bufferPool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, 2048)
				return &b
			},
		},
	}

	for i := range segmentCount {
		path := fmt.Sprintf("%s/seg_%02d.log", dirPath, i)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}

		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			file.Close()
			return nil, err
		}

		ctx, cancel := context.WithCancel(context.Background())
		seg := &walSegment{
			ctx:          ctx,
			cancel:       cancel,
			mu:           sync.Mutex{},
			syncInterval: syncInterval,
			wrt:          bufio.NewWriterSize(file, int(bufferSize)),
			file:         file,
			path:         path,
		}
		wal.segments[i] = seg
	}

	return wal, nil
}

func (w *Wal) start() {
	for _, seg := range w.segments {
		go seg.backgroundSync()
	}
}

func (w *Wal) stop() {
	for _, seg := range w.segments {
		seg.mu.Lock()
		seg.cancel()
		seg.wrt.Flush()
		seg.file.Sync()
		seg.file.Close()
		seg.mu.Unlock()
	}
}

func (w *Wal) getSegment(hash hashKey) *walSegment {
	return w.segments[hash%hashKey(w.count)]
}

func (w *Wal) publish(key Key, hash hashKey, msg proto.Message) error {
	entry := w.entryPool.Get().(*pb.WalEntry)
	defer w.entryPool.Put(entry)

	switch m := msg.(type) {
	case *pb.WalEntry:
		entry.Entry = m.Entry
	case *pb.SetRequest:
		if wrapper, ok := entry.Entry.(*pb.WalEntry_Set); ok {
			wrapper.Set = m
		} else {
			entry.Entry = &pb.WalEntry_Set{Set: m}
		}
	case *pb.DeleteRequest:
		if wrapper, ok := entry.Entry.(*pb.WalEntry_Delete); ok {
			wrapper.Delete = m
		} else {
			entry.Entry = &pb.WalEntry_Delete{Delete: m}
		}
	default:
		return fmt.Errorf("unsupported message type: %T", msg)
	}

	bufPtr := w.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	data, err := proto.MarshalOptions{}.MarshalAppend(buf, entry)
	if err != nil {
		w.bufferPool.Put(bufPtr)
		return err
	}
	*bufPtr = data
	defer w.bufferPool.Put(bufPtr)

	headerPtr := w.headerPool.Get().(*[]byte)
	header := *headerPtr
	defer w.headerPool.Put(headerPtr)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	seg := w.getSegment(hash)
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if _, err := seg.wrt.Write(header); err != nil {
		return err
	}

	if _, err := seg.wrt.Write(data); err != nil {
		return err
	}

	return nil
}

func (w *Wal) replay() (map[Key]Value, error) {
	results := make(map[Key]Value)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	for i := range w.count {
		wg.Add(1)
		go func(seg *walSegment) {
			defer wg.Done()
			if err := w.replaySegment(seg, results, &mu); err != nil {
				errOnce.Do(func() { firstErr = err })
			}
		}(w.segments[i])
	}

	wg.Wait()
	return results, firstErr
}

func (w *Wal) replaySegment(seg *walSegment, results map[Key]Value, resultsMu *sync.Mutex) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	err := seg.wrt.Flush()
	if err != nil {
		return err
	}
	if _, err := seg.file.Seek(0, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(seg.file)
	headerPtr := w.headerPool.Get().(*[]byte)
	header := *headerPtr
	defer w.headerPool.Put(headerPtr)
	var payload []byte

	for {
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		size := int(binary.BigEndian.Uint32(header))
		if cap(payload) < size {
			payload = make([]byte, size)
		}
		payload = payload[:size]

		if _, err := io.ReadFull(reader, payload); err != nil {
			return err
		}

		entry := w.entryPool.Get().(*pb.WalEntry)
		entry.Reset()
		if err := proto.Unmarshal(payload, entry); err != nil {
			w.entryPool.Put(entry)
			return err
		}

		resultsMu.Lock()
		switch kv := entry.Entry.(type) {
		case *pb.WalEntry_Set:
			results[kv.Set.Key] = Value{
				Data:      kv.Set.Value,
				Timestamp: kv.Set.Timestamp,
				Tombstone: false,
			}
		case *pb.WalEntry_Delete:
			results[kv.Delete.Key] = Value{
				Timestamp: kv.Delete.Timestamp,
				Tombstone: true,
			}
		}
		resultsMu.Unlock()
		w.entryPool.Put(entry)
	}

	_, err = seg.file.Seek(0, io.SeekEnd)
	return err
}

func (w *Wal) clear() error {
	for _, seg := range w.segments {
		seg.mu.Lock()
		if err := seg.file.Truncate(0); err != nil {
			seg.mu.Unlock()
			return err
		}
		if _, err := seg.file.Seek(0, 0); err != nil {
			seg.mu.Unlock()
			return err
		}
		seg.wrt.Reset(seg.file)
		seg.mu.Unlock()
	}
	return nil
}
